#!/usr/bin/env python3
"""
Backup Manager Demo - Simple API server to control Vector for slowlog backup

IMPORTANT: This demo's purpose is ONLY to:
1. Generate Vector configurations
2. Manage Vector process state (start, monitor, stop)

This demo does NOT perform any data processing. All data processing is done by
Vector itself through its exec source, which executes scripts in demo/extension/.

Data Flow:
- Management API (this file) → Generates Vector TOML config
- Vector exec source → Executes demo/extension/sources/parquet_s3_processor.py
- Vector transforms → Applies VRL-based filtering/transformation
- Vector file sink → Outputs to files
- Background thread → Monitors files and imports to MySQL (temporary solution)

Future: Custom Vector plugins (Rust) will replace the Python scripts.
"""
import os
import json
import subprocess
import tempfile
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List
from flask import Flask, request, jsonify
from flask_cors import CORS
import psutil
import toml

app = Flask(__name__)
CORS(app)

# Configuration
VECTOR_BINARY = os.environ.get("VECTOR_BINARY", "vector")
CONFIG_DIR = Path(os.environ.get("CONFIG_DIR", "/tmp/vector-tasks"))
CONFIG_DIR.mkdir(parents=True, exist_ok=True)

# In-memory task storage (in production, use a database)
tasks: Dict[str, Dict] = {}


def find_vector_binary() -> str:
    """Find Vector binary path"""
    # Check environment variable
    if os.environ.get("VECTOR_BINARY"):
        return os.environ.get("VECTOR_BINARY")
    
    # Check project directory
    project_root = Path(__file__).parent.parent
    debug_vector = project_root / "target" / "debug" / "vector"
    if debug_vector.exists() and os.access(debug_vector, os.X_OK):
        return str(debug_vector.resolve())
    
    release_vector = project_root / "target" / "release" / "vector"
    if release_vector.exists() and os.access(release_vector, os.X_OK):
        return str(release_vector.resolve())
    
    # Check system PATH
    if os.system(f"which {VECTOR_BINARY} > /dev/null 2>&1") == 0:
        return VECTOR_BINARY
    
    return VECTOR_BINARY


VECTOR_BINARY = find_vector_binary()


def get_parquet_processor_script_path() -> Path:
    """Get the path to the Parquet S3 processor script
    
    The script is located in demo/extension/sources/ and will be executed
    by Vector's exec source. This script will be converted to a Rust-based
    Vector plugin in the future.
    """
    # Get the demo directory (parent of this file's directory)
    demo_dir = Path(__file__).parent
    script_path = demo_dir / "extension" / "sources" / "parquet_s3_processor.py"
    
    if not script_path.exists():
        raise FileNotFoundError(f"Parquet processor script not found: {script_path}")
    
    return script_path


# Note: get_mysql_writer_script_path() is no longer needed
# MySQL writing is now handled directly by Vector's tidb sink
# This function is kept for backward compatibility but not used


def generate_vector_config(
    task_id: str,
    processor_script: Path,
    mysql_connection: str,
    mysql_table: str,
    s3_bucket: str,
    s3_prefix: str,
    s3_region: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    filter_keywords: Optional[List[str]] = None,
) -> str:
    """Generate Vector TOML configuration for slowlog backup
    
    This function ONLY generates Vector configuration. It does NOT process any data.
    
    Configuration structure:
    1. exec source: Executes Python script (demo/extension/sources/parquet_s3_processor.py)
       - Script reads Parquet files from S3 and outputs JSON Lines to stdout
       - Vector reads stdout and creates events
    2. remap transform: Parses JSON Lines (if needed)
    3. filter transform: Applies keyword filtering using VRL (if provided)
    4. tidb sink: Writes data directly to MySQL/TiDB database
    
    Note: All data processing is done by Vector, not by this management API.
    The Python script is executed by Vector's exec source, not by this app.
    """
    
    # Build keyword filter condition if provided
    keyword_filter_condition = None
    if filter_keywords:
        conditions = [f'contains(string!(.message), "{kw}")' for kw in filter_keywords]
        keyword_filter_condition = " or ".join(conditions)
    
    # Generate Vector config - uses exec source to run Python script
    # Create data_dir first (Vector requires it to exist)
    data_dir = Path(f"/tmp/vector-data/{task_id}")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Note: Environment variables for the script (S3_BUCKET, S3_PREFIX, etc.)
    # will be set when starting the Vector process, not in the config itself.
    # The script reads from environment variables.
    
    config = {
        "data_dir": str(data_dir),
        
        "api": {
            "enabled": True,
            "address": "127.0.0.1:0",  # Random port for Vector API
        },
        
        "sources": {
            "parquet_processor": {
                "type": "exec",
                "command": ["python3", str(processor_script)],
                "mode": "streaming",  # Use streaming mode - script runs and outputs data
                "streaming": {
                    "respawn_on_exit": False  # Don't respawn - script runs once and exits
                },
                "decoding": {
                    "codec": "json"
                },
                # Vector exec source will run the script and read its stdout
                # Each line of JSON output becomes an event
                # When script exits (after processing all files), Vector will finish processing remaining events
                # Environment variables are inherited from Vector process
                # (set by management API before starting Vector)
            }
        },
        
        "transforms": {}
    }
    
    # Add transform to process exec source output
    # The exec source outputs JSON Lines, so we parse them
    config["transforms"]["parse_json"] = {
        "type": "remap",
        "inputs": ["parquet_processor"],
        "source": '''
            # Parse JSON Lines from exec source output
            # Vector exec source with json decoding already parses JSON
            # But we ensure the message field is properly set
            if exists(.message) {
                .message = string!(.message)
            }
            true
        ''',
    }
    
    # Time filtering is already done in the Python script
    # Vector-level filtering would be redundant here
    next_input = "parse_json"
    
    # Add keyword filter if provided
    if keyword_filter_condition:
        config["transforms"]["keyword_filter"] = {
            "type": "filter",
            "inputs": [next_input],
            "condition": keyword_filter_condition,
        }
        sink_input = "keyword_filter"
    else:
        sink_input = next_input
    
    # Add tidb sink - write directly to MySQL/TiDB
    # Parse MySQL connection string to extract components
    # Format: mysql://user:password@host:port/database
    mysql_parts = mysql_connection.replace("mysql://", "").split("@")
    user_pass = mysql_parts[0].split(":")
    mysql_user, mysql_pass = user_pass
    host_port = mysql_parts[1].split("/")
    host_port_parts = host_port[0].split(":")
    mysql_host = host_port_parts[0]
    mysql_port = int(host_port_parts[1]) if len(host_port_parts) > 1 else 3306
    mysql_database = host_port[1]
    
    # Build connection string for tidb sink
    tidb_connection_string = f"mysql://{mysql_user}:{mysql_pass}@{mysql_host}:{mysql_port}/{mysql_database}"
    
    config["sinks"] = {
        "tidb_sink": {
            "type": "tidb",
            "inputs": [sink_input],
            "connection_string": tidb_connection_string,
            "table": mysql_table,
            "batch_size": 1000,
            "max_connections": 10,
            "connection_timeout": 30,
        }
    }
    
    # Convert to TOML string
    return toml.dumps(config)


def start_vector_process(
    task_id: str,
    config_content: str,
    mysql_connection: str,
    mysql_table: str,
    vector_binary: str = None,
    script_env: Optional[Dict[str, str]] = None,
) -> int:
    """Start Vector process with given configuration
    
    This function ONLY starts and manages the Vector process. It does NOT process data.
    
    Args:
        task_id: Task identifier
        config_content: Vector TOML configuration content
        mysql_connection: MySQL connection string (for compatibility, not used directly)
        mysql_table: MySQL table name (for compatibility, not used directly)
        vector_binary: Path to Vector binary (optional)
        script_env: Environment variables to pass to Vector (inherited by exec source scripts)
    
    Note: 
    - Data processing is done by Vector's exec source (executes Python script)
    - MySQL import is handled directly by Vector's tidb sink
    - No background thread needed anymore
    """
    
    # Use provided vector_binary or fallback to VECTOR_BINARY
    vector_cmd = vector_binary if vector_binary else VECTOR_BINARY
    
    # Write config to temporary file
    config_file = CONFIG_DIR / f"{task_id}.toml"
    config_file.write_text(config_content)
    
    # Prepare environment variables
    # Merge script_env with current environment
    env = os.environ.copy()
    if script_env:
        env.update(script_env)
    
    # Start Vector process
    # Note: Vector will inherit environment variables (AWS_ACCESS_KEY_ID, etc.)
    # and pass them to exec source scripts
    cmd = [vector_cmd, "--config", str(config_file)]
    
    # Create log files for Vector output (for debugging)
    log_dir = Path(f"/tmp/vector-logs/{task_id}")
    log_dir.mkdir(parents=True, exist_ok=True)
    stdout_file = log_dir / "stdout.log"
    stderr_file = log_dir / "stderr.log"
    
    # Start Vector process with pipes to capture output
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,  # Line buffered
        env=env,  # Pass environment variables to Vector
    )
    
    # Start threads to read and print Vector output in real-time
    def read_output(pipe, file_path, prefix):
        """Read from pipe and print to console + write to file"""
        with open(file_path, 'w') as f:
            try:
                for line in iter(pipe.readline, ''):
                    if not line:
                        break
                    # Print to console with prefix
                    print(f"[Vector {task_id}] {prefix}: {line.rstrip()}")
                    # Also write to file
                    f.write(line)
                    f.flush()
            except Exception as e:
                print(f"[Vector {task_id}] Error reading {prefix}: {e}")
        pipe.close()
    
    # Start threads to read stdout and stderr
    stdout_thread = threading.Thread(
        target=read_output,
        args=(process.stdout, stdout_file, "OUT"),
        daemon=True
    )
    stderr_thread = threading.Thread(
        target=read_output,
        args=(process.stderr, stderr_file, "ERR"),
        daemon=True
    )
    stdout_thread.start()
    stderr_thread.start()
    
    # Note: MySQL import is now handled directly by Vector's tidb sink
    # No background thread needed anymore
    
    # Start task monitoring thread to detect completion and cleanup
    # For one-time tasks, Vector should exit when exec source script finishes
    monitor_thread = threading.Thread(
        target=monitor_vector_task,
        args=(task_id, process.pid, None),  # No output_dir needed anymore
        daemon=True
    )
    monitor_thread.start()
    
    # Check if process started successfully
    time.sleep(0.5)  # Give process a moment to start
    if process.poll() is not None:
        # Process already exited, wait a bit for stderr to be read
        time.sleep(0.5)
        error_msg = "Unknown error"
        if stderr_file.exists():
            error_content = stderr_file.read_text()
            if error_content:
                error_msg = error_content[:500]  # First 500 chars
        print(f"[Task {task_id}] ❌ Vector process exited immediately: {error_msg}")
        raise Exception(f"Vector process failed to start: {error_msg}")
    
    print(f"[Task {task_id}] ✓ Vector process started with PID: {process.pid}")
    return process.pid


def monitor_vector_task(task_id: str, pid: int, output_dir: Optional[Path]):
    """Monitor Vector process and detect when one-time task completes
    
    For one-time tasks with oneshot exec source:
    - Script runs once and exits
    - Vector processes remaining events and should exit
    - We detect this and update task status
    
    Note: output_dir is optional and only used for legacy file-based monitoring.
    With tidb sink, data is written directly to MySQL, so file monitoring is not needed.
    """
    max_wait_time = 300  # Maximum 5 minutes for task completion
    check_interval = 2  # Check every 2 seconds
    
    start_time = time.time()
    
    print(f"[Monitor {task_id}] Starting task monitoring (PID: {pid})")
    
    while True:
        try:
            # Check if process is still running
            try:
                proc = psutil.Process(pid)
                if not proc.is_running():
                    # Process exited
                    exit_code = proc.returncode
                    print(f"[Monitor {task_id}] Vector process exited with code {exit_code}")
                    
                    # Wait a bit for final data to be written
                    time.sleep(2)
                    
                    # Update task status
                    if task_id in tasks:
                        if exit_code == 0:
                            tasks[task_id]["status"] = "completed"
                            print(f"[Monitor {task_id}] ✓ Task completed successfully")
                        else:
                            tasks[task_id]["status"] = "failed"
                            tasks[task_id]["error"] = f"Vector exited with code {exit_code}"
                            print(f"[Monitor {task_id}] ❌ Task failed with exit code {exit_code}")
                        tasks[task_id]["updated_at"] = datetime.now().isoformat()
                    break
            except psutil.NoSuchProcess:
                # Process already gone
                print(f"[Monitor {task_id}] Vector process not found, task may have completed")
                if task_id in tasks:
                    tasks[task_id]["status"] = "completed"
                    tasks[task_id]["updated_at"] = datetime.now().isoformat()
                break
            
            # Check timeouts
            elapsed = time.time() - start_time
            
            if elapsed > max_wait_time:
                print(f"[Monitor {task_id}] ⚠️  Task exceeded max wait time ({max_wait_time}s), stopping")
                # Force stop Vector process
                try:
                    proc = psutil.Process(pid)
                    proc.terminate()
                    time.sleep(2)
                    if proc.is_running():
                        proc.kill()
                except:
                    pass
                if task_id in tasks:
                    tasks[task_id]["status"] = "timeout"
                    tasks[task_id]["updated_at"] = datetime.now().isoformat()
                break
            
            # For oneshot mode, check if process is actually doing something (CPU usage)
            if elapsed > 60:
                try:
                    proc = psutil.Process(pid)
                    cpu_percent = proc.cpu_percent(interval=1)
                    if cpu_percent < 1.0:  # Very low CPU usage
                        # Process might be done, but give it more time
                        pass
                except:
                    pass
            
            time.sleep(check_interval)
            
        except Exception as e:
            print(f"[Monitor {task_id}] Error in monitoring: {e}")
            time.sleep(check_interval)
    
    print(f"[Monitor {task_id}] Monitoring stopped")


def import_to_mysql(output_dir: Path, mysql_connection: str, mysql_table: str, task_id: str):
    """Import JSON lines from files in directory to MySQL table (real-time monitoring)
    
    NOTE: This function is no longer used. MySQL writing is now handled directly
    by Vector's tidb sink. This function is kept for backward compatibility.
    """
    try:
        import pymysql
    except ImportError:
        print("Warning: pymysql not installed, skipping MySQL import")
        print("Install with: pip install pymysql")
        return
    
    # Parse MySQL connection
    mysql_parts = mysql_connection.replace("mysql://", "").split("@")
    user_pass = mysql_parts[0].split(":")
    mysql_user, mysql_pass = user_pass
    host_port = mysql_parts[1].split("/")
    host_port_parts = host_port[0].split(":")
    mysql_host = host_port_parts[0]
    mysql_port = int(host_port_parts[1]) if len(host_port_parts) > 1 else 3306
    mysql_database = host_port[1]
    
    # Wait for directory to exist and files to appear
    max_wait = 60
    waited = 0
    while not output_dir.exists() and waited < max_wait:
        time.sleep(1)
        waited += 1
    
    if not output_dir.exists():
        print(f"Warning: Output directory {output_dir} not created after {max_wait} seconds")
        return
    
    # Connect to MySQL
    try:
        conn = pymysql.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_pass,
            database=mysql_database,
            charset='utf8mb4'
        )
        cursor = conn.cursor()
        
        # Real-time file monitoring - monitor all .jsonl files in directory
        batch_size = 100
        batch = []
        processed_files = set()
        file_positions = {}  # Track position for each file
        no_change_count = 0
        max_no_change = 60  # Stop after 60 seconds of no changes
        
        print(f"[MySQL Import] Starting to import from {output_dir} to MySQL table {mysql_table}")
        print(f"[MySQL Import] Connection: {mysql_host}:{mysql_port}/{mysql_database}")
        
        total_imported = 0
        last_log_time = time.time()
        
        # Monitor directory for new files and existing files for new lines
        while True:
            try:
                # Find all .jsonl files in directory
                jsonl_files = list(output_dir.glob("*.jsonl"))
                
                if not jsonl_files:
                    no_change_count += 1
                    if no_change_count >= max_no_change:
                        print(f"[MySQL Import] No files found for {max_no_change} seconds, stopping import")
                        break
                    time.sleep(1)
                    continue
                
                no_change_count = 0
                has_new_data = False
                
                # Process each file
                for output_file in jsonl_files:
                    file_path_str = str(output_file)
                    
                    # Initialize position for new files
                    if file_path_str not in file_positions:
                        file_positions[file_path_str] = 0
                        print(f"[MySQL Import] Found new file: {output_file.name}")
                    
                    if not output_file.exists():
                        continue
                    
                    try:
                        current_size = output_file.stat().st_size
                        last_position = file_positions[file_path_str]
                        
                        if current_size > last_position:
                            has_new_data = True
                            with open(output_file, 'r', encoding='utf-8', errors='ignore') as f:
                                # Seek to last position
                                f.seek(last_position)
                                
                                new_lines = f.readlines()
                                if new_lines:
                                    file_positions[file_path_str] = f.tell()
                                    
                                    for line in new_lines:
                                        line = line.strip()
                                        if not line:
                                            continue
                                        
                                        try:
                                            data = json.loads(line)
                                            # Extract message field (the slowlog line)
                                            message = data.get('message', '')
                                            if not message:
                                                # Try other common fields
                                                message = data.get('log', data.get('text', line))
                                            
                                            # Get timestamp
                                            timestamp_str = data.get('timestamp')
                                            if timestamp_str:
                                                try:
                                                    # Convert ISO 8601 to MySQL DATETIME format
                                                    ts_str = timestamp_str.replace('Z', '+00:00')
                                                    dt = datetime.fromisoformat(ts_str)
                                                    # Convert to MySQL datetime format: YYYY-MM-DD HH:MM:SS
                                                    mysql_timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
                                                except:
                                                    mysql_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                            else:
                                                mysql_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                            
                                            # Insert into MySQL (one line at a time for demo)
                                            sql = f"INSERT INTO {mysql_table} (log_line, log_timestamp, task_id) VALUES (%s, %s, %s)"
                                            batch.append((message, mysql_timestamp, task_id))
                                            
                                            if len(batch) >= batch_size:
                                                cursor.executemany(sql, batch)
                                                conn.commit()
                                                total_imported += len(batch)
                                                print(f"[MySQL Import] ✓ Imported {len(batch)} lines (total: {total_imported})")
                                                batch = []
                                                
                                        except json.JSONDecodeError as e:
                                            # If not JSON, insert as plain text
                                            sql = f"INSERT INTO {mysql_table} (log_line, log_timestamp, task_id) VALUES (%s, %s, %s)"
                                            mysql_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                            batch.append((line, mysql_timestamp, task_id))
                                            
                                            if len(batch) >= batch_size:
                                                cursor.executemany(sql, batch)
                                                conn.commit()
                                                total_imported += len(batch)
                                                print(f"[MySQL Import] ✓ Imported {len(batch)} lines (total: {total_imported})")
                                                batch = []
                                        except Exception as e:
                                            print(f"[MySQL Import] ⚠️  Error processing line: {e}")
                                            print(f"[MySQL Import] Line content: {line[:100]}...")
                                    
                    except Exception as e:
                        print(f"[MySQL Import] ⚠️  Error reading file {output_file.name}: {e}")
                        time.sleep(0.5)
                        continue
                
                # Log progress periodically
                if has_new_data:
                    last_log_time = time.time()
                elif time.time() - last_log_time > 10:
                    print(f"[MySQL Import] Waiting for new data... (total imported: {total_imported})")
                    last_log_time = time.time()
                
                # Small sleep to avoid busy loop
                time.sleep(0.5)
                    
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error reading file: {e}")
                time.sleep(1)
        
        # Insert remaining batch (after while loop exits)
        if batch:
            sql = f"INSERT INTO {mysql_table} (log_line, log_timestamp, task_id) VALUES (%s, %s, %s)"
            cursor.executemany(sql, batch)
            conn.commit()
            total_imported += len(batch)
            print(f"[MySQL Import] ✓ Imported final {len(batch)} lines (total: {total_imported})")
        
        cursor.close()
        conn.close()
        print(f"[MySQL Import] ✓ Finished importing {total_imported} total lines to MySQL table {mysql_table}")
            
    except Exception as e:
        print(f"Error importing to MySQL: {e}")
        import traceback
        traceback.print_exc()


@app.route("/api/v1/health", methods=["GET"])
def health():
    """Health check endpoint"""
    return jsonify({"status": "ok", "vector_binary": VECTOR_BINARY})


@app.route("/api/v1/tasks", methods=["POST"])
def create_task():
    """Create a new backup task"""
    try:
        data = request.json
        
        # Validate required fields
        required_fields = ["s3_bucket", "s3_prefix", "mysql_connection", "mysql_table"]
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400
        
        task_id = str(uuid.uuid4())
        
        # Extract time range if provided
        time_range = data.get("time_range")
        start_time = None
        end_time = None
        if time_range:
            start_time = time_range.get("start")
            end_time = time_range.get("end")
        
        # Step 1: Get extension script path
        print(f"[Task {task_id}] Step 1: Getting extension script...")
        try:
            processor_script = get_parquet_processor_script_path()
        except FileNotFoundError as e:
            return jsonify({"error": str(e)}), 500
        
        # Step 2: Generate Vector configuration
        # The script will be executed by Vector's exec source with environment variables
        # Data will be written directly to MySQL using tidb sink
        print(f"[Task {task_id}] Step 2: Generating Vector configuration...")
        vector_config = generate_vector_config(
            task_id=task_id,
            processor_script=processor_script,
            mysql_connection=data["mysql_connection"],
            mysql_table=data["mysql_table"],
            s3_bucket=data["s3_bucket"],
            s3_prefix=data["s3_prefix"],
            s3_region=data.get("s3_region", "us-west-2"),
            start_time=start_time,
            end_time=end_time,
            filter_keywords=data.get("filter_keywords"),
        )
        
        # Step 3: Start Vector process
        print(f"[Task {task_id}] Step 3: Starting Vector process...")
        
        # Check if Vector is available
        vector_binary_path = Path(VECTOR_BINARY)
        actual_vector_path = None
        
        if vector_binary_path.exists() and os.access(vector_binary_path, os.X_OK):
            # Vector found at configured path
            actual_vector_path = str(vector_binary_path.resolve())
        else:
            # Try to find Vector in project directory
            project_root = Path(__file__).parent.parent
            project_vector = project_root / "target" / "debug" / "vector"
            if project_vector.exists() and os.access(project_vector, os.X_OK):
                actual_vector_path = str(project_vector.resolve())
            else:
                # Try release build
                project_vector = project_root / "target" / "release" / "vector"
                if project_vector.exists() and os.access(project_vector, os.X_OK):
                    actual_vector_path = str(project_vector.resolve())
        
        if not actual_vector_path:
            return jsonify({"error": "Vector binary not found. Please build Vector first."}), 500
        
        # MySQL connection and table are already used in generate_vector_config
        # to configure the tidb sink directly
        mysql_connection = data["mysql_connection"]
        mysql_table = data["mysql_table"]
        
        # Prepare environment variables for the source script only
        # Note: MySQL connection is configured in Vector's tidb sink config, not via environment variables
        script_env = {
            # For source script (parquet processor)
            "S3_BUCKET": data["s3_bucket"],
            "S3_PREFIX": data["s3_prefix"],
            "S3_REGION": data.get("s3_region", "us-west-2"),
            "TASK_ID": task_id,
        }
        if start_time:
            script_env["START_TIME"] = start_time
        if end_time:
            script_env["END_TIME"] = end_time
        
        # Start Vector process
        print(f"[Task {task_id}] ✓ Vector found: {actual_vector_path}, starting Vector process...")
        pid = start_vector_process(
            task_id,
            vector_config,
            data["mysql_connection"],
            data["mysql_table"],
            vector_binary=actual_vector_path,
            script_env=script_env,
        )
        
        # Store task info
        tasks[task_id] = {
            "task_id": task_id,
            "status": "running",
            "pid": pid,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "config": {
                "s3_bucket": data["s3_bucket"],
                "s3_prefix": data["s3_prefix"],
                "mysql_table": data["mysql_table"],
            }
        }
        
        return jsonify({
            "message": f"Task created and started with PID: {pid}",
            "task_id": task_id,
            "status": "running",
            "pid": pid
        }), 201
        
    except Exception as e:
        print(f"Error creating task: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/tasks/<task_id>", methods=["GET"])
def get_task(task_id: str):
    """Get task status"""
    if task_id not in tasks:
        return jsonify({"error": "Task not found"}), 404
    
    task = tasks[task_id]
    
    # Check if process is still running
    if task["status"] == "running":
        try:
            process = psutil.Process(task["pid"])
            if not process.is_running():
                # Process exited, check exit code
                exit_code = process.returncode
                if exit_code == 0:
                    task["status"] = "completed"
                else:
                    task["status"] = "failed"
                    task["error"] = f"Vector exited with code {exit_code}"
                task["updated_at"] = datetime.now().isoformat()
        except psutil.NoSuchProcess:
            task["status"] = "completed"
            task["updated_at"] = datetime.now().isoformat()
    
    response = {
        "task_id": task["task_id"],
        "status": task["status"],
        "pid": task.get("pid"),
        "created_at": task["created_at"],
        "updated_at": task["updated_at"],
        "config": task.get("config", {}),
    }
    
    # Add error information if available
    if "error" in task:
        response["error"] = task["error"]
    
    return jsonify(response)


@app.route("/api/v1/tasks", methods=["GET"])
def list_tasks():
    """List all tasks"""
    return jsonify({
        "tasks": list(tasks.values())
    })


if __name__ == "__main__":
    print("Backup Manager Demo API server")
    print(f"Vector binary: {VECTOR_BINARY}")
    print(f"Config directory: {CONFIG_DIR}")
    print("Server starting on http://0.0.0.0:8080")
    app.run(host="0.0.0.0", port=8080, debug=True)
