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
- Vector delta_lake_watermark source → Reads from Delta Lake table in S3 with checkpoint support
- Vector transforms → Converts to slowlog format and applies VRL-based filtering
- Vector tidb sink → Writes data directly to MySQL/TiDB database

Features:
- Fault recovery: Checkpoint support enables resume from last processed record
- Incremental sync: Only processes new data since last checkpoint
- At-least-once delivery: Acknowledgment mechanism ensures data reliability
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
from typing import Optional, Dict, List, Tuple
from flask import Flask, request, jsonify
from flask_cors import CORS
import psutil
import toml
import boto3
from botocore.exceptions import ClientError

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
    processor_script: Optional[Path],  # Not used anymore, kept for compatibility
    mysql_connection: str,
    mysql_table: str,
    s3_bucket: str,
    s3_prefix: str,
    s3_region: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    filter_keywords: Optional[List[str]] = None,
    unique_id_column: Optional[str] = None,  # Optional unique ID column for precise sync
    order_by_column: Optional[str] = None,  # Optional: column name for ordering (default: "time")
    condition: Optional[str] = None,  # Optional: SQL WHERE condition for source-level filtering
    use_transform: bool = True,  # Optional: whether to use transform to convert to slowlog format (default: True)
) -> str:
    """Generate Vector TOML configuration for slowlog backup using delta_lake_watermark source
    
    This function ONLY generates Vector configuration. It does NOT process any data.
    
    Configuration structure:
    1. delta_lake_watermark source: Reads from Delta Lake table in S3 with checkpoint support
       - Supports incremental sync with fault recovery
       - Uses DuckDB to query Delta Lake tables with SQL WHERE conditions (predicate pushdown)
       - Automatically handles checkpointing for resume capability
       - Supports source-level filtering via 'condition' parameter (more efficient than transform filtering)
    2. remap transform: Converts Delta Lake records to slowlog format
    3. tidb sink: Writes data directly to MySQL/TiDB database
    
    Note: All data processing is done by Vector, not by this management API.
    The delta_lake_watermark source provides built-in checkpoint support for fault recovery.
    
    Args:
        order_by_column: Column name for ordering (default: "time"). This should be a timestamp column.
        condition: SQL WHERE condition for source-level filtering (e.g., "type = 'error' AND severity > 3").
                   This is more efficient than filtering in transforms because it uses predicate pushdown.
        filter_keywords: DEPRECATED - Use 'condition' parameter instead for better performance.
                        If provided, will be converted to SQL condition for source-level filtering.
        use_transform: Whether to use transform to convert Delta Lake records to slowlog format (default: True).
                       Set to False if MySQL table structure matches Delta Lake table structure.
                       When False, tidb sink will automatically map Delta Lake fields to MySQL columns.
                       When True, transform combines multiple fields into a single 'log_line' text field.
    """
    
    # Generate Vector config - uses delta_lake_watermark source
    # Create data_dir first (Vector requires it to exist, and checkpoint will be stored here)
    data_dir = Path(f"/tmp/vector-data/{task_id}")
    checkpoint_dir = data_dir / "checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    
    # Build Delta Lake table endpoint from S3 bucket and prefix
    # Remove trailing slash from prefix if present
    s3_prefix_clean = s3_prefix.rstrip('/')
    delta_table_endpoint = f"s3://{s3_bucket}/{s3_prefix_clean}"
    
    # Determine order_by_column (default to "time" if not provided)
    order_by_col = order_by_column or "time"
    
    # Build SQL condition for source-level filtering (more efficient than transform filtering)
    # Priority: 1. condition parameter, 2. filter_keywords (converted to SQL)
    sql_condition = condition
    if not sql_condition and filter_keywords:
        # Convert keyword filter to SQL condition (assuming keywords are in 'prev_stmt' or 'digest' column)
        # This uses predicate pushdown for better performance
        keyword_conditions = [f"(prev_stmt LIKE '%{kw}%' OR digest LIKE '%{kw}%')" for kw in filter_keywords]
        sql_condition = " OR ".join(keyword_conditions)
    
    # Configure delta_lake_watermark source
    # Note: unique_id_column is optional but recommended for precise incremental sync
    # If the table has a unique ID column (like id, uuid, request_id), specify it here
    # Otherwise, set to None and the source will use >= for checkpoint recovery
    delta_source_config = {
        "type": "delta_lake_watermark",
        "endpoint": delta_table_endpoint,
        "cloud_provider": "aws",
        "data_dir": str(checkpoint_dir),
        "order_by_column": order_by_col,  # Configurable column for ordering
        "batch_size": 10000,
        "poll_interval_secs": 0,  # 0 = sync once within time range then exit; >0 = continuous polling
        "duckdb_memory_limit": "2GB",
        "region": s3_region,  # AWS region for S3 (e.g. us-west-2), required for delta_lake_watermark S3 access
        "acknowledgements": True,  # source waits for sent events to be acked before exiting
    }
    
    # Set unique_id_column if provided
    # This enables precise incremental sync with no duplicates and no missed data
    if unique_id_column:
        delta_source_config["unique_id_column"] = unique_id_column
    
    # Add time range if provided
    if start_time:
        delta_source_config["begin_time"] = start_time
    if end_time:
        delta_source_config["end_time"] = end_time
    
    # Add SQL condition for source-level filtering (predicate pushdown - more efficient)
    if sql_condition:
        delta_source_config["condition"] = sql_condition
    
    config = {
        "data_dir": str(data_dir),
        
        "api": {
            "enabled": True,
            "address": "127.0.0.1:0",  # Random port for Vector API
        },
        
        "sources": {
            # Enable internal_metrics to see component metrics in vector top
            "internal_metrics": {
                "type": "internal_metrics",
            },
            
            "delta_lake_source": delta_source_config
        },
        
        "transforms": {}
    }
    
    # Determine if transform is needed
    # Transform is only needed if MySQL table structure doesn't match Delta Lake table structure
    # If MySQL table has columns matching Delta Lake fields (time, db, user, host, etc.),
    # tidb sink will automatically map them, so no transform is needed.
    # 
    # Current MySQL table structure (from create_mysql_table.sql):
    # - id (AUTO_INCREMENT)
    # - log_line (TEXT) - requires transform to combine multiple fields into text
    # - log_timestamp (DATETIME) - requires transform to convert time field
    # - task_id (VARCHAR) - requires transform to add task_id
    # - created_at (TIMESTAMP, auto-generated)
    #
    # If your MySQL table has columns matching Delta Lake fields directly (e.g., time, db, user, host),
    # you can skip the transform and let tidb sink handle the mapping automatically.
    
    if use_transform:
        # Transform is needed to convert structured Delta Lake records to slowlog text format
        # Delta Lake records have fields: time, db, user, host, query_time, result_rows, prev_stmt, digest, etc.
        # MySQL table expects: log_line (TEXT), log_timestamp (DATETIME), task_id (VARCHAR)
        config["transforms"]["format_slowlog"] = {
            "type": "remap",
            "inputs": ["delta_lake_source"],
            "source": f"""
                # Convert Delta Lake record to slowlog format
                # Use dynamic order_by_column ({order_by_col}) for timestamp field
                time_str = string!(.{order_by_col} ?? "")
                db_str = string!(.db ?? "")
                user_str = string!(.user ?? "")
                host_str = string!(.host ?? "")
                query_time_str = string!(.query_time ?? "")
                result_rows_str = string!(.result_rows ?? "")
                sql_str = string!(.prev_stmt ?? "") ?? string!(.digest ?? "")
                
                message = "# Time: " + time_str + " | DB: " + db_str + " | User: " + user_str + "@" + host_str + " | Query_time: " + query_time_str + " | Rows: " + result_rows_str + " | SQL: " + sql_str
                
                # Set log_timestamp from order_by_column field (convert Unix timestamp to ISO 8601)
                # Use dynamic field name based on order_by_column configuration
                # Note: 'timestamp' is a reserved keyword in VRL, so we use 'log_timestamp' instead
                # Also set @timestamp for Vector's internal timestamp handling
                log_timestamp = if exists(.{order_by_col}) {{ format_timestamp!(to_int!(.{order_by_col}) ?? 0, format: "%+") }} else {{ now() }}
                .@timestamp = log_timestamp
                
                source = "delta_lake"
                task_id = get_env_var("TASK_ID") ?? ""
            """
        }
        sink_input = "format_slowlog"
    else:
        # No transform needed - tidb sink will automatically map Delta Lake fields to MySQL columns
        # Make sure MySQL table has columns matching Delta Lake field names (time, db, user, host, etc.)
        # tidb sink supports automatic field mapping (case-insensitive)
        # 
        # Example MySQL table structure that matches Delta Lake:
        # CREATE TABLE slowlogs (
        #     id BIGINT AUTO_INCREMENT PRIMARY KEY,
        #     time BIGINT,  -- matches Delta Lake 'time' field
        #     db VARCHAR(255),  -- matches Delta Lake 'db' field
        #     user VARCHAR(255),  -- matches Delta Lake 'user' field
        #     host VARCHAR(255),  -- matches Delta Lake 'host' field
        #     query_time FLOAT,  -- matches Delta Lake 'query_time' field
        #     result_rows INT,  -- matches Delta Lake 'result_rows' field
        #     prev_stmt TEXT,  -- matches Delta Lake 'prev_stmt' field
        #     digest VARCHAR(255),  -- matches Delta Lake 'digest' field
        #     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        # );
        sink_input = "delta_lake_source"
    
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


def generate_sync_logs_vector_config(
    task_id: str,
    source_bucket: str,
    dest_bucket: str,
    dest_prefix: str,
    *,
    cluster_id: Optional[str] = None,
    project_id: Optional[str] = None,
    types: Optional[List[str]] = None,
    source_prefix: Optional[str] = None,
    pattern: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    max_keys: int = 10000,
    cloud_provider: str = "aws",
    region: Optional[str] = "us-west-2",
    max_file_bytes: int = 32 * 1024 * 1024,
    content_format: str = "text",
    raw_log_components: Optional[List[str]] = None,
    dest_aws_access_key_id: Optional[str] = None,
    dest_aws_secret_access_key: Optional[str] = None,
    dest_aws_session_token: Optional[str] = None,
    output_format: str = "text",
    parse_lines: bool = False,
    line_parse_regexes: Optional[List[str]] = None,
) -> str:
    """Generate Vector config for syncing log files.

    Full flow in Vector: file_list fetch and decompress, official aws_s3 sink writes to target bucket by key_prefix template.
    output_format is the encoding for S3 (text/json/csv etc). parse_lines=True enables per-line parsing; line_parse_regexes (list of regexes with (?P<name>...) capture groups) uses custom regex only, else built-in Python/HTTP rules.

    Two modes:
    1) types mode: cluster_id, project_id, types (e.g. ["raw_logs"]), start_time, end_time
    2) prefix mode: source_prefix, optionally pattern and start_time/end_time
    """
    endpoint = f"s3://{source_bucket}"
    data_dir = Path(f"/tmp/vector-data/{task_id}")
    data_dir.mkdir(parents=True, exist_ok=True)

    file_list_source = {
        "type": "file_list",
        "endpoint": endpoint,
        "cloud_provider": cloud_provider,
        "max_keys": max_keys,
        "poll_interval_secs": 0,  # one-shot
        "emit_metadata": True,
        "emit_content": True,
        "emit_per_line": bool(parse_lines),
        "decompress_gzip": True,
    }
    if line_parse_regexes:
        file_list_source["line_parse_regexes"] = line_parse_regexes
    if region:
        file_list_source["region"] = region

    if types and len(types) > 0:
        file_list_source["cluster_id"] = cluster_id
        if project_id:
            file_list_source["project_id"] = project_id
        file_list_source["types"] = types
        if start_time:
            file_list_source["start_time"] = start_time
        if end_time:
            file_list_source["end_time"] = end_time
        if raw_log_components:
            file_list_source["raw_log_components"] = raw_log_components
    else:
        if not source_prefix:
            raise ValueError("sync_logs: provide source_prefix or types")
        file_list_source["prefix"] = source_prefix.rstrip("/") + "/"
        if pattern:
            file_list_source["pattern"] = pattern
        if start_time:
            file_list_source["time_range_start"] = start_time
        if end_time:
            file_list_source["time_range_end"] = end_time

    dest_prefix_normalized = dest_prefix.rstrip("/") + "/" if dest_prefix else ""
    # Official aws_s3 supported codecs: text, json, csv, logfmt, raw_message, syslog, gelf (avro/cef/protobuf need schema, not supported)
    SUPPORTED_OUTPUT_FORMATS = ("text", "json", "csv", "logfmt", "raw_message", "syslog", "gelf")
    fmt = (output_format or "text").lower()
    if fmt not in SUPPORTED_OUTPUT_FORMATS:
        raise ValueError(
            f"output_format must be one of {', '.join(SUPPORTED_OUTPUT_FORMATS)}, got {output_format}; "
            "avro/cef/protobuf require schema config, not supported"
        )

    # Use official aws_s3: key_prefix template {{ component }}/{{ hour_partition }}/; encoding from output_format
    aws_s3_sink = {
        "type": "aws_s3",
        "inputs": ["file_list"],
        "bucket": dest_bucket,
        "key_prefix": dest_prefix_normalized + "{{ component }}/{{ hour_partition }}/",
        "encoding": {"codec": fmt},
        # Short timeout_secs: default 300s waits too long for small batches; 10s for faster flush
        "batch": {"max_bytes": max_file_bytes, "timeout_secs": 10},
        "compression": "gzip",
    }
    if fmt == "csv":
        # With parse_lines: each record has line_type, log_timestamp, logger, level, tag, message_body (Python) or client_ip, method, path, status (HTTP), for column filtering
        aws_s3_sink["encoding"]["csv"] = {
            "fields": (
                [
                    "file_path", "data_type", "hour_partition", "component",
                    "line_type", "log_timestamp", "logger", "level", "tag", "message_body",
                    "client_ip", "request_date", "method", "path", "protocol", "status", "response_size",
                    "message",
                    "file_size", "last_modified", "bucket", "full_path",
                    "@timestamp",
                ]
                if parse_lines
                else [
                    "file_path", "data_type", "hour_partition", "component",
                    "file_size", "last_modified", "bucket", "full_path",
                    "@timestamp", "message",
                ]
            ),
        }
    if region:
        aws_s3_sink["region"] = region
    if dest_aws_access_key_id and dest_aws_secret_access_key:
        aws_s3_sink["auth"] = {
            "access_key_id": dest_aws_access_key_id,
            "secret_access_key": dest_aws_secret_access_key,
        }
        if dest_aws_session_token:
            aws_s3_sink["auth"]["session_token"] = dest_aws_session_token

    config = {
        "data_dir": str(data_dir),
        "api": {"enabled": True, "address": "127.0.0.1:0"},
        "sources": {"file_list": file_list_source},
        "sinks": {"to_s3": aws_s3_sink},
    }
    return toml.dumps(config)


def generate_sync_logs_to_mysql_config(
    task_id: str,
    source_bucket: str,
    mysql_connection: str,
    mysql_table: str,
    *,
    cluster_id: Optional[str] = None,
    project_id: Optional[str] = None,
    types: Optional[List[str]] = None,
    source_prefix: Optional[str] = None,
    pattern: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    max_keys: int = 10000,
    cloud_provider: str = "aws",
    region: Optional[str] = None,
    raw_log_components: Optional[List[str]] = None,
    max_file_bytes: int = 32 * 1024 * 1024,
    content_format: str = "text",
    parse_lines: bool = False,
    line_parse_regexes: Optional[List[str]] = None,
) -> str:
    """Generate Vector config with file_list source + tidb sink, writing parsed log lines to local MySQL/TiDB.

    Same source and parse params as sync-logs (types/raw_log_components/time_range, parse_lines, line_parse_regexes),
    but writes to MySQL table; tidb sink maps event fields to columns by table schema.
    Table schema must match event fields; see demo/config/create_parsed_logs_table.sql.
    """
    endpoint = f"s3://{source_bucket}"
    data_dir = Path(f"/tmp/vector-data/{task_id}")
    data_dir.mkdir(parents=True, exist_ok=True)

    file_list_source = {
        "type": "file_list",
        "endpoint": endpoint,
        "cloud_provider": cloud_provider,
        "max_keys": max_keys,
        "poll_interval_secs": 0,
        "emit_metadata": True,
        "emit_content": True,
        "emit_per_line": bool(parse_lines),
        "decompress_gzip": True,
    }
    if line_parse_regexes:
        file_list_source["line_parse_regexes"] = line_parse_regexes
    if region:
        file_list_source["region"] = region

    if types and len(types) > 0:
        file_list_source["cluster_id"] = cluster_id
        if project_id:
            file_list_source["project_id"] = project_id
        file_list_source["types"] = types
        if start_time:
            file_list_source["start_time"] = start_time
        if end_time:
            file_list_source["end_time"] = end_time
        if raw_log_components:
            file_list_source["raw_log_components"] = raw_log_components
    else:
        if not source_prefix:
            raise ValueError("sync_logs_to_mysql: provide source_prefix or types")
        file_list_source["prefix"] = source_prefix.rstrip("/") + "/"
        if pattern:
            file_list_source["pattern"] = pattern
        if start_time:
            file_list_source["time_range_start"] = start_time
        if end_time:
            file_list_source["time_range_end"] = end_time

    # tidb sink: same connection string parsing as generate_vector_config
    mysql_parts = mysql_connection.replace("mysql://", "").split("@")
    user_pass = mysql_parts[0].split(":")
    mysql_user, mysql_pass = user_pass[0], user_pass[1] if len(user_pass) > 1 else ""
    host_port = mysql_parts[1].split("/")
    host_port_parts = host_port[0].split(":")
    mysql_host = host_port_parts[0]
    mysql_port = int(host_port_parts[1]) if len(host_port_parts) > 1 else 3306
    mysql_database = host_port[1]
    tidb_connection_string = f"mysql://{mysql_user}:{mysql_pass}@{mysql_host}:{mysql_port}/{mysql_database}"

    config = {
        "data_dir": str(data_dir),
        "api": {"enabled": True, "address": "127.0.0.1:0"},
        "sources": {"file_list": file_list_source},
        "sinks": {
            "tidb_sink": {
                "type": "tidb",
                "inputs": ["file_list"],
                "connection_string": tidb_connection_string,
                "table": mysql_table,
                "batch_size": 1000,
                "max_connections": 10,
                "connection_timeout": 30,
            }
        },
    }
    return toml.dumps(config)


def run_vector_sync(
    task_id: str,
    config_content: str,
    vector_binary: str,
    timeout_secs: int = 300,
    env_extra: Optional[Dict[str, str]] = None,
) -> Tuple[bool, Optional[str], Optional[Path]]:
    """Run Vector synchronously and wait for exit. Returns (success, error_msg, vector_log_path).
    Logs are written to log_file in real time; tail -f during execution.
    """
    config_file = CONFIG_DIR / f"{task_id}_sync_logs.toml"
    log_file = CONFIG_DIR / f"{task_id}_sync_logs.log"
    config_file.write_text(config_content)
    env = os.environ.copy()
    if env_extra:
        env.update(env_extra)
    env["TASK_ID"] = task_id
    cmd = [vector_binary, "--config", str(config_file)]
    try:
        # Stream Vector stdout/stderr to file for tail -f
        with open(log_file, "w", encoding="utf-8") as f:
            f.write("=== Vector (stdout + stderr) ===\n")
            f.flush()
            proc = subprocess.Popen(
                cmd,
                stdout=f,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
            )
            try:
                proc.wait(timeout=timeout_secs)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
                return False, f"Vector timeout ({timeout_secs}s)", log_file
        if proc.returncode != 0:
            err = _read_tail(log_file, max_chars=500)
            return False, err or f"Vector exited with code {proc.returncode}", log_file
        return True, None, log_file
    except Exception as e:
        return False, str(e), None


def _read_tail(path: Path, max_chars: int = 500) -> str:
    """Read up to max_chars from the end of the file (for error message)."""
    if not path.exists():
        return ""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            f.seek(0, 2)
            size = f.tell()
            if size <= max_chars:
                f.seek(0)
                return f.read()
            f.seek(size - max_chars)
            return f.read()
    except Exception:
        return ""


def parse_file_list_output(output_path: Path) -> List[str]:
    """Parse file_path list from file_list file sink output (JSONL)."""
    if not output_path.exists():
        return []
    keys = []
    for line in output_path.read_text().strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            # file_list event field: file_path is bucket-relative path
            path = obj.get("file_path") or obj.get("full_path")
            if path:
                keys.append(path)
        except json.JSONDecodeError:
            continue
    return keys


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
    - Data processing is done by Vector's delta_lake_watermark source
    - MySQL import is handled directly by Vector's tidb sink
    - No background thread needed anymore
    - Checkpoint support enables fault recovery
    """
    
    # Use provided vector_binary or fallback to VECTOR_BINARY
    vector_cmd = vector_binary if vector_binary else VECTOR_BINARY
    
    # Write config to temporary file
    config_file = CONFIG_DIR / f"{task_id}.toml"
    config_file.write_text(config_content)
    
    # Prepare environment variables
    # Merge script_env with current environment
    # For delta_lake_watermark source, we need AWS credentials for S3 access
    env = os.environ.copy()
    if script_env:
        env.update(script_env)
    
    # Add TASK_ID to environment for transforms
    env["TASK_ID"] = task_id
    
    # Start Vector process
    # Note: Vector will inherit environment variables (AWS_ACCESS_KEY_ID, etc.)
    # for delta_lake_watermark source to access S3
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
            # Convert ISO 8601 strings to Unix timestamps (seconds)
            # Delta Lake time column is typically Unix timestamp (numeric)
            from datetime import datetime
            start_str = time_range.get("start")
            end_str = time_range.get("end")
            if start_str:
                try:
                    # Parse ISO 8601 and convert to Unix timestamp
                    dt = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
                    start_time = str(int(dt.timestamp()))
                except (ValueError, AttributeError):
                    # If conversion fails, use original string (might be already a timestamp)
                    start_time = start_str
            if end_str:
                try:
                    # Parse ISO 8601 and convert to Unix timestamp
                    dt = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
                    end_time = str(int(dt.timestamp()))
                except (ValueError, AttributeError):
                    # If conversion fails, use original string (might be already a timestamp)
                    end_time = end_str
        
        # Extract optional parameters
        unique_id_column = data.get("unique_id_column")  # Optional: "id", "uuid", "digest", etc.
        order_by_column = data.get("order_by_column")  # Optional: column name for ordering (default: "time")
        condition = data.get("condition")  # Optional: SQL WHERE condition for source-level filtering
        use_transform = data.get("use_transform", True)  # Optional: whether to use transform (default: True)
        
        # Step 1: Generate Vector configuration
        # Using delta_lake_watermark source for fault recovery support
        # No need for processor script anymore - delta_lake_watermark handles everything
        print(f"[Task {task_id}] Step 1: Generating Vector configuration with delta_lake_watermark source...")
        vector_config = generate_vector_config(
            task_id=task_id,
            processor_script=None,  # Not needed anymore
            mysql_connection=data["mysql_connection"],
            mysql_table=data["mysql_table"],
            s3_bucket=data["s3_bucket"],
            s3_prefix=data["s3_prefix"],
            s3_region=data.get("s3_region", "us-west-2"),
            start_time=start_time,
            end_time=end_time,
            filter_keywords=data.get("filter_keywords"),  # DEPRECATED: Use 'condition' instead
            unique_id_column=unique_id_column,  # Optional: for precise incremental sync
            order_by_column=order_by_column,  # Optional: column name for ordering (default: "time")
            condition=condition,  # Optional: SQL WHERE condition for source-level filtering (more efficient)
            use_transform=use_transform,  # Optional: whether to use transform (default: True)
        )
        
        # Step 2: Start Vector process
        print(f"[Task {task_id}] Step 2: Starting Vector process...")
        
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
        
        # Prepare environment variables
        # For delta_lake_watermark source, we need AWS credentials and region for S3 access
        s3_region = data.get("s3_region", "us-west-2")
        script_env = {
            "TASK_ID": task_id,  # For transforms to use
            "AWS_REGION": s3_region,  # Required for delta_lake_watermark S3 access (DuckDB uses this)
            # AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY should be set in the environment or via IAM roles
        }
        
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


def list_s3_files_with_boto3(
    bucket: str,
    prefix: str,
    pattern: Optional[str] = None,
    time_range_start: Optional[str] = None,
    time_range_end: Optional[str] = None,
    max_keys: int = 10000,
) -> List[Dict[str, any]]:
    """List files from S3 bucket using boto3 with filtering
    
    Returns list of file metadata dictionaries.
    """
    s3_client = boto3.client('s3')
    
    files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    # Parse time range
    start_dt = None
    end_dt = None
    if time_range_start:
        try:
            start_dt = datetime.fromisoformat(time_range_start.replace('Z', '+00:00'))
        except:
            pass
    if time_range_end:
        try:
            end_dt = datetime.fromisoformat(time_range_end.replace('Z', '+00:00'))
        except:
            pass
    
    # Compile pattern if provided
    import re
    pattern_regex = None
    if pattern:
        # Convert glob pattern to regex
        regex_str = pattern.replace('*', '.*').replace('?', '.')
        regex_str = regex_str.replace('{YYYYMMDDHH}', r'\d{10}')
        pattern_regex = re.compile(f'^{regex_str}$')
    
    try:
        page_iterator = paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=1000  # S3 API limit per page
        )
        
        for page in page_iterator:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                key = obj['Key']
                last_modified = obj['LastModified']
                size = obj['Size']
                
                # Filter by time range
                if start_dt and last_modified < start_dt:
                    continue
                if end_dt and last_modified > end_dt:
                    continue
                
                # Filter by pattern
                if pattern_regex and not pattern_regex.search(key):
                    continue
                
                files.append({
                    "key": key,
                    "size": size,
                    "last_modified": last_modified.isoformat(),
                })
                
                if len(files) >= max_keys:
                    break
            
            if len(files) >= max_keys:
                break
                
    except ClientError as e:
        raise Exception(f"Failed to list S3 files: {str(e)}")
    
    return files


def copy_s3_files_with_boto3(
    source_bucket: str,
    source_keys: List[str],
    dest_bucket: str,
    dest_prefix: str,
    source_prefix: Optional[str] = None,
) -> Dict[str, any]:
    """Copy files from source S3 bucket to destination using boto3
    
    Args:
        source_bucket: Source S3 bucket name
        source_keys: List of source S3 keys to copy
        dest_bucket: Destination S3 bucket name
        dest_prefix: Destination prefix (files will be copied under this prefix)
        source_prefix: Optional source prefix to remove from keys when building dest path
    
    Returns:
        Dict with copy results: {"copied": count, "failed": count, "errors": [...]}
    """
    s3_client = boto3.client('s3')
    
    copied = 0
    failed = 0
    errors = []
    
    dest_prefix = dest_prefix.rstrip('/')
    if source_prefix:
        source_prefix = source_prefix.rstrip('/')
    
    for source_key in source_keys:
        try:
            # Remove leading slash if present
            source_key = source_key.lstrip('/')
            
            # Build destination key
            # If source_prefix is provided, remove it from source_key to get relative path
            if source_prefix and source_key.startswith(source_prefix):
                relative_path = source_key[len(source_prefix):].lstrip('/')
                dest_key = f"{dest_prefix}/{relative_path}" if relative_path else dest_prefix
            else:
                # Use full source key under dest_prefix
                dest_key = f"{dest_prefix}/{source_key}"
            
            # Copy object (server-side copy, no data transfer through our server)
            copy_source = {
                'Bucket': source_bucket,
                'Key': source_key
            }
            
            s3_client.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket,
                Key=dest_key
            )
            
            copied += 1
            if copied % 100 == 0:
                print(f"[S3 Copy] Progress: {copied}/{len(source_keys)} files copied...")
            else:
                print(f"[S3 Copy] ✓ Copied s3://{source_bucket}/{source_key} -> s3://{dest_bucket}/{dest_key}")
            
        except ClientError as e:
            failed += 1
            error_msg = f"Failed to copy {source_key}: {str(e)}"
            errors.append(error_msg)
            print(f"[S3 Copy] ❌ {error_msg}")
        except Exception as e:
            failed += 1
            error_msg = f"Unexpected error copying {source_key}: {str(e)}"
            errors.append(error_msg)
            print(f"[S3 Copy] ❌ {error_msg}")
    
    return {
        "copied": copied,
        "failed": failed,
        "errors": errors[:10]  # Limit to first 10 errors
    }


@app.route("/api/v1/sync-logs", methods=["POST"])
def sync_logs():
    """Sync logs: Vector does full flow (file_list fetch+decompress -> official aws_s3 write to target bucket by key_prefix template).

    Demo only generates Vector config and runs Vector; no copy logic.

    Request body (choose one):
    A) By type (e.g. TiDB raw_logs):
    {
        "source_bucket": "my-bucket",
        "dest_bucket": "dest-bucket",
        "dest_prefix": "backup/logs/",
        "cluster_id": "10324983984131567830",
        "project_id": "1372813089209061633",
        "types": ["raw_logs"],
        "time_range": { "start": "2026-01-08T00:00:00Z", "end": "2026-01-08T23:59:59Z" },
        "region": "us-west-2",
        "max_keys": 10000,
        "max_file_bytes": 33554432,
        "content_format": "text",
        "dest_aws_access_key_id": "...",
        "dest_aws_secret_access_key": "...",
        "dest_aws_session_token": "..."
    }
    dest_aws_* optional; if set, sink uses these creds for dest bucket; source bucket still uses env vars.
    B) By prefix:
    {
        "source_bucket": "my-bucket",
        "source_prefix": "path/to/logs/",
        "dest_bucket": "dest-bucket",
        "dest_prefix": "backup/",
        "pattern": "*.log.gz",
        "time_range": { "start": "...", "end": "..." },
        "region": "us-west-2",
        "max_keys": 10000
    }
    region optional, default "us-west-2". Output under dest_bucket/dest_prefix, partitioned by component/hour_partition.
    output_format optional, default "text": S3 encoding (text/json/csv etc). parse_lines optional, default false. line_parse_regexes optional: list of regex strings with (?P<name>...) captures; if omitted, built-in Python/HTTP rules. dest_bucket and dest_prefix required.
    timeout_secs optional, default 3600: max Vector subprocess time; increase for large ranges.

    Credentials: source bucket uses AWS creds from env; dest bucket can use dest_aws_access_key_id, dest_aws_secret_access_key, dest_aws_session_token for separate read-only source + writable dest.

    Vector logs: stdout/stderr written to CONFIG_DIR/{task_id}_sync_logs.log (default /tmp/vector-tasks/). Response returns vector_log_path. If success but no files in dest, check logs:
    - file_list found files (keywords file_list_files_found_total, list_files_at)
    - source path correct (raw_logs: diagnosis/data/{cluster_id}/merged-logs/{YYYYMMDDHH}/{component}/*.log)
    - aws_s3 template_failed etc (events dropped if component/hour_partition missing)
    """
    try:
        data = request.json or {}
        source_bucket = data.get("source_bucket")
        dest_bucket = data.get("dest_bucket")
        dest_prefix = data.get("dest_prefix", "")
        output_format = (data.get("output_format") or "text").lower()
        parse_lines = bool(data.get("parse_lines"))
        line_parse_regexes = data.get("line_parse_regexes")  # optional list of regex strings
        if not source_bucket or not dest_bucket:
            return jsonify({"error": "source_bucket and dest_bucket required"}), 400
        _supported = ("text", "json", "csv", "logfmt", "raw_message", "syslog", "gelf")
        if output_format not in _supported:
            return jsonify({"error": f"output_format must be one of {', '.join(_supported)}; avro/cef/protobuf require schema"}), 400

        task_id = str(uuid.uuid4())
        time_range = data.get("time_range") or {}
        start_time = time_range.get("start")
        end_time = time_range.get("end")
        max_keys = data.get("max_keys", 10000)
        cloud_provider = data.get("cloud_provider", "aws")
        region = data.get("region", "us-west-2")
        max_file_bytes = data.get("max_file_bytes", 32 * 1024 * 1024)
        content_format = data.get("content_format", "text")
        raw_log_components = data.get("raw_log_components") or data.get("components")
        timeout_secs = data.get("timeout_secs", 3600)
        dest_aws_access_key_id = data.get("dest_aws_access_key_id")
        dest_aws_secret_access_key = data.get("dest_aws_secret_access_key")
        dest_aws_session_token = data.get("dest_aws_session_token")

        types = data.get("types")
        if types and len(types) > 0:
            cluster_id = data.get("cluster_id")
            project_id = data.get("project_id")
            if not cluster_id:
                return jsonify({"error": "cluster_id required when using types"}), 400
            if not start_time or not end_time:
                return jsonify({"error": "time_range.start and time_range.end required when using types (e.g. raw_logs)"}), 400
            source_prefix = None
            pattern = None
        else:
            source_prefix = data.get("source_prefix")
            if not source_prefix:
                return jsonify({"error": "provide source_prefix or types"}), 400
            pattern = data.get("pattern")
            cluster_id = project_id = None

        vector_binary_path = Path(VECTOR_BINARY)
        if not vector_binary_path.exists() or not os.access(vector_binary_path, os.X_OK):
            project_root = Path(__file__).parent.parent
            for name in ("debug", "release"):
                candidate = project_root / "target" / name / "vector"
                if candidate.exists() and os.access(candidate, os.X_OK):
                    vector_binary_path = candidate
                    break
        if not vector_binary_path.exists() or not os.access(vector_binary_path, os.X_OK):
            return jsonify({"error": "Vector binary not found; build first"}), 500
        vector_binary = str(vector_binary_path.resolve())

        config_content = generate_sync_logs_vector_config(
            task_id=task_id,
            source_bucket=source_bucket,
            dest_bucket=dest_bucket,
            dest_prefix=dest_prefix,
            cluster_id=cluster_id,
            project_id=project_id,
            types=types,
            source_prefix=source_prefix,
            pattern=pattern,
            start_time=start_time,
            end_time=end_time,
            max_keys=max_keys,
            cloud_provider=cloud_provider,
            region=region,
            raw_log_components=raw_log_components,
            max_file_bytes=max_file_bytes,
            content_format=content_format,
            dest_aws_access_key_id=dest_aws_access_key_id,
            dest_aws_secret_access_key=dest_aws_secret_access_key,
            dest_aws_session_token=dest_aws_session_token,
            output_format=output_format,
            parse_lines=parse_lines,
            line_parse_regexes=line_parse_regexes,
        )

        ok, err, vector_log_path = run_vector_sync(task_id, config_content, vector_binary, timeout_secs=timeout_secs)
        if not ok:
            return jsonify({"error": f"Vector failed: {err}", "task_id": task_id}), 500

        log_path_str = str(vector_log_path) if vector_log_path else None
        tasks[task_id] = {
            "task_id": task_id,
            "status": "completed",
            "type": "sync_logs",
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "config": {
                "source_bucket": source_bucket,
                "dest_bucket": dest_bucket,
                "dest_prefix": dest_prefix.rstrip("/") + "/" if dest_prefix else "",
                "output_format": output_format,
                "parse_lines": parse_lines,
                "line_parse_regexes": line_parse_regexes,
            },
            "result": {
                "message": "Done by Vector file_list + aws_s3 sink (key_prefix template); output in dest bucket by component/hour_partition, encoding "
                + output_format
                + (", line parsing enabled" if parse_lines else ""),
                "vector_log_path": log_path_str,
            },
        }

        return jsonify({
            "message": "Sync done (Vector file_list fetch+decompress + aws_s3 write by component/hour, encoding " + output_format + ")",
            "task_id": task_id,
            "status": "completed",
            "dest_bucket": dest_bucket,
            "dest_prefix": dest_prefix.rstrip("/") + "/" if dest_prefix else "",
            "output_format": output_format,
            "vector_log_path": log_path_str,
        }), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/sync-logs-to-mysql", methods=["POST"])
def sync_logs_to_mysql():
    """Fetch logs from S3 (file_list), parse per line, write to local MySQL/TiDB (tidb sink).

    Request body: same source/parse params as sync-logs; additionally require mysql_connection, mysql_table. No dest_bucket/dest_prefix.
    Table schema must match event fields; tidb sink maps by column name (case-insensitive). Example: demo/config/create_parsed_logs_table.sql.

    Example request:
    {
        "source_bucket": "my-bucket",
        "cluster_id": "10324983984131567830",
        "types": ["raw_logs"],
        "time_range": { "start": "2026-01-08T00:00:00Z", "end": "2026-01-08T01:00:00Z" },
        "raw_log_components": ["loki", "operator"],
        "parse_lines": true,
        "line_parse_regexes": [],   // optional; if omitted, use built-in Python/HTTP rules
        "mysql_connection": "mysql://root:root@localhost:3306/testdb",
        "mysql_table": "parsed_logs",
        "max_keys": 10000,
        "region": "us-west-2",
        "timeout_secs": 3600
    }
    """
    try:
        data = request.json or {}
        source_bucket = data.get("source_bucket")
        mysql_connection = data.get("mysql_connection")
        mysql_table = data.get("mysql_table")
        parse_lines = bool(data.get("parse_lines"))
        line_parse_regexes = data.get("line_parse_regexes")

        if not source_bucket:
            return jsonify({"error": "source_bucket required"}), 400
        if not mysql_connection or not mysql_table:
            return jsonify({"error": "mysql_connection and mysql_table required"}), 400

        task_id = str(uuid.uuid4())
        time_range = data.get("time_range") or {}
        start_time = time_range.get("start")
        end_time = time_range.get("end")
        max_keys = data.get("max_keys", 10000)
        cloud_provider = data.get("cloud_provider", "aws")
        region = data.get("region", "us-west-2")
        raw_log_components = data.get("raw_log_components") or data.get("components")
        timeout_secs = data.get("timeout_secs", 3600)

        types = data.get("types")
        if types and len(types) > 0:
            cluster_id = data.get("cluster_id")
            project_id = data.get("project_id")
            if not cluster_id:
                return jsonify({"error": "cluster_id required when using types"}), 400
            if not start_time or not end_time:
                return jsonify({"error": "time_range.start and time_range.end required when using types (e.g. raw_logs)"}), 400
            source_prefix = None
            pattern = None
        else:
            source_prefix = data.get("source_prefix")
            if not source_prefix:
                return jsonify({"error": "provide source_prefix or types"}), 400
            pattern = data.get("pattern")
            cluster_id = project_id = None

        vector_binary_path = Path(VECTOR_BINARY)
        if not vector_binary_path.exists() or not os.access(vector_binary_path, os.X_OK):
            project_root = Path(__file__).parent.parent
            for name in ("debug", "release"):
                candidate = project_root / "target" / name / "vector"
                if candidate.exists() and os.access(candidate, os.X_OK):
                    vector_binary_path = candidate
                    break
        if not vector_binary_path.exists() or not os.access(vector_binary_path, os.X_OK):
            return jsonify({"error": "Vector binary not found; build first"}), 500
        vector_binary = str(vector_binary_path.resolve())

        config_content = generate_sync_logs_to_mysql_config(
            task_id=task_id,
            source_bucket=source_bucket,
            mysql_connection=mysql_connection,
            mysql_table=mysql_table,
            cluster_id=cluster_id,
            project_id=project_id,
            types=types,
            source_prefix=source_prefix,
            pattern=pattern,
            start_time=start_time,
            end_time=end_time,
            max_keys=max_keys,
            cloud_provider=cloud_provider,
            region=region,
            raw_log_components=raw_log_components,
            parse_lines=parse_lines,
            line_parse_regexes=line_parse_regexes,
        )

        ok, err, vector_log_path = run_vector_sync(task_id, config_content, vector_binary, timeout_secs=timeout_secs)
        if not ok:
            return jsonify({"error": f"Vector failed: {err}", "task_id": task_id}), 500

        log_path_str = str(vector_log_path) if vector_log_path else None
        tasks[task_id] = {
            "task_id": task_id,
            "status": "completed",
            "type": "sync_logs_to_mysql",
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "config": {
                "source_bucket": source_bucket,
                "mysql_connection": "mysql://***@.../" + mysql_connection.split("/")[-1] if "/" in mysql_connection else "***",
                "mysql_table": mysql_table,
                "parse_lines": parse_lines,
                "line_parse_regexes": line_parse_regexes,
            },
            "result": {"message": "file_list fetch + line parsing, tidb sink writes to MySQL", "vector_log_path": log_path_str},
        }

        return jsonify({
            "message": "Sync done; parsed logs written to MySQL table",
            "task_id": task_id,
            "status": "completed",
            "mysql_table": mysql_table,
            "vector_log_path": log_path_str,
        }), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/v1/copy-files", methods=["POST"])
def copy_files():
    """Copy files from source S3 bucket to destination S3 bucket
    
    Request body:
    {
        "source_bucket": "my-source-bucket",
        "source_prefix": "path/to/files/",
        "dest_bucket": "my-dest-bucket",
        "dest_prefix": "backup/",
        "pattern": "{YYYYMMDDHH}/*.log",  # Optional
        "time_range": {  # Optional
            "start": "2026-01-08T00:00:00Z",
            "end": "2026-01-08T23:59:59Z"
        },
        "max_keys": 10000  # Optional, default 10000
    }
    
    This endpoint:
    1. Uses boto3 to list files from source bucket
    2. Uses boto3 to copy files to destination bucket
    """
    try:
        data = request.json
        
        # Validate required fields
        required_fields = ["source_bucket", "source_prefix", "dest_bucket", "dest_prefix"]
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400
        
        task_id = str(uuid.uuid4())
        
        # Extract optional parameters
        pattern = data.get("pattern")
        time_range = data.get("time_range")
        time_range_start = None
        time_range_end = None
        if time_range:
            time_range_start = time_range.get("start")
            time_range_end = time_range.get("end")
        max_keys = data.get("max_keys", 10000)
        
        print(f"[Copy Task {task_id}] Step 1: Listing files from s3://{data['source_bucket']}/{data['source_prefix']}...")
        
        # Step 1: List files using boto3 (more reliable than Vector for this use case)
        file_list = list_s3_files_with_boto3(
            bucket=data["source_bucket"],
            prefix=data["source_prefix"],
            pattern=pattern,
            time_range_start=time_range_start,
            time_range_end=time_range_end,
            max_keys=max_keys,
        )
        
        if not file_list:
            return jsonify({
                "message": "No files found matching criteria",
                "task_id": task_id,
                "files_found": 0,
                "copied": 0
            }), 200
        
        print(f"[Copy Task {task_id}] Found {len(file_list)} files, starting copy...")
        
        # Step 2: Copy files using boto3
        source_keys = [f["key"] for f in file_list]
        copy_result = copy_s3_files_with_boto3(
            source_bucket=data["source_bucket"],
            source_keys=source_keys,
            dest_bucket=data["dest_bucket"],
            dest_prefix=data["dest_prefix"],
            source_prefix=data["source_prefix"],  # Preserve relative path structure
        )
        
        # Store task info
        tasks[task_id] = {
            "task_id": task_id,
            "status": "completed",
            "type": "copy",
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "config": {
                "source_bucket": data["source_bucket"],
                "source_prefix": data["source_prefix"],
                "dest_bucket": data["dest_bucket"],
                "dest_prefix": data["dest_prefix"],
            },
            "result": {
                "files_found": len(file_list),
                "copied": copy_result["copied"],
                "failed": copy_result["failed"],
            }
        }
        
        return jsonify({
            "message": f"Copy task completed",
            "task_id": task_id,
            "status": "completed",
            "files_found": len(file_list),
            "copied": copy_result["copied"],
            "failed": copy_result["failed"],
            "errors": copy_result["errors"] if copy_result["failed"] > 0 else None
        }), 200
        
    except subprocess.TimeoutExpired:
        return jsonify({"error": "File listing timed out"}), 500
    except Exception as e:
        print(f"Error copying files: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    print("Backup Manager Demo API server")
    print(f"Vector binary: {VECTOR_BINARY}")
    print(f"Config directory: {CONFIG_DIR}")
    print("Server starting on http://0.0.0.0:8080")
    app.run(host="0.0.0.0", port=8080, debug=True)
