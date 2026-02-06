# Development Attempts and Issues Log

This document records all attempts, issues encountered, and solutions during the demo development.

## 2025-01-XX: Initial Demo Implementation

### Requirement
Create a demo that uses Vector to backup slowlogs from S3 to MySQL, with the management API only generating Vector configurations and managing Vector state.

### Attempt 1: Direct S3 Source
**Approach**: Use Vector's `aws_s3` source directly to read from S3.

**Issues**:
- Vector's `aws_s3` source is designed for SQS-based streaming, not direct file listing
- Does not support Parquet file parsing
- Complex configuration required

**Result**: Abandoned - not suitable for Parquet files.

### Attempt 2: Download to Local, Then File Source
**Approach**: Python app downloads Parquet files to local directory, Vector reads using `file` source.

**Issues**:
- Vector's `file` source reads files as text/binary, cannot parse Parquet
- Requires local disk space
- Python app is doing data acquisition (should be Vector's job)

**Result**: Abandoned - violates demo principle (app should only manage Vector).

### Attempt 3: Python Preprocessing to JSONL
**Approach**: Python app downloads Parquet, converts to JSONL, Vector reads JSONL.

**Issues**:
- Still violates principle - Python app is processing data
- User feedback: "demo的目的只是生成vector的配置和对vector状态进行管理"

**Result**: Abandoned - user explicitly stated app should not process data.

### Attempt 4: Vector Exec Source with Python Script
**Approach**: Use Vector's `exec` source to execute a Python script that processes Parquet files.

**Implementation**:
- Created `demo/extension/sources/parquet_s3_processor.py`
- Script reads from S3, processes Parquet, outputs JSON Lines to stdout
- Vector `exec` source executes the script and reads stdout
- Management API only generates Vector config and manages Vector state

**Benefits**:
- ✅ Data processing is done by Vector (via exec source)
- ✅ Management API only generates config and manages state
- ✅ Clear separation of concerns
- ✅ Easy to convert to Rust plugin later

**Current Status**: ✅ Working

**Future Improvement**:
- Convert Python script to Rust-based Vector source plugin
- Plugin will handle S3 authentication, file listing, Parquet parsing natively
- Better performance, type safety, no subprocess overhead

## 2025-01-XX: MySQL Sink Implementation

### Requirement
Use Vector's exec sink to write data directly to MySQL, instead of using file sink + Python monitoring thread.

### Implementation
- Created `demo/extension/sinks/mysql_writer.py`
- Script reads JSON Lines from stdin (sent by Vector exec sink)
- Writes to MySQL in batches
- Updated `generate_vector_config` to use exec sink instead of file sink
- Removed `import_to_mysql` thread (no longer needed)

**Benefits**:
- ✅ Consistent architecture: source and sink both use exec scripts
- ✅ Simpler code: no file monitoring, no separate threads
- ✅ Direct data flow: Vector → exec sink → MySQL
- ✅ Better error handling: Vector manages the sink process

**Current Status**: ✅ Working (with file sink + monitoring thread)

**Issue Encountered**:
- Vector doesn't have `exec` sink (only has `exec` source)
- Error: `unknown variant exec, expected one of amqp, appsignal, ...`

**Solution**:
- Use `file` sink to output JSON Lines to files
- Use background thread to monitor files and import to MySQL
- The `mysql_writer.py` script exists but is not used directly by Vector
- In production, would need a custom Vector sink plugin

**Future Improvement**:
- Create a custom Rust-based Vector sink plugin for MySQL
- Plugin will handle MySQL connections, connection pooling, batching natively
- Better performance, type safety, no subprocess overhead, no file monitoring needed

## 2025-01-XX: One-time Task Completion Detection

### Requirement
One-time tasks should stop Vector process automatically when data processing completes.

### Issue Encountered
- Vector processes were still running after tasks completed
- `exec` source in `streaming` mode keeps running even after script exits
- Multiple Vector processes accumulating in system

### Solution
- Changed `exec` source `mode` from `streaming` to `oneshot`
  - `oneshot` mode: Script runs once, exits, Vector processes remaining events and exits
  - `streaming` mode: Script keeps running, Vector waits for continuous output
- Added `monitor_vector_task` function to detect task completion
  - Monitors Vector process status
  - Detects when process exits (normal completion)
  - Updates task status to "completed" or "failed"
  - Handles cleanup

**Current Status**: ✅ Working

**Benefits**:
- ✅ Vector processes exit automatically when tasks complete
- ✅ No process accumulation
- ✅ Proper task status tracking
- ✅ Resource cleanup

## 2025-01-XX: Code Organization

### Requirement
Organize Python extension code into `demo/extension` directory structure.

### Implementation
- Created `demo/extension/sources/` for source scripts
- Created `demo/extension/transforms/` for transform scripts (future)
- Created `demo/extension/sinks/` for sink scripts (future)
- Moved Parquet processor to `demo/extension/sources/parquet_s3_processor.py`
- Updated `app.py` to reference scripts from extension directory

**Benefits**:
- Clear separation between management API and data processing logic
- Easy to identify what will become Vector plugins
- Better code organization

## Known Issues

### Issue 1: Parquet Processing Performance
**Description**: Python script processes Parquet files sequentially, which may be slow for large datasets.

**Solution**: Future Rust plugin will use parallel processing and native Parquet parsing.

### Issue 2: Environment Variable Passing
**Description**: Currently passing configuration via environment variables to the Python script.

**Solution**: Future Rust plugin will use Vector's configuration system directly.

### Issue 3: Error Handling
**Description**: Python script errors are written to stderr, but Vector may not surface them clearly.

**Solution**: Future Rust plugin will use Vector's error handling and logging system.

## Lessons Learned

1. **Vector exec source is powerful**: Can execute any script/command, making it easy to prototype
2. **Separation of concerns**: Management API should only manage Vector, not process data
3. **Clear migration path**: Python scripts → Rust plugins is a good development approach
4. **Documentation is critical**: Recording attempts prevents repeating mistakes
