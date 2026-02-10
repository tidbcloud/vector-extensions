# Checkpoint Mechanism for Data Synchronization Tasks

## Overview

This document describes how checkpoint mechanisms work for one-time tasks and scheduled tasks in the data synchronization system, ensuring data consistency and fault tolerance.

## Checkpoint Strategy by Task Type

### One-time Tasks

**Characteristics:**
- Execute once and exit
- Each task runs in an independent Vector instance
- Task completes when all data is processed

**Checkpoint Requirements:**
1. **File-level checkpoint**: Track which files have been processed
2. **Row-level checkpoint**: Track progress within large files (optional)
3. **Recovery**: Resume from last checkpoint if task is interrupted

**Implementation:**

#### 1. Vector's Built-in Checkpoint (via `data_dir`)

Vector automatically manages checkpoints for supported sources when `data_dir` is configured:

```toml
data_dir = "/tmp/vector-data/{task_id}"

[sources.parquet_processor]
type = "exec"
# Vector stores checkpoint state in data_dir
```

**Limitations:**
- `exec` source doesn't support Vector's built-in checkpoint mechanism
- Need custom checkpoint management for exec-based sources

#### 2. Custom Checkpoint for Exec Source

Since `exec` source doesn't support Vector's checkpoint, we need to implement custom checkpoint in the Python script:

**Checkpoint Data Structure:**
```python
{
    "task_id": "uuid",
    "last_processed_file": "s3://bucket/prefix/file.parquet",
    "last_processed_timestamp": "2025-06-06T18:00:00Z",
    "processed_files": ["file1.parquet", "file2.parquet"],
    "total_processed": 1000,
    "checkpoint_time": "2025-06-06T18:05:00Z"
}
```

**Checkpoint Location:**
- Local file: `/tmp/vector-checkpoints/{task_id}.json`
- Or in `data_dir`: `/tmp/vector-data/{task_id}/checkpoint.json`

**Checkpoint Update Strategy:**
- Update after each file is processed
- Atomic write (write to temp file, then rename)
- Load checkpoint on script startup

#### 3. Checkpoint Implementation in Python Script

```python
import json
import os
from pathlib import Path
from datetime import datetime

CHECKPOINT_DIR = Path("/tmp/vector-checkpoints")
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

def load_checkpoint(task_id: str) -> dict:
    """Load checkpoint for task"""
    checkpoint_file = CHECKPOINT_DIR / f"{task_id}.json"
    if checkpoint_file.exists():
        with open(checkpoint_file, 'r') as f:
            return json.load(f)
    return {
        "task_id": task_id,
        "processed_files": [],
        "last_processed_file": None,
        "last_processed_timestamp": None,
        "total_processed": 0,
    }

def save_checkpoint(task_id: str, checkpoint: dict):
    """Save checkpoint atomically"""
    checkpoint_file = CHECKPOINT_DIR / f"{task_id}.json"
    temp_file = CHECKPOINT_DIR / f"{task_id}.json.tmp"
    
    checkpoint["checkpoint_time"] = datetime.utcnow().isoformat() + "Z"
    
    # Write to temp file first
    with open(temp_file, 'w') as f:
        json.dump(checkpoint, f, indent=2)
        f.flush()
        os.fsync(f.fileno())  # Force write to disk
    
    # Atomic rename
    temp_file.replace(checkpoint_file)

def process_parquet_files():
    """Process Parquet files with checkpoint support"""
    task_id = os.environ.get('TASK_ID', 'default')
    checkpoint = load_checkpoint(task_id)
    processed_files = set(checkpoint.get("processed_files", []))
    
    # List and process files
    for parquet_key in parquet_files:
        # Skip already processed files
        if parquet_key in processed_files:
            continue
        
        # Process file...
        # ... (existing processing logic)
        
        # Update checkpoint after each file
        checkpoint["processed_files"].append(parquet_key)
        checkpoint["last_processed_file"] = parquet_key
        checkpoint["last_processed_timestamp"] = datetime.utcnow().isoformat() + "Z"
        checkpoint["total_processed"] += len(df)
        save_checkpoint(task_id, checkpoint)
```

### Scheduled Tasks

**Characteristics:**
- Run periodically (e.g., every hour, daily)
- Single Vector instance handles multiple tasks
- Tasks share the same Vector process

**Checkpoint Requirements:**
1. **Per-task checkpoint**: Each scheduled task has its own checkpoint
2. **Time-based checkpoint**: Track last successful execution time
3. **Incremental processing**: Only process new data since last checkpoint

**Implementation:**

#### 1. Vector's Built-in Checkpoint

For sources that support checkpoint (e.g., `aws_s3`, `file`), Vector automatically tracks progress:

```toml
data_dir = "/vector/data/checkpoints"

[sources.s3_logs]
type = "aws_s3"
bucket = "logs-bucket"
# Vector tracks which files have been read
```

#### 2. Custom Checkpoint per Task

For scheduled tasks, checkpoint should include:

```json
{
    "task_id": "scheduled-backup-001",
    "last_successful_run": "2025-06-06T18:00:00Z",
    "last_processed_time": "2025-06-06T18:00:00Z",
    "next_run_time": "2025-06-06T19:00:00Z",
    "execution_count": 100,
    "last_execution_status": "success",
    "processed_files": ["file1", "file2"],
    "total_processed": 50000
}
```

#### 3. Checkpoint Location for Scheduled Tasks

- **Shared directory**: `/vector/data/checkpoints/scheduled/`
- **Per-task file**: `{task_id}.json`
- **Vector data_dir**: Vector's own checkpoint in `data_dir`

## Checkpoint Determination

### How Checkpoints are Determined

1. **Source-level Checkpoint**:
   - Vector sources (like `aws_s3`, `file`) automatically track file positions
   - Stored in `data_dir` by Vector
   - Format: Vector's internal checkpoint format

2. **Application-level Checkpoint**:
   - Custom checkpoint for exec sources or complex scenarios
   - Stored as JSON files
   - Managed by application code

3. **Database-level Checkpoint**:
   - For sinks that write to databases, can track last inserted record
   - Query database to find last processed record
   - Use timestamps or sequence numbers

### Checkpoint Recovery

**For One-time Tasks:**

1. **On Task Start**:
   ```python
   # Load checkpoint
   checkpoint = load_checkpoint(task_id)
   
   # Skip already processed files
   processed_files = set(checkpoint.get("processed_files", []))
   
   # Resume from last position
   if checkpoint.get("last_processed_file"):
       # Start from next file after last_processed_file
       pass
   ```

2. **On Task Interruption**:
   - Checkpoint is saved periodically
   - On restart, load checkpoint and resume

3. **On Task Completion**:
   - Mark checkpoint as completed
   - Optionally archive checkpoint

**For Scheduled Tasks:**

1. **On Each Run**:
   ```python
   # Load checkpoint
   checkpoint = load_checkpoint(task_id)
   
   # Determine time range for this run
   last_run = checkpoint.get("last_successful_run")
   current_time = datetime.utcnow()
   
   # Process data from last_run to current_time
   ```

2. **After Successful Run**:
   ```python
   # Update checkpoint
   checkpoint["last_successful_run"] = current_time.isoformat() + "Z"
   checkpoint["execution_count"] += 1
   checkpoint["last_execution_status"] = "success"
   save_checkpoint(task_id, checkpoint)
   ```

3. **On Failure**:
   ```python
   # Don't update last_successful_run
   # Next run will retry from same position
   checkpoint["last_execution_status"] = "failed"
   save_checkpoint(task_id, checkpoint)
   ```

## Current Demo Implementation

### Current State

The current demo implementation:
- ✅ Uses `data_dir` for Vector's internal state
- ❌ Does NOT implement custom checkpoint for exec source
- ❌ Does NOT track processed files
- ❌ Does NOT support task recovery

### Recommended Enhancements

1. **Add Checkpoint Support to Python Script**:
   - Track processed files
   - Save checkpoint after each file
   - Load checkpoint on startup

2. **Add Checkpoint API to Management Server**:
   - `GET /api/v1/tasks/{task_id}/checkpoint` - Get checkpoint status
   - `POST /api/v1/tasks/{task_id}/reset-checkpoint` - Reset checkpoint
   - `POST /api/v1/tasks/{task_id}/resume` - Resume from checkpoint

3. **Add Checkpoint Monitoring**:
   - Display checkpoint status in task status
   - Show progress based on checkpoint
   - Alert on checkpoint staleness

## Best Practices

1. **Atomic Writes**: Always use atomic file operations for checkpoint updates
2. **Frequent Updates**: Update checkpoint frequently (after each file or every N records)
3. **Validation**: Validate checkpoint data on load
4. **Cleanup**: Archive or delete checkpoints for completed tasks
5. **Monitoring**: Monitor checkpoint age and staleness
6. **Error Handling**: Handle checkpoint corruption gracefully

## Example: Complete Checkpoint Flow

### One-time Task Flow

```
1. Task Created
   ↓
2. Load Checkpoint (if exists)
   ↓
3. List Files to Process
   ↓
4. Skip Already Processed Files (from checkpoint)
   ↓
5. Process Next File
   ↓
6. Update Checkpoint (after each file)
   ↓
7. Continue until all files processed
   ↓
8. Mark Checkpoint as Completed
   ↓
9. Task Complete
```

### Scheduled Task Flow

```
1. Scheduled Time Reached
   ↓
2. Load Checkpoint
   ↓
3. Determine Time Range (last_run to now)
   ↓
4. Process Data in Time Range
   ↓
5. Update Checkpoint (last_successful_run = now)
   ↓
6. Wait for Next Schedule
```

## Integration with Vector

### Vector's Checkpoint Support

Vector supports checkpoint for:
- ✅ `aws_s3` source (tracks file positions)
- ✅ `file` source (tracks file positions)
- ✅ `kafka` source (tracks offsets)
- ❌ `exec` source (does NOT support checkpoint)

### Workaround for Exec Source

Since `exec` source doesn't support Vector's checkpoint:
1. Implement checkpoint in the script itself
2. Use external checkpoint storage (file, database)
3. Load checkpoint before processing
4. Update checkpoint during processing

## Future Improvements

1. **Database-backed Checkpoint**: Store checkpoints in database for distributed systems
2. **Checkpoint Replication**: Replicate checkpoints for high availability
3. **Checkpoint Compression**: Compress checkpoint data for large tasks
4. **Checkpoint Encryption**: Encrypt sensitive checkpoint data
5. **Checkpoint Versioning**: Support checkpoint schema evolution
