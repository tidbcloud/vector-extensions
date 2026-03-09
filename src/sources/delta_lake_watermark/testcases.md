# Delta Lake Watermark Source Test Cases

This document outlines test cases for the `delta_lake_watermark` source, organized for task tracking and implementation.

## Test Strategy

- **Local Delta Lake**: Most tests use local file system Delta Lake tables to avoid cloud storage credentials
- **Mock DuckDB**: Use DuckDB with local Parquet files or mock data
- **Cloud Storage**: Only test cloud-specific features (AWS, GCP, Azure, Aliyun) when necessary

## Test Categories

### 1. Unit Tests

#### 1.1 Configuration Tests

- [x] **TC-001**: Test configuration validation ✅
  - Valid configuration should pass
  - Invalid `cloud_provider` should fail
  - Invalid `endpoint` format should fail
  - Invalid time format should fail
  - Zero `batch_size` should fail
  - **Location**: `src/sources/delta_lake_watermark/mod.rs`
  - **Type**: Unit test
  - **Dependencies**: None
  - **Status**: Implemented in `mod.rs::tests`

- [x] **TC-002**: Test default values ✅
  - Verify default `cloud_provider` is "aws"
  - Verify default `order_by_column` is "time"
  - Verify default `batch_size` is 10000
  - Verify default `poll_interval_secs` is 30
  - Verify default `acknowledgements` is true
  - **Location**: `src/sources/delta_lake_watermark/mod.rs`
  - **Type**: Unit test
  - **Dependencies**: None
  - **Status**: Implemented in `mod.rs::tests`

- [x] **TC-003**: Test GenerateConfig ✅
  - Verify `generate_config()` produces valid TOML
  - Verify all required fields are present
  - **Location**: `src/sources/delta_lake_watermark/mod.rs`
  - **Type**: Unit test
  - **Dependencies**: None
  - **Status**: Implemented in `mod.rs::tests`

#### 1.2 Checkpoint Tests

- [x] **TC-004**: Test checkpoint creation ✅
  - Create checkpoint with default values
  - Verify initial state is "running"
  - Verify `last_watermark` is None
  - **Location**: `src/sources/delta_lake_watermark/checkpoint.rs`
  - **Type**: Unit test
  - **Dependencies**: None
  - **Status**: Implemented in `checkpoint.rs::tests::test_checkpoint_default`

- [x] **TC-005**: Test checkpoint save/load ✅
  - Save checkpoint to file
  - Load checkpoint from file
  - Verify all fields are preserved
  - **Location**: `src/sources/delta_lake_watermark/checkpoint.rs`
  - **Type**: Unit test
  - **Dependencies**: `tempfile` crate
  - **Status**: Implemented in `checkpoint.rs::tests::test_checkpoint_save_load`

- [x] **TC-006**: Test checkpoint update ✅
  - Update watermark with timestamp
  - Update watermark with timestamp and unique_id
  - Verify checkpoint reflects updates
  - **Location**: `src/sources/delta_lake_watermark/checkpoint.rs`
  - **Type**: Unit test
  - **Dependencies**: None
  - **Status**: Implemented in `checkpoint.rs::tests::test_checkpoint_update_watermark`

- [x] **TC-007**: Test checkpoint status transitions ✅
  - Mark as finished
  - Mark as error
  - Verify status changes
  - **Location**: `src/sources/delta_lake_watermark/checkpoint.rs`
  - **Type**: Unit test
  - **Dependencies**: None
  - **Status**: Implemented in `checkpoint.rs::tests::test_checkpoint_status_transitions`

- [x] **TC-008**: Test checkpoint path generation ✅
  - Generate path for S3 endpoint
  - Generate path for GCS endpoint
  - Verify path is safe (no special characters)
  - **Location**: `src/sources/delta_lake_watermark/checkpoint.rs`
  - **Type**: Unit test
  - **Dependencies**: None
  - **Status**: Implemented in `checkpoint.rs::tests::test_checkpoint_path_generation`

- [x] **TC-009**: Test checkpoint load from non-existent file ✅
  - Load checkpoint when file doesn't exist
  - Should return default checkpoint
  - **Location**: `src/sources/delta_lake_watermark/checkpoint.rs`
  - **Type**: Unit test
  - **Dependencies**: None
  - **Status**: Implemented in `checkpoint.rs::tests::test_checkpoint_load_nonexistent`

- [x] **TC-010**: Test checkpoint load from corrupted file ✅
  - Load checkpoint from invalid JSON
  - Should return default checkpoint (graceful degradation)
  - **Location**: `src/sources/delta_lake_watermark/checkpoint.rs`
  - **Type**: Unit test
  - **Dependencies**: None
  - **Status**: Implemented in `checkpoint.rs::tests::test_checkpoint_load_corrupted`

- [x] **TC-011**: Test last_watermark_datetime conversion ✅
  - Convert valid RFC3339 timestamp
  - Handle None watermark
  - Handle invalid timestamp format
  - **Location**: `src/sources/delta_lake_watermark/checkpoint.rs`
  - **Type**: Unit test
  - **Dependencies**: None
  - **Status**: Implemented in `checkpoint.rs::tests::test_last_watermark_datetime`

#### 1.3 DuckDB Query Tests

- [x] **TC-012**: Test DuckDB executor initialization ✅
  - Create executor with valid endpoint
  - Verify connection is established
  - Verify memory limit is set (if provided)
  - **Location**: `src/sources/delta_lake_watermark/duckdb_query.rs`
  - **Type**: Unit test
  - **Dependencies**: `duckdb` crate
  - **Status**: Implemented in `duckdb_query.rs::tests::test_duckdb_executor_initialization` and `test_duckdb_executor_with_memory_limit`

- [x] **TC-013**: Test query building - basic ✅
  - Build query with condition containing time range
  - Verify WHERE clause includes time range from condition
  - Verify ORDER BY clause
  - Verify LIMIT clause
  - **Location**: `src/sources/delta_lake_watermark/duckdb_query.rs`
  - **Type**: Unit test
  - **Dependencies**: None
  - **Status**: Implemented in `duckdb_query.rs::tests::test_query_building_basic`

- [x] **TC-014**: Test query building - with checkpoint ✅
  - Build query with existing checkpoint
  - Verify WHERE clause uses last_watermark
  - Verify unique_id handling when present
  - **Location**: `src/sources/delta_lake_watermark/duckdb_query.rs`
  - **Type**: Unit test
  - **Dependencies**: None
  - **Status**: Implemented in `duckdb_query.rs::tests::test_query_building_with_checkpoint`

- [x] **TC-015**: Test query building - with condition ✅
  - Build query with additional WHERE condition
  - Verify condition is properly escaped
  - Verify condition is combined with time range
  - **Location**: `src/sources/delta_lake_watermark/duckdb_query.rs`
  - **Type**: Unit test
  - **Dependencies**: None
  - **Status**: Implemented in `duckdb_query.rs::tests::test_query_building_with_condition`

- [x] **TC-016**: Test query building - same timestamp handling ✅
  - Build query with unique_id_column
  - Verify OR condition for same timestamp
  - Verify unique_id comparison
  - **Location**: `src/sources/delta_lake_watermark/duckdb_query.rs`
  - **Type**: Unit test
  - **Dependencies**: None
  - **Status**: Implemented in `duckdb_query.rs::tests::test_query_building_same_timestamp_handling`

- [x] **TC-016a**: Test query building - without unique_id_column ✅
  - Build query without unique_id_column but with checkpoint
  - Verify uses >= (not >) to include same timestamp records for data completeness
  - Verify no OR condition is used
  - **Location**: `src/sources/delta_lake_watermark/duckdb_query.rs`
  - **Type**: Unit test
  - **Dependencies**: None
  - **Status**: Implemented in `duckdb_query.rs::tests::test_query_building_without_unique_id`
  - **Note**: Without unique_id_column, the source uses >= to ensure data completeness when multiple records share the same timestamp. This may cause duplicate processing of same-timestamp records after restart, but ensures no data is missed. Users should either ensure order_by_column is unique or provide unique_id_column for precise incremental sync.

- [x] **TC-017**: Test cloud storage configuration - AWS ✅
  - Configure for AWS S3
  - Verify no special configuration needed
  - **Location**: `src/sources/delta_lake_watermark/duckdb_query.rs`
  - **Type**: Unit test
  - **Dependencies**: None
  - **Status**: Implemented in `duckdb_query.rs::tests::test_cloud_storage_config_aws`

- [x] **TC-018**: Test cloud storage configuration - Aliyun ✅
  - Configure for Aliyun OSS
  - Verify OSS_ENDPOINT is set
  - Verify s3_use_path_style is false
  - **Location**: `src/sources/delta_lake_watermark/duckdb_query.rs`
  - **Type**: Unit test
  - **Dependencies**: Environment variables
  - **Status**: Implemented in `duckdb_query.rs::tests::test_cloud_storage_config_aliyun`

- [x] **TC-019**: Test value extraction from DuckDB row ✅
  - Extract String values
  - Extract integer values (i64)
  - Extract float values (f64)
  - Extract boolean values
  - Extract NULL values
  - **Location**: `src/sources/delta_lake_watermark/duckdb_query.rs`
  - **Type**: Unit test
  - **Dependencies**: `duckdb` crate
  - **Status**: Implemented in `duckdb_query.rs::tests::test_extract_value_as_string_concept` (conceptual test, actual extraction tested through execute_query integration)

- [x] **TC-020**: Test RecordBatch to events conversion ✅
  - Convert RecordBatch with multiple rows
  - Verify all columns are included
  - Verify NULL values are handled
  - Verify data types are preserved as strings
  - **Location**: `src/sources/delta_lake_watermark/duckdb_query.rs`
  - **Type**: Unit test
  - **Dependencies**: `arrow` crate
  - **Status**: Implemented in `duckdb_query.rs::tests::test_record_batch_to_events` and `test_record_batch_to_events_with_null`

- [x] **TC-021**: Test empty query result ✅
  - Execute query that returns no rows
  - Verify empty RecordBatch is returned
  - Verify schema is preserved
  - **Location**: `src/sources/delta_lake_watermark/duckdb_query.rs`
  - **Type**: Unit test
  - **Dependencies**: `duckdb` crate
  - **Status**: Implemented in `duckdb_query.rs::tests::test_empty_query_result` (conceptual test, actual empty result handling tested through execute_query integration)

#### 1.4 Controller Tests

- [x] **TC-022**: Test controller initialization ✅
  - Create controller with valid config
  - Verify checkpoint is loaded
  - Verify executor is created
  - **Location**: `src/sources/delta_lake_watermark/controller.rs`
  - **Type**: Unit test
  - **Dependencies**: Mock SourceSender, local Delta Lake
  - **Status**: Implemented in `controller.rs::tests::test_controller_structure` and `test_controller_fields`

- [x] **TC-023**: Test JSON value to LogValue conversion ✅
  - Convert JSON Null to LogValue::Null
  - Convert JSON Boolean to LogValue::Boolean
  - Convert JSON Number (integer) to LogValue::Integer
  - Convert JSON Number (float) to LogValue::Float
  - Convert JSON String to LogValue::Bytes
  - Convert JSON Array to LogValue::Array
  - Convert JSON Object to LogValue::Object
  - **Location**: `src/sources/delta_lake_watermark/controller.rs`
  - **Type**: Unit test
  - **Dependencies**: None
  - **Status**: Implemented in `controller.rs::tests::test_json_value_to_log_value`

### 2. Integration Tests

#### 2.1 Local Delta Lake Tests

- [ ] **TC-024**: Test end-to-end sync - one-off task
  - Create local Delta Lake table with test data
  - Configure source with condition containing time range (e.g., `condition = "time >= 1717632000 AND time <= 1718044799"`)
  - Run source and verify all data is synced
  - Verify checkpoint is updated correctly
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Local Delta Lake table, `deltalake` crate

- [ ] **TC-025**: Test incremental sync with checkpoint
  - Create local Delta Lake table
  - Run first sync, create checkpoint
  - Add more data to table
  - Run second sync, verify only new data is synced
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Local Delta Lake table

- [ ] **TC-026**: Test batch processing
  - Create table with data larger than batch_size
  - Verify data is processed in batches
  - Verify checkpoint is updated after each batch
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Local Delta Lake table

- [ ] **TC-027**: Test filtering with condition
  - Create table with mixed data
  - Apply WHERE condition filter
  - Verify only matching rows are synced
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Local Delta Lake table

- [ ] **TC-028**: Test same timestamp handling
  - Create table with multiple rows having same timestamp
  - Configure unique_id_column
  - Verify all rows are processed in correct order
  - Verify no rows are skipped
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Local Delta Lake table

- [ ] **TC-029**: Test streaming mode
  - Configure source with condition containing only start time (no end time, e.g., `condition = "time >= 1717632000"`)
  - Run source, process initial data
  - Add new data to table
  - Verify source polls and processes new data
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Local Delta Lake table

- [ ] **TC-030**: Test fault recovery
  - Run sync, create checkpoint
  - Simulate crash (kill process)
  - Restart source
  - Verify sync resumes from checkpoint
  - Verify no data is duplicated
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Local Delta Lake table

- [ ] **TC-031**: Test schema evolution
  - Create table with initial schema
  - Sync some data
  - Add new columns to table
  - Sync more data
  - Verify new columns are included
  - Verify old data still works
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Local Delta Lake table

- [ ] **TC-032**: Test empty table handling
  - Create empty Delta Lake table
  - Run source
  - Verify source handles gracefully
  - Verify checkpoint is not updated
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Local Delta Lake table

- [ ] **TC-033**: Test time range limit reached
  - Create table with data
  - Set condition with time range ending in middle of data (e.g., `condition = "time >= 1717632000 AND time <= 1717700000"`)
  - Run source
  - Verify sync stops at the end time specified in condition
  - Verify checkpoint reflects the last processed record
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Local Delta Lake table

#### 2.2 Acknowledgment Tests

- [ ] **TC-034**: Test with acknowledgements enabled
  - Configure source with acknowledgements = true
  - Send events to sink
  - Verify checkpoint only updates after ack
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Mock sink with ack support

- [ ] **TC-035**: Test with acknowledgements disabled
  - Configure source with acknowledgements = false
  - Send events
  - Verify checkpoint updates immediately
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Mock sink

- [ ] **TC-036**: Test ack failure handling
  - Configure with acknowledgements
  - Simulate ack failure
  - Verify checkpoint is not updated
  - Verify retry behavior
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Mock sink with ack failure

#### 2.3 Metrics Tests

- [ ] **TC-037**: Test metrics initialization
  - Start source
  - Verify metrics are registered
  - Verify initial values are correct
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Metrics registry

- [ ] **TC-038**: Test watermark timestamp metric
  - Process data
  - Verify `delta_sync_watermark_timestamp` is updated
  - Verify value matches checkpoint
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Metrics registry, local Delta Lake

- [ ] **TC-039**: Test rows processed metric
  - Process multiple batches
  - Verify `delta_sync_rows_processed_total` increments
  - Verify count matches actual rows
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Metrics registry, local Delta Lake

- [ ] **TC-040**: Test finished status metric
  - Complete one-off task
  - Verify `delta_sync_is_finished` is set to 1.0
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Metrics registry, local Delta Lake

### 3. Cloud Storage Tests (Optional)

#### 3.1 AWS S3 Tests

- [ ] **TC-041**: Test AWS S3 endpoint
  - Configure with s3:// endpoint
  - Verify connection to S3
  - Verify data can be queried
  - **Location**: `tests/delta_lake_watermark_cloud.rs`
  - **Type**: Integration test (requires AWS credentials)
  - **Dependencies**: AWS S3 bucket with Delta Lake table, AWS credentials
  - **Note**: Can be skipped in CI, manual test only

#### 3.2 GCP Cloud Storage Tests

- [ ] **TC-042**: Test GCP Cloud Storage endpoint
  - Configure with gs:// endpoint
  - Verify connection to GCS
  - Verify data can be queried
  - **Location**: `tests/delta_lake_watermark_cloud.rs`
  - **Type**: Integration test (requires GCP credentials)
  - **Dependencies**: GCS bucket with Delta Lake table, GCP credentials
  - **Note**: Can be skipped in CI, manual test only

#### 3.3 Azure Blob Storage Tests

- [ ] **TC-043**: Test Azure Blob Storage endpoint
  - Configure with az:// endpoint
  - Verify connection to Azure
  - Verify data can be queried
  - **Location**: `tests/delta_lake_watermark_cloud.rs`
  - **Type**: Integration test (requires Azure credentials)
  - **Dependencies**: Azure container with Delta Lake table, Azure credentials
  - **Note**: Can be skipped in CI, manual test only

#### 3.4 Aliyun OSS Tests

- [ ] **TC-044**: Test Aliyun OSS endpoint
  - Configure with oss:// endpoint
  - Set OSS_ENDPOINT environment variable
  - Verify connection to OSS
  - Verify data can be queried
  - **Location**: `tests/delta_lake_watermark_cloud.rs`
  - **Type**: Integration test (requires Aliyun credentials)
  - **Dependencies**: OSS bucket with Delta Lake table, OSS credentials
  - **Note**: Can be skipped in CI, manual test only

### 4. Error Handling Tests

- [ ] **TC-045**: Test invalid Delta Lake table
  - Point to non-existent table
  - Verify graceful error handling
  - Verify error message is clear
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: None

- [ ] **TC-046**: Test DuckDB connection failure
  - Simulate DuckDB connection error
  - Verify error is handled gracefully
  - Verify source can recover
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Mock DuckDB failure

- [ ] **TC-047**: Test query execution failure
  - Execute invalid SQL query
  - Verify error is caught and logged
  - Verify source continues running
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Local Delta Lake table

- [ ] **TC-048**: Test checkpoint file write failure
  - Simulate disk full or permission error
  - Verify error is handled
  - Verify source continues (with warning)
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Mock filesystem error

- [ ] **TC-049**: Test memory limit exceeded
  - Configure small duckdb_memory_limit
  - Query large dataset
  - Verify OOM is prevented or handled
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Local Delta Lake table with large data

- [ ] **TC-050**: Test network timeout (for cloud storage)
  - Simulate network timeout
  - Verify retry logic
  - Verify error is logged
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Mock network failure

### 5. Performance Tests

- [ ] **TC-051**: Test large batch processing
  - Process table with 100K+ rows
  - Verify memory usage is controlled
  - Verify processing completes successfully
  - **Location**: `tests/delta_lake_watermark_performance.rs`
  - **Type**: Performance test
  - **Dependencies**: Local Delta Lake table with large dataset

- [ ] **TC-052**: Test query performance with predicate pushdown
  - Create partitioned Delta Lake table
  - Query with time range filter
  - Verify only relevant partitions are scanned
  - **Location**: `tests/delta_lake_watermark_performance.rs`
  - **Type**: Performance test
  - **Dependencies**: Local partitioned Delta Lake table

- [ ] **TC-053**: Test concurrent queries
  - Run multiple sources against same table
  - Verify no conflicts
  - Verify each maintains own checkpoint
  - **Location**: `tests/delta_lake_watermark_performance.rs`
  - **Type**: Performance test
  - **Dependencies**: Local Delta Lake table

### 6. Edge Cases

- [ ] **TC-054**: Test very large timestamps
  - Use timestamps far in future
  - Verify comparison works correctly
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Local Delta Lake table

- [ ] **TC-055**: Test very old timestamps
  - Use timestamps far in past
  - Verify comparison works correctly
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Local Delta Lake table

- [ ] **TC-056**: Test timezone handling
  - Use timestamps with different timezones
  - Verify UTC conversion
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Local Delta Lake table

- [ ] **TC-057**: Test special characters in data
  - Include special characters in table data
  - Verify JSON encoding is correct
  - Verify no data corruption
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Local Delta Lake table

- [ ] **TC-058**: Test NULL values in order_by_column
  - Create table with NULL timestamps
  - Verify NULL handling in WHERE clause
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Local Delta Lake table

- [ ] **TC-059**: Test missing unique_id_column values
  - Create table where some rows lack unique_id
  - Verify graceful handling
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Local Delta Lake table

- [ ] **TC-060**: Test checkpoint with invalid timestamp format
  - Manually create checkpoint with invalid timestamp
  - Verify source handles gracefully
  - Verify user should specify time range in condition when no valid checkpoint exists
  - **Location**: `tests/delta_lake_watermark_integration.rs`
  - **Type**: Integration test
  - **Dependencies**: Local checkpoint file manipulation

## Test Implementation Priority

### Phase 1: Core Functionality (Must Have)
- TC-001 to TC-003: Configuration tests
- TC-004 to TC-011: Checkpoint tests
- TC-012 to TC-021: DuckDB query tests
- TC-022 to TC-023: Controller basic tests
- TC-024 to TC-033: Local Delta Lake integration tests

### Phase 2: Advanced Features (Should Have)
- TC-034 to TC-036: Acknowledgment tests
- TC-037 to TC-040: Metrics tests
- TC-045 to TC-050: Error handling tests

### Phase 3: Edge Cases and Performance (Nice to Have)
- TC-051 to TC-053: Performance tests
- TC-054 to TC-060: Edge case tests

### Phase 4: Cloud Storage (Optional)
- TC-041 to TC-044: Cloud storage tests (manual testing only)

## Test Data Setup

### Local Delta Lake Table Structure

For most tests, use a local Delta Lake table with the following schema:

```python
# Python script to create test Delta Lake table
import pandas as pd
from deltalake import DeltaTable

# Schema
schema = {
    "time": "timestamp",
    "unique_id": "string",
    "type": "string",
    "severity": "integer",
    "message": "string",
    "data": "string"
}

# Sample data
data = [
    {"time": "2026-01-01T00:00:00Z", "unique_id": "id-001", "type": "error", "severity": 5, "message": "Error 1", "data": "data1"},
    {"time": "2026-01-01T01:00:00Z", "unique_id": "id-002", "type": "info", "severity": 1, "message": "Info 1", "data": "data2"},
    # ... more test data
]

df = pd.DataFrame(data)
df["time"] = pd.to_datetime(df["time"])

# Write to Delta Lake
df.to_delta("file:///tmp/test_delta_table")
```

## Test Utilities Needed

- [ ] **Test Helper**: Create local Delta Lake table with test data
- [ ] **Test Helper**: Mock DuckDB connection for unit tests
- [ ] **Test Helper**: Mock SourceSender for controller tests
- [ ] **Test Helper**: Verify checkpoint file contents
- [ ] **Test Helper**: Verify metrics values
- [ ] **Test Helper**: Clean up test artifacts

## Notes

- All tests should be deterministic and isolated
- Use temporary directories for test data
- Clean up after each test
- Mock external dependencies when possible
- Use real Delta Lake tables only for integration tests
- Cloud storage tests require manual setup and credentials
