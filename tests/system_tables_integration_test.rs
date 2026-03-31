// Integration test for system_tables source with deltalake sink
// Tests the complete data pipeline: mock data -> system_tables events -> deltalake -> verify files

#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]

use std::collections::BTreeMap;
use std::fs;
use vector_lib::event::{Event, LogEvent, ObjectMap};

// Helper: build a sqlstatement event with a given schema
fn make_sqlstatement_event(
    index: i64,
    table_name: &str,
    instance: &str,
    extra_fields: &[(&str, &str)], // (field_name, mysql_type)
    extra_values: &[(&str, vector_lib::event::Value)], // (field_name, value)
) -> Event {
    let mut log = LogEvent::from(BTreeMap::new());

    // Vector system fields
    log.insert("_vector_table", table_name);
    log.insert("_vector_source_table", "CLUSTER_STATEMENTS_SUMMARY");
    log.insert("_vector_source_schema", "information_schema");
    log.insert("_vector_instance", instance);
    log.insert("_vector_timestamp", "2024-06-01T00:00:00Z");

    // Base data fields
    log.insert("DIGEST", format!("digest_{}", index));
    log.insert("EXEC_COUNT", index * 100);
    log.insert("AVG_LATENCY", index as f64 * 1.5);
    log.insert("INSTANCE", instance);

    // Extra data values (e.g., new column added after schema evolution)
    for (field, value) in extra_values {
        log.insert(*field, value.clone());
    }

    // Build _schema_metadata
    let mut schema_meta = ObjectMap::new();
    schema_meta.insert(
        "_partition_by".into(),
        vector_lib::event::Value::from("date"),
    );

    // Base fields
    for (field, mysql_type) in &[
        ("DIGEST", "varchar(64)"),
        ("EXEC_COUNT", "bigint"),
        ("AVG_LATENCY", "double"),
        ("INSTANCE", "varchar(64)"),
    ] {
        let mut m = ObjectMap::new();
        m.insert("mysql_type".into(), vector_lib::event::Value::from(*mysql_type));
        schema_meta.insert((*field).into(), vector_lib::event::Value::Object(m));
    }

    // Extra schema fields
    for (field, mysql_type) in extra_fields {
        let mut m = ObjectMap::new();
        m.insert("mysql_type".into(), vector_lib::event::Value::from(*mysql_type));
        schema_meta.insert((*field).into(), vector_lib::event::Value::Object(m));
    }

    log.insert(
        "_schema_metadata",
        vector_lib::event::Value::Object(schema_meta),
    );

    Event::Log(log)
}

// Import DeltaLake writer components
use vector_extensions::sinks::deltalake::{DeltaLakeWriter, DeltaTableConfig, WriteConfig};

/// Generate mock system_tables events similar to what the source would produce
fn generate_system_tables_mock_events() -> Vec<Event> {
    let mut events = Vec::new();

    // Generate 3 mock events simulating CLUSTER_STATEMENTS_SUMMARY data
    for i in 0..3 {
        let mut log = LogEvent::from(BTreeMap::new());

        // Vector metadata fields (as system_tables source would add)
        log.insert("_vector_id", (i + 1) as i64);
        log.insert("_vector_table", "hist_cluster_statements_summary");
        log.insert("_vector_source_table", "CLUSTER_STATEMENTS_SUMMARY");
        log.insert("_vector_source_schema", "metrics_schema");
        log.insert("_vector_instance", format!("tidb-{}", i));
        log.insert("_vector_timestamp", chrono::Utc::now().to_rfc3339());
        log.insert("_vector_collection_method", "test");
        log.insert("_vector_collection_duration_ms", 100_i64);
        log.insert("_vector_row_count", 1_i64);

        // Actual data fields from CLUSTER_STATEMENTS_SUMMARY
        log.insert("DIGEST", format!("digest_{}", i));
        log.insert("DIGEST_TEXT", format!("SELECT * FROM table_{}", i));
        log.insert("EXEC_COUNT", ((i + 1) * 100) as i64);
        log.insert("SUM_LATENCY", ((i + 1) * 1000) as i64);
        log.insert("AVG_LATENCY", 10.5);
        log.insert("INSTANCE", format!("tidb-{}", i));
        log.insert("SCHEMA_NAME", "test_schema");

        // Add schema metadata for DeltaLake
        let mut schema_meta = ObjectMap::new();

        // Define partition strategy
        schema_meta.insert(
            "_partition_by".into(),
            vector_lib::event::Value::from("INSTANCE"),
        );

        // Define field types
        let mut digest_meta = ObjectMap::new();
        digest_meta.insert(
            "mysql_type".into(),
            vector_lib::event::Value::from("varchar(64)"),
        );
        schema_meta.insert(
            "DIGEST".into(),
            vector_lib::event::Value::Object(digest_meta),
        );

        let mut digest_text_meta = ObjectMap::new();
        digest_text_meta.insert("mysql_type".into(), vector_lib::event::Value::from("text"));
        schema_meta.insert(
            "DIGEST_TEXT".into(),
            vector_lib::event::Value::Object(digest_text_meta),
        );

        let mut exec_count_meta = ObjectMap::new();
        exec_count_meta.insert(
            "mysql_type".into(),
            vector_lib::event::Value::from("bigint"),
        );
        schema_meta.insert(
            "EXEC_COUNT".into(),
            vector_lib::event::Value::Object(exec_count_meta),
        );

        let mut sum_latency_meta = ObjectMap::new();
        sum_latency_meta.insert(
            "mysql_type".into(),
            vector_lib::event::Value::from("bigint"),
        );
        schema_meta.insert(
            "SUM_LATENCY".into(),
            vector_lib::event::Value::Object(sum_latency_meta),
        );

        let mut avg_latency_meta = ObjectMap::new();
        avg_latency_meta.insert(
            "mysql_type".into(),
            vector_lib::event::Value::from("double"),
        );
        schema_meta.insert(
            "AVG_LATENCY".into(),
            vector_lib::event::Value::Object(avg_latency_meta),
        );

        let mut instance_meta = ObjectMap::new();
        instance_meta.insert(
            "mysql_type".into(),
            vector_lib::event::Value::from("varchar(64)"),
        );
        schema_meta.insert(
            "INSTANCE".into(),
            vector_lib::event::Value::Object(instance_meta),
        );

        let mut schema_name_meta = ObjectMap::new();
        schema_name_meta.insert(
            "mysql_type".into(),
            vector_lib::event::Value::from("varchar(64)"),
        );
        schema_meta.insert(
            "SCHEMA_NAME".into(),
            vector_lib::event::Value::Object(schema_name_meta),
        );

        log.insert(
            "_schema_metadata",
            vector_lib::event::Value::Object(schema_meta),
        );

        events.push(Event::Log(log));
    }

    events
}

#[tokio::test]
async fn test_system_tables_to_deltalake_integration() {
    // Create a temporary directory for testing
    let temp_dir = std::env::temp_dir();
    let test_id = format!(
        "system_tables_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let base_path = temp_dir.join(test_id);

    // Ensure the base directory exists
    fs::create_dir_all(&base_path).expect("Failed to create base directory");

    println!("Test directory: {:?}", base_path);

    // Generate mock system_tables events
    let events = generate_system_tables_mock_events();
    assert_eq!(events.len(), 3, "Should generate 3 mock events");

    // Verify event structure before writing
    for event in &events {
        if let Event::Log(log) = event {
            assert!(
                log.get("_vector_table").is_some(),
                "Event should have _vector_table field"
            );
            assert!(
                log.get("DIGEST").is_some(),
                "Event should have DIGEST field"
            );
            assert!(
                log.get("_schema_metadata").is_some(),
                "Event should have _schema_metadata field"
            );
        }
    }

    // Create DeltaLakeWriter configuration
    let table_config = DeltaTableConfig {
        name: "hist_cluster_statements_summary".to_string(),
        schema_evolution: Some(true),
    };

    let write_config = WriteConfig {
        batch_size: 1000,
        timeout_secs: 30,
    };

    // Create DeltaLakeWriter
    let mut writer = DeltaLakeWriter::new(base_path.clone(), table_config, write_config, None);

    // Write events to DeltaLake
    let write_result = writer.write_events(events).await;
    if let Err(e) = &write_result {
        eprintln!("Write failed: {}", e);
        if base_path.exists() {
            eprintln!("Base path contents:");
            if let Ok(entries) = fs::read_dir(&base_path) {
                for entry in entries {
                    if let Ok(entry) = entry {
                        eprintln!("  - {:?}", entry.path());
                    }
                }
            }
        }
    }
    write_result.expect("Failed to write events to DeltaLake");

    // Verify Delta Lake directory structure
    println!("Verifying Delta Lake directory structure...");

    // 1. Check table directory exists
    assert!(
        base_path.exists(),
        "Table directory should exist: {:?}",
        base_path
    );

    // 2. Check _delta_log directory exists
    let delta_log_path = base_path.join("_delta_log");
    assert!(
        delta_log_path.exists(),
        "Delta log directory should exist: {:?}",
        delta_log_path
    );

    // 3. Check for .json transaction log files
    let json_files: Vec<_> = fs::read_dir(&delta_log_path)
        .expect("Failed to read delta log directory")
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension()? == "json" {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    assert!(
        !json_files.is_empty(),
        "Should have at least one .json transaction log file"
    );

    println!("Found {} transaction log files", json_files.len());

    // 4. Check for .parquet data files
    let mut parquet_files = Vec::new();

    // Collect parquet files (including those in partition directories)
    let entries: Vec<_> = fs::read_dir(&base_path)
        .expect("Failed to read table directory")
        .collect();

    for entry in entries {
        let entry = entry.expect("Failed to read entry");
        let path = entry.path();

        if path.is_dir() && !path.to_string_lossy().contains("_delta_log") {
            // Check partition directory
            if let Ok(sub_entries) = fs::read_dir(&path) {
                for sub_entry in sub_entries {
                    if let Ok(sub_entry) = sub_entry {
                        let sub_path = sub_entry.path();
                        if sub_path
                            .extension()
                            .map(|ext| ext == "parquet")
                            .unwrap_or(false)
                        {
                            parquet_files.push(sub_path);
                        }
                    }
                }
            }
        } else if path
            .extension()
            .map(|ext| ext == "parquet")
            .unwrap_or(false)
        {
            parquet_files.push(path);
        }
    }

    assert!(
        !parquet_files.is_empty(),
        "Should have at least one .parquet data file"
    );

    println!("Found {} parquet data files", parquet_files.len());

    // 5. Verify transaction log content
    for json_file in &json_files {
        let content = fs::read_to_string(json_file).expect("Failed to read JSON file");
        assert!(
            !content.is_empty(),
            "JSON file should not be empty: {:?}",
            json_file
        );
        println!(
            "Transaction log content (first 300 chars): {}",
            &content[..content.len().min(300)]
        );
    }

    // 6. Verify parquet files are not empty
    for parquet_file in &parquet_files {
        let metadata = fs::metadata(parquet_file).expect("Failed to get file metadata");
        assert!(
            metadata.len() > 0,
            "Parquet file should not be empty: {:?}",
            parquet_file
        );
        println!(
            "Parquet file: {:?}, size: {} bytes",
            parquet_file,
            metadata.len()
        );
    }

    println!("Integration test passed!");
    println!("  - Generated 3 mock system_tables events");
    println!("  - Wrote events to DeltaLake sink");
    println!("  - Verified {} transaction log files", json_files.len());
    println!("  - Verified {} parquet data files", parquet_files.len());
    println!("  - All files exist and contain data");

    // Clean up temporary directory
    let _ = fs::remove_dir_all(&base_path);
}

#[tokio::test]
async fn test_multiple_tables_to_deltalake() {
    // Create a temporary directory for testing
    let temp_dir = std::env::temp_dir();
    let test_id = format!(
        "multi_table_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let base_path = temp_dir.join(test_id);

    fs::create_dir_all(&base_path).expect("Failed to create base directory");

    // Generate events for two different tables
    let mut all_events = Vec::new();

    // Table 1: hist_cluster_statements_summary
    for i in 0..2 {
        let mut log = LogEvent::from(BTreeMap::new());
        log.insert("_vector_table", "hist_cluster_statements_summary");
        log.insert("_vector_source_table", "CLUSTER_STATEMENTS_SUMMARY");
        log.insert("_vector_source_schema", "metrics_schema");
        log.insert("_vector_instance", format!("tidb-{}", i));
        log.insert("_vector_timestamp", chrono::Utc::now().to_rfc3339());
        log.insert("DIGEST", format!("digest_{}", i));
        log.insert("EXEC_COUNT", (i + 1) as i64 * 100);

        let mut schema_meta = ObjectMap::new();
        schema_meta.insert(
            "_partition_by".into(),
            vector_lib::event::Value::from("DIGEST"),
        );

        let mut digest_meta = ObjectMap::new();
        digest_meta.insert(
            "mysql_type".into(),
            vector_lib::event::Value::from("varchar(64)"),
        );
        schema_meta.insert(
            "DIGEST".into(),
            vector_lib::event::Value::Object(digest_meta),
        );

        let mut exec_count_meta = ObjectMap::new();
        exec_count_meta.insert(
            "mysql_type".into(),
            vector_lib::event::Value::from("bigint"),
        );
        schema_meta.insert(
            "EXEC_COUNT".into(),
            vector_lib::event::Value::Object(exec_count_meta),
        );

        log.insert(
            "_schema_metadata",
            vector_lib::event::Value::Object(schema_meta),
        );

        all_events.push(Event::Log(log));
    }

    // Table 2: hist_slow_query
    for i in 0..2 {
        let mut log = LogEvent::from(BTreeMap::new());
        log.insert("_vector_table", "hist_slow_query");
        log.insert("_vector_source_table", "SLOW_QUERY");
        log.insert("_vector_source_schema", "metrics_schema");
        log.insert("_vector_instance", format!("tidb-{}", i));
        log.insert("_vector_timestamp", chrono::Utc::now().to_rfc3339());
        log.insert("QUERY_ID", format!("query_{}", i));
        log.insert("DURATION", (i + 1) as i64 * 1000);

        let mut schema_meta = ObjectMap::new();
        schema_meta.insert(
            "_partition_by".into(),
            vector_lib::event::Value::from("QUERY_ID"),
        );

        let mut query_id_meta = ObjectMap::new();
        query_id_meta.insert(
            "mysql_type".into(),
            vector_lib::event::Value::from("varchar(64)"),
        );
        schema_meta.insert(
            "QUERY_ID".into(),
            vector_lib::event::Value::Object(query_id_meta),
        );

        let mut duration_meta = ObjectMap::new();
        duration_meta.insert(
            "mysql_type".into(),
            vector_lib::event::Value::from("bigint"),
        );
        schema_meta.insert(
            "DURATION".into(),
            vector_lib::event::Value::Object(duration_meta),
        );

        log.insert(
            "_schema_metadata",
            vector_lib::event::Value::Object(schema_meta),
        );

        all_events.push(Event::Log(log));
    }

    // Write table 1
    let table1_path = base_path.join("hist_cluster_statements_summary");
    let mut writer1 = DeltaLakeWriter::new(
        table1_path.clone(),
        DeltaTableConfig {
            name: "hist_cluster_statements_summary".to_string(),
            schema_evolution: Some(true),
        },
        WriteConfig {
            batch_size: 1000,
            timeout_secs: 30,
        },
        None,
    );

    let table1_events: Vec<_> = all_events
        .iter()
        .filter(|e| {
            if let Event::Log(log) = e {
                log.get("_vector_table")
                    .and_then(|v| v.as_str())
                    .map(|s| s == "hist_cluster_statements_summary")
                    .unwrap_or(false)
            } else {
                false
            }
        })
        .cloned()
        .collect();

    writer1
        .write_events(table1_events)
        .await
        .expect("Failed to write table 1");

    // Write table 2
    let table2_path = base_path.join("hist_slow_query");
    let mut writer2 = DeltaLakeWriter::new(
        table2_path.clone(),
        DeltaTableConfig {
            name: "hist_slow_query".to_string(),
            schema_evolution: Some(true),
        },
        WriteConfig {
            batch_size: 1000,
            timeout_secs: 30,
        },
        None,
    );

    let table2_events: Vec<_> = all_events
        .iter()
        .filter(|e| {
            if let Event::Log(log) = e {
                log.get("_vector_table")
                    .and_then(|v| v.as_str())
                    .map(|s| s == "hist_slow_query")
                    .unwrap_or(false)
            } else {
                false
            }
        })
        .cloned()
        .collect();

    writer2
        .write_events(table2_events)
        .await
        .expect("Failed to write table 2");

    // Verify both tables have Delta Lake structure
    assert!(table1_path.exists(), "Table 1 should exist");
    assert!(table2_path.exists(), "Table 2 should exist");

    assert!(
        table1_path.join("_delta_log").exists(),
        "Table 1 should have _delta_log"
    );
    assert!(
        table2_path.join("_delta_log").exists(),
        "Table 2 should have _delta_log"
    );

    println!("Multi-table test passed!");
    println!("  - Table 1: hist_cluster_statements_summary");
    println!("  - Table 2: hist_slow_query");

    // Clean up
    let _ = fs::remove_dir_all(&base_path);
}

/// Test that sqlstatement events with an evolved schema (new column added) can be written
/// to the same Delta table, and that old data files remain valid alongside new ones.
///
/// Scenario:
///   Batch 1 (old schema): DIGEST, EXEC_COUNT, AVG_LATENCY, INSTANCE
///   Batch 2 (new schema): same columns + PLAN_DIGEST (new column added)
///
/// Expected:
///   - Both batches write successfully (SchemaMode::Merge handles the evolution)
///   - Delta transaction log records at least 2 versions
///   - The merged schema in the log contains PLAN_DIGEST
///   - Old parquet files (batch 1) do NOT contain PLAN_DIGEST — that is fine;
///     Delta Lake treats missing columns as null when reading the full table
#[tokio::test]
async fn test_sqlstatement_schema_evolution_add_column() {
    use vector_extensions::sinks::deltalake::{DeltaLakeWriter, DeltaTableConfig, WriteConfig};

    let temp_dir = std::env::temp_dir();
    let test_id = format!(
        "sqlstmt_schema_evo_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let table_path = temp_dir.join(test_id);
    fs::create_dir_all(&table_path).expect("Failed to create table directory");
    println!("Test table path: {:?}", table_path);

    let table_name = "hist_cluster_statements_summary";
    let write_config = WriteConfig {
        batch_size: 1000,
        timeout_secs: 30,
    };

    // -----------------------------------------------------------------------
    // Batch 1: write 3 events with the original schema (no PLAN_DIGEST)
    // -----------------------------------------------------------------------
    let batch1: Vec<Event> = (0..3)
        .map(|i| {
            make_sqlstatement_event(
                i,
                table_name,
                &format!("tidb-{}", i),
                &[], // no extra schema fields
                &[], // no extra values
            )
        })
        .collect();

    let mut writer1 = DeltaLakeWriter::new(
        table_path.clone(),
        DeltaTableConfig {
            name: table_name.to_string(),
            schema_evolution: Some(true),
        },
        write_config.clone(),
        None,
    );

    writer1
        .write_events(batch1)
        .await
        .expect("Batch 1 (old schema) write failed");

    println!("Batch 1 written with old schema (no PLAN_DIGEST)");

    // Verify table was created
    let delta_log_path = table_path.join("_delta_log");
    assert!(
        delta_log_path.exists(),
        "_delta_log must exist after batch 1"
    );

    let log_files_after_batch1: Vec<_> = fs::read_dir(&delta_log_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "json").unwrap_or(false))
        .collect();
    println!(
        "Transaction log files after batch 1: {}",
        log_files_after_batch1.len()
    );
    assert!(
        !log_files_after_batch1.is_empty(),
        "Should have transaction log after batch 1"
    );

    // Verify old schema in log does NOT mention PLAN_DIGEST
    let plan_digest_in_batch1 = log_files_after_batch1.iter().any(|e| {
        fs::read_to_string(e.path())
            .unwrap_or_default()
            .contains("PLAN_DIGEST")
    });
    assert!(
        !plan_digest_in_batch1,
        "Batch 1 schema should not contain PLAN_DIGEST yet"
    );

    // -----------------------------------------------------------------------
    // Batch 2: new writer to the same table, schema now includes PLAN_DIGEST
    //
    // Using a fresh DeltaLakeWriter simulates what happens when the process
    // restarts and the schema has been extended (e.g., TiDB added a column
    // to CLUSTER_STATEMENTS_SUMMARY).
    // -----------------------------------------------------------------------
    let batch2: Vec<Event> = (3..6)
        .map(|i| {
            make_sqlstatement_event(
                i,
                table_name,
                &format!("tidb-{}", i),
                // New column in schema metadata
                &[("PLAN_DIGEST", "varchar(64)")],
                // New column value
                &[(
                    "PLAN_DIGEST",
                    vector_lib::event::Value::from(format!("plan_{}", i)),
                )],
            )
        })
        .collect();

    let mut writer2 = DeltaLakeWriter::new(
        table_path.clone(),
        DeltaTableConfig {
            name: table_name.to_string(),
            schema_evolution: Some(true),
        },
        write_config,
        None,
    );

    writer2
        .write_events(batch2)
        .await
        .expect("Batch 2 (new schema with PLAN_DIGEST) write failed");

    println!("Batch 2 written with evolved schema (PLAN_DIGEST added)");

    // -----------------------------------------------------------------------
    // Assertions: verify schema evolution is reflected in the transaction log
    // -----------------------------------------------------------------------
    let log_files_after_batch2: Vec<_> = fs::read_dir(&delta_log_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "json").unwrap_or(false))
        .collect();

    println!(
        "Transaction log files after batch 2: {}",
        log_files_after_batch2.len()
    );

    // After a schema-evolving write, Delta Lake records a new version
    assert!(
        log_files_after_batch2.len() > log_files_after_batch1.len(),
        "Expected more transaction log versions after schema evolution write. \
         Before: {}, after: {}",
        log_files_after_batch1.len(),
        log_files_after_batch2.len()
    );

    // The new transaction log entry must reference PLAN_DIGEST
    let plan_digest_in_batch2 = log_files_after_batch2.iter().any(|e| {
        fs::read_to_string(e.path())
            .unwrap_or_default()
            .contains("PLAN_DIGEST")
    });
    assert!(
        plan_digest_in_batch2,
        "Transaction log after batch 2 must mention PLAN_DIGEST (schema was evolved)"
    );

    // -----------------------------------------------------------------------
    // Collect all parquet files to confirm both batches produced data files
    // -----------------------------------------------------------------------
    fn collect_parquet_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut result = Vec::new();
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() && !path.to_string_lossy().contains("_delta_log") {
                    result.extend(collect_parquet_files(&path));
                } else if path.extension().map(|x| x == "parquet").unwrap_or(false) {
                    result.push(path);
                }
            }
        }
        result
    }

    let parquet_files = collect_parquet_files(&table_path);
    println!("Total parquet files: {}", parquet_files.len());
    assert!(
        parquet_files.len() >= 2,
        "Expected at least 2 parquet files (one per batch), found {}",
        parquet_files.len()
    );

    for f in &parquet_files {
        let size = fs::metadata(f).unwrap().len();
        assert!(size > 0, "Parquet file {:?} must not be empty", f);
        println!("  {:?}  ({} bytes)", f, size);
    }

    // -----------------------------------------------------------------------
    // DuckDB query: read the evolved table and verify null-fill for old rows
    //
    // DuckDB's delta extension uses the merged schema from the Delta log.
    // Old parquet files don't have PLAN_DIGEST, so DuckDB fills those rows
    // with NULL automatically — this is the "old data valid against new schema"
    // guarantee we want to verify.
    // -----------------------------------------------------------------------
    {
        use duckdb::Connection;

        let conn = Connection::open_in_memory().expect("Failed to open DuckDB connection");

        // Load delta extension
        let load_result = conn
            .execute("INSTALL delta;", [])
            .and_then(|_| conn.execute("LOAD delta;", []));

        if let Err(e) = load_result {
            println!(
                "Skipping DuckDB delta_scan verification (delta extension unavailable): {}",
                e
            );
        } else {
            let table_uri = format!("file://{}", table_path.to_string_lossy());

            // Total row count: both batches = 6 rows
            let total: i64 = conn
                .query_row(
                    &format!("SELECT COUNT(*) FROM delta_scan('{}')", table_uri),
                    [],
                    |row| row.get(0),
                )
                .expect("DuckDB COUNT query failed");
            println!("DuckDB: total rows = {}", total);
            assert_eq!(total, 6, "Expected 6 rows total (3 old + 3 new)");

            // Rows where PLAN_DIGEST IS NULL => batch 1 (old rows)
            let null_count: i64 = conn
                .query_row(
                    &format!(
                        "SELECT COUNT(*) FROM delta_scan('{}') WHERE PLAN_DIGEST IS NULL",
                        table_uri
                    ),
                    [],
                    |row| row.get(0),
                )
                .expect("DuckDB NULL count query failed");
            println!("DuckDB: rows with PLAN_DIGEST IS NULL (old batch) = {}", null_count);
            assert_eq!(
                null_count, 3,
                "Old 3 rows must have PLAN_DIGEST=NULL after schema merge"
            );

            // Rows where PLAN_DIGEST IS NOT NULL => batch 2 (new rows)
            let non_null_count: i64 = conn
                .query_row(
                    &format!(
                        "SELECT COUNT(*) FROM delta_scan('{}') WHERE PLAN_DIGEST IS NOT NULL",
                        table_uri
                    ),
                    [],
                    |row| row.get(0),
                )
                .expect("DuckDB NOT NULL count query failed");
            println!(
                "DuckDB: rows with PLAN_DIGEST NOT NULL (new batch) = {}",
                non_null_count
            );
            assert_eq!(
                non_null_count, 3,
                "New 3 rows must have PLAN_DIGEST populated"
            );

            println!("DuckDB delta_scan verification passed!");
            println!("  Total rows: 6 (old 3 + new 3)");
            println!("  Old rows: PLAN_DIGEST=NULL (null-filled by DuckDB for old parquet)");
            println!("  New rows: PLAN_DIGEST='plan_N' (written in batch 2)");
        }
    }

    println!("Schema evolution test passed!");
    println!("  Batch 1 (old schema, no PLAN_DIGEST): OK");
    println!("  Batch 2 (new schema, PLAN_DIGEST added): OK");
    println!("  Delta log records schema change: OK");
    println!("  Both parquet files valid: OK");

    // Clean up
    let _ = fs::remove_dir_all(&table_path);
}

/// Regression test: same writer, new column added mid-run.
///
/// This test intentionally reproduces the bug where `fixed_arrow_schema` is locked
/// after the first write. With the current implementation:
///   - Batch 1 is written with old schema (no PLAN_DIGEST) — succeeds
///   - Batch 2 arrives on the SAME writer with new schema (+PLAN_DIGEST)
///   - Because fixed_arrow_schema is already set, PLAN_DIGEST is dropped silently
///   - DuckDB delta_scan shows PLAN_DIGEST column is absent from the table schema
///
/// When the underlying bug is fixed (fixed_arrow_schema detects new columns and
/// resets), this test should be updated to assert non-null counts instead.
#[tokio::test]
async fn test_sqlstatement_same_writer_new_column_not_visible() {
    use duckdb::Connection;
    use vector_extensions::sinks::deltalake::{DeltaLakeWriter, DeltaTableConfig, WriteConfig};

    let temp_dir = std::env::temp_dir();
    let test_id = format!(
        "sqlstmt_same_writer_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let table_path = temp_dir.join(test_id);
    std::fs::create_dir_all(&table_path).expect("Failed to create table directory");

    let table_name = "hist_cluster_statements_summary";
    let write_config = WriteConfig {
        batch_size: 1000,
        timeout_secs: 30,
    };

    // Single long-running writer — simulates production process that stays up
    let mut writer = DeltaLakeWriter::new(
        table_path.clone(),
        DeltaTableConfig {
            name: table_name.to_string(),
            schema_evolution: Some(true),
        },
        write_config,
        None,
    );

    // Batch 1: old schema (no PLAN_DIGEST)
    let batch1: Vec<Event> = (0..3)
        .map(|i| {
            make_sqlstatement_event(
                i,
                table_name,
                &format!("tidb-{}", i),
                &[],
                &[],
            )
        })
        .collect();
    writer
        .write_events(batch1)
        .await
        .expect("Batch 1 write failed");

    // Batch 2: new schema with PLAN_DIGEST — same writer, no restart
    let batch2: Vec<Event> = (3..6)
        .map(|i| {
            make_sqlstatement_event(
                i,
                table_name,
                &format!("tidb-{}", i),
                &[("PLAN_DIGEST", "varchar(64)")],
                &[(
                    "PLAN_DIGEST",
                    vector_lib::event::Value::from(format!("plan_{}", i)),
                )],
            )
        })
        .collect();
    writer
        .write_events(batch2)
        .await
        .expect("Batch 2 write failed");

    // DuckDB query to check whether PLAN_DIGEST is visible
    let conn = Connection::open_in_memory().expect("Failed to open DuckDB connection");
    let load_result = conn
        .execute("INSTALL delta;", [])
        .and_then(|_| conn.execute("LOAD delta;", []));

    if let Err(e) = load_result {
        println!("Skipping DuckDB check (delta extension unavailable): {}", e);
        let _ = std::fs::remove_dir_all(&table_path);
        return;
    }

    let table_uri = format!("file://{}", table_path.to_string_lossy());

    // Check if PLAN_DIGEST column exists in the Delta schema at all
    // by trying to query it; if the column is missing, the query will error.
    let schema_has_plan_digest: bool = conn
        .execute(
            &format!(
                "SELECT PLAN_DIGEST FROM delta_scan('{}') LIMIT 1",
                table_uri
            ),
            [],
        )
        .is_ok();

    // The same writer must pick up new columns from _schema_metadata and write them
    // to Delta Lake. After schema evolution, PLAN_DIGEST should be in the table.
    assert!(
        schema_has_plan_digest,
        "same-writer schema evolution failed: PLAN_DIGEST not found in Delta table schema. \
         Root cause: fixed_arrow_schema is locked after first write and never reset when \
         new columns appear in _schema_metadata."
    );

    // Also verify null-fill: old rows have PLAN_DIGEST=NULL, new rows have it populated
    let null_count: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM delta_scan('{}') WHERE PLAN_DIGEST IS NULL",
                table_uri
            ),
            [],
            |row| row.get(0),
        )
        .expect("DuckDB NULL count query failed");
    assert_eq!(null_count, 3, "Old 3 rows must have PLAN_DIGEST=NULL");

    let non_null_count: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM delta_scan('{}') WHERE PLAN_DIGEST IS NOT NULL",
                table_uri
            ),
            [],
            |row| row.get(0),
        )
        .expect("DuckDB NOT NULL count query failed");
    assert_eq!(non_null_count, 3, "New 3 rows must have PLAN_DIGEST populated");

    println!("Same-writer schema evolution test passed!");
    println!("  Old rows: PLAN_DIGEST=NULL");
    println!("  New rows: PLAN_DIGEST='plan_N'");

    let _ = std::fs::remove_dir_all(&table_path);
}

