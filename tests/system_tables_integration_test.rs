// Integration test for system_tables source with deltalake sink
// Tests the complete data pipeline: mock data -> system_tables events -> deltalake -> verify files

#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]

use std::collections::BTreeMap;
use std::fs;
use vector_lib::event::{Event, LogEvent, ObjectMap};

// Import DeltaLake writer components
use vector_extensions::sinks::deltalake::{writer::DeltaLakeWriter, DeltaTableConfig, WriteConfig};

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

#[tokio::test]
async fn test_statements_summary_multi_node_filter() {
    use chrono::{Duration, Utc};
    use serde_json::json;
    use std::collections::HashMap;
    use vector_extensions::sources::system_tables::collectors::coprocessor_collector::apply_time_filter;

    // Simulate 3 TiDB nodes with different last-active times
    // Node 1: inactive for 3 days (very old window)
    // Node 2: inactive for 7 hours (old window)
    // Node 3: active recently (current window, within 30 minutes)

    let now = Utc::now().naive_utc();
    let node1_end = (now - Duration::days(3)).format("%Y-%m-%d %H:%M:%S").to_string();
    let node1_begin = (now - Duration::days(3) - Duration::minutes(30)).format("%Y-%m-%d %H:%M:%S").to_string();

    let node2_end = (now - Duration::hours(7)).format("%Y-%m-%d %H:%M:%S").to_string();
    let node2_begin = (now - Duration::hours(7) - Duration::minutes(30)).format("%Y-%m-%d %H:%M:%S").to_string();

    let node3_end = (now - Duration::minutes(10)).format("%Y-%m-%d %H:%M:%S").to_string();
    let node3_begin = (now - Duration::minutes(40)).format("%Y-%m-%d %H:%M:%S").to_string();

    let mut rows = Vec::new();

    // Node 1 rows (very old, should be filtered out)
    for i in 0..4 {
        let mut row = HashMap::new();
        row.insert("SUMMARY_BEGIN_TIME".to_string(), json!(node1_begin));
        row.insert("SUMMARY_END_TIME".to_string(), json!(node1_end));
        row.insert("DIGEST_TEXT".to_string(), json!(format!("old_query_{}", i)));
        row.insert("EXEC_COUNT".to_string(), json!(1));
        rows.push(row);
    }

    // Node 2 rows (old, should be filtered out)
    for i in 0..2 {
        let mut row = HashMap::new();
        row.insert("SUMMARY_BEGIN_TIME".to_string(), json!(node2_begin));
        row.insert("SUMMARY_END_TIME".to_string(), json!(node2_end));
        row.insert("DIGEST_TEXT".to_string(), json!(format!("stale_query_{}", i)));
        row.insert("EXEC_COUNT".to_string(), json!(1));
        rows.push(row);
    }

    // Node 3 rows (recent, should be kept)
    for i in 0..4 {
        let mut row = HashMap::new();
        row.insert("SUMMARY_BEGIN_TIME".to_string(), json!(node3_begin));
        row.insert("SUMMARY_END_TIME".to_string(), json!(node3_end));
        row.insert("DIGEST_TEXT".to_string(), json!(format!("recent_query_{}", i)));
        row.insert("EXEC_COUNT".to_string(), json!(1));
        rows.push(row);
    }

    assert_eq!(rows.len(), 10, "Should have 10 total rows before filtering");

    // Apply the auto-filter (1 hour window)
    let where_clause = "SUMMARY_END_TIME >= DATE_SUB(NOW(), INTERVAL 1 HOUR)";
    let filtered = apply_time_filter(rows, where_clause);

    // Only Node 3's recent rows should remain
    assert_eq!(filtered.len(), 4, "Should keep only the 4 recent rows from active node");

    for row in &filtered {
        let digest = row.get("DIGEST_TEXT").unwrap().as_str().unwrap();
        assert!(
            digest.starts_with("recent_query_"),
            "Filtered rows should only contain recent queries, got: {}",
            digest
        );
    }

    println!("Multi-node filter test passed!");
    println!("  - Node 1 (3 days old): 4 rows filtered out");
    println!("  - Node 2 (7 hours old): 2 rows filtered out");
    println!("  - Node 3 (recent): 4 rows kept");
}
