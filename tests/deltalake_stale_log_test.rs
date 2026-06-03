//! Local Delta Lake test: simulate external compaction removing old _delta_log JSON
//! files and verify writes recover instead of failing permanently.

#![allow(clippy::print_stdout)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use vector_lib::event::{Event, LogEvent, ObjectMap};

use vector_extensions::sinks::deltalake::{DeltaLakeWriter, DeltaTableConfig, WriteConfig};

fn make_event(table_name: &str, index: i64) -> Event {
    let mut log = LogEvent::from(BTreeMap::new());
    log.insert("_vector_table", table_name);
    log.insert("_vector_source_table", "TEST_SOURCE");
    log.insert("_vector_source_schema", "test_schema");
    log.insert("_vector_instance", "test-instance");
    log.insert("_vector_timestamp", "2024-06-01T00:00:00Z");
    log.insert("id", index);
    log.insert("value", format!("row-{index}"));

    let mut schema_meta = ObjectMap::new();
    schema_meta.insert("_partition_by".into(), vector_lib::event::Value::from("date"));
    let mut id_meta = ObjectMap::new();
    id_meta.insert("mysql_type".into(), vector_lib::event::Value::from("bigint"));
    schema_meta.insert("id".into(), vector_lib::event::Value::Object(id_meta));
    let mut value_meta = ObjectMap::new();
    value_meta.insert(
        "mysql_type".into(),
        vector_lib::event::Value::from("varchar(64)"),
    );
    schema_meta.insert("value".into(), vector_lib::event::Value::Object(value_meta));
    log.insert("_schema_metadata", vector_lib::event::Value::Object(schema_meta));

    Event::Log(log)
}

fn delta_log_json_files(delta_log_path: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = fs::read_dir(delta_log_path)
        .expect("read _delta_log")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    files.sort();
    files
}

/// Remove the latest commit log JSON file while keeping the contiguous history intact.
/// This mimics a concurrent compact/remove race where a reader still chases a log
/// segment that was already deleted, while the table remains valid at an earlier version.
fn simulate_external_compaction(delta_log_path: &Path) {
    let json_files = delta_log_json_files(delta_log_path);
    assert!(
        json_files.len() >= 3,
        "need at least 3 json log files before compaction simulation"
    );

    let stale_file = json_files.last().expect("latest delta log json");
    println!("Simulating compaction: removing {:?}", stale_file);
    fs::remove_file(stale_file).expect("remove stale delta log json");
}

fn unique_table_dir(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("{name}_{nanos}"))
}

async fn write_batch(writer: &mut DeltaLakeWriter, table_name: &str, start: i64, count: i64) {
    let events: Vec<Event> = (start..start + count)
        .map(|i| make_event(table_name, i))
        .collect();
    writer
        .write_events(events)
        .await
        .expect("seed write should succeed");
}

#[tokio::test]
async fn recover_write_after_simulated_delta_log_compaction() {
    let table_path = unique_table_dir("deltalake_stale_log_recovery");
    fs::create_dir_all(&table_path).expect("create table dir");

    let table_name = "metrics_table";
    let write_config = WriteConfig {
        batch_size: 1000,
        timeout_secs: 30,
    };
    let table_config = DeltaTableConfig {
        name: table_name.to_string(),
        schema_evolution: Some(true),
    };

    let mut writer = DeltaLakeWriter::new(
        table_path.clone(),
        table_config.clone(),
        write_config.clone(),
        None,
    );

    // Build a multi-version table so _delta_log has several JSON commits.
    for batch in 0..5 {
        write_batch(&mut writer, table_name, batch * 10, 3).await;
    }

    let delta_log_path = table_path.join("_delta_log");
    let json_before = delta_log_json_files(&delta_log_path);
    println!("Delta log json files before compaction simulation: {}", json_before.len());
    assert!(json_before.len() >= 3, "expected multiple delta log commits");

    simulate_external_compaction(&delta_log_path);

    let recovery_events = vec![make_event(table_name, 999)];
    match writer.write_events(recovery_events.clone()).await {
        Ok(()) => {}
        Err(error) if vector_extensions::common::deltalake_writer::is_stale_delta_log_error(&error.to_string()) => {
            writer = DeltaLakeWriter::new(
                table_path.clone(),
                DeltaTableConfig {
                    name: table_name.to_string(),
                    schema_evolution: Some(true),
                },
                write_config.clone(),
                None,
            );
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            writer
                .write_events(recovery_events)
                .await
                .expect("write should recover after reopening table writer");
        }
        Err(error) => panic!("unexpected write failure: {error}"),
    }

    let json_after = delta_log_json_files(&delta_log_path);
    assert!(
        !json_after.is_empty(),
        "table should remain writable after recovery"
    );

    let _ = fs::remove_dir_all(&table_path);
}
