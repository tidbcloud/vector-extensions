use chrono::{DateTime, Timelike};
use ordered_float::NotNan;
use serde_json::Value;
use vector_lib::event::{Event, KeyString, LogEvent, Value as LogValue};
use tracing::instrument::Instrument;
use futures::StreamExt;
use tokio::time;
use tokio_stream::wrappers::IntervalStream;
use vector::shutdown::ShutdownSignal;
use crate::sources::mocked_topsql::shutdown::{pair, ShutdownNotifier, ShutdownSubscriber};
use vector::{internal_events::StreamClosedError, SourceSender};
use std::time::Duration;
use rand::Rng;
use rand::distr::{Alphanumeric, Uniform, StandardUniform};
use std::collections::BTreeMap;
use crc32fast::Hasher as Crc32Hasher;

const SQL_CONSTANT: &str = "SELECT
  `tbl_test_001`.`column0`,
  `tbl_test_001`.`column1`,
  `tbl_test_001`.`column2`,
  `tbl_test_001`.`column3`,
  `tbl_test_001`.`column4`,
  `tbl_test_001`.`column5`,
  `tbl_test_001`.`column6`,
  `tbl_test_001`.`column7`,
  `tbl_test_001`.`column8`,
  `tbl_test_001`.`column9`,
  `tbl_test_001`.`column10`,
  `tbl_test_001`.`column11`,
  `tbl_test_001`.`column12`,
  `tbl_test_001`.`column13`,
  `tbl_test_001`.`column14`,
  `tbl_test_001`.`column15`,
  `tbl_test_001`.`column16`,
  `tbl_test_001`.`column17`,
  `tbl_test_001`.`column18`,
  `tbl_test_001`.`column19`,
  `tbl_test_001`.`column20`,
  `tbl_test_001`.`column21`,
  `tbl_test_001`.`column22`,
  `tbl_test_001`.`column23`,
  `tbl_test_001`.`column24`,
  `tbl_test_001`.`column25`,
  `tbl_test_001`.`column26`,
  `tbl_test_001`.`column27`,
  `tbl_test_001`.`column28`,
  `tbl_test_001`.`column29`,
  `tbl_test_001`.`column30`,
  `tbl_test_001`.`column31`,
  `tbl_test_001`.`column32`,
  `tbl_test_001`.`column33`,
  `tbl_test_001`.`column34`,
  `tbl_test_001`.`column35`,
  `tbl_test_001`.`column36`,
  `tbl_test_001`.`column37`,
  `tbl_test_001`.`column38`,
  `tbl_test_001`.`column39`,
  `tbl_test_001`.`column40`,
  `tbl_test_001`.`column41`,
  `tbl_test_001`.`column42`,
  `tbl_test_001`.`column43`,
  `tbl_test_001`.`column44`,
  `tbl_test_001`.`column45`,
  `tbl_test_001`.`column46`,
  `tbl_test_001`.`column47`,
  `tbl_test_001`.`column48`,
  `tbl_test_001`.`column49`,
  `tbl_test_001`.`column50`,
  `tbl_test_001`.`column51`,
  `tbl_test_001`.`column52`,
  `tbl_test_001`.`column53`,
  `tbl_test_001`.`column54`,
  `tbl_test_001`.`column55`,
  `tbl_test_001`.`column56`,
  `tbl_test_001`.`column57`,
  `tbl_test_001`.`column58`,
  `tbl_test_001`.`column59`,
  `tbl_test_001`.`column60`,
  `tbl_test_001`.`column61`,
  `tbl_test_001`.`column62`,
  `tbl_test_001`.`column63`,
  `tbl_test_001`.`column64`,
  `tbl_test_001`.`column65`,
  `tbl_test_001`.`column66`
FROM
  `tbl_test_001`
WHERE
  `column0` = ?
  AND `column1` = ?
LIMIT
  ?";

const PLAN_CONSTANT: &str = "	Projection   	root	db_test_0001.tbl_test_001.column0, db_test_0001.tbl_test_001.column1, db_test_0001.tbl_test_001.column2, db_test_0001.tbl_test_001.column3, db_test_0001.tbl_test_001.column4, db_test_0001.tbl_test_001.column5, db_test_0001.tbl_test_001.column6, db_test_0001.tbl_test_001.column7, db_test_0001.tbl_test_001.column8, db_test_0001.tbl_test_001.column9, db_test_0001.tbl_test_001.column10, db_test_0001.tbl_test_001.column11, db_test_0001.tbl_test_001.column12, db_test_0001.tbl_test_001.column13, db_test_0001.tbl_test_001.column14, db_test_0001.tbl_test_001.column15, db_test_0001.tbl_test_001.column16, db_test_0001.tbl_test_001.column17, db_test_0001.tbl_test_001.column18, db_test_0001.tbl_test_001.column19, db_test_0001.tbl_test_001.column20, db_test_0001.tbl_test_001.column21, db_test_0001.tbl_test_001.column22, db_test_0001.tbl_test_001.column23, db_test_0001.tbl_test_001.column24, db_test_0001.tbl_test_001.column25, db_test_0001.tbl_test_001.column26, db_test_0001.tbl_test_001.column27, db_test_0001.tbl_test_001.column28, db_test_0001.tbl_test_001.column29, db_test_0001.tbl_test_001.column30, db_test_0001.tbl_test_001.column31, db_test_0001.tbl_test_001.column32, db_test_0001.tbl_test_001.column33, db_test_0001.tbl_test_001.column34, db_test_0001.tbl_test_001.column35, db_test_0001.tbl_test_001.column36, db_test_0001.tbl_test_001.column37, db_test_0001.tbl_test_001.column38, db_test_0001.tbl_test_001.column39, db_test_0001.tbl_test_001.column40, db_test_0001.tbl_test_001.column41, db_test_0001.tbl_test_001.column42, db_test_0001.tbl_test_001.column43, db_test_0001.tbl_test_001.column44, db_test_0001.tbl_test_001.column45, db_test_0001.tbl_test_001.column46, db_test_0001.tbl_test_001.column47, db_test_0001.tbl_test_001.column48, db_test_0001.tbl_test_001.column49, db_test_0001.tbl_test_001.column50, db_test_0001.tbl_test_001.column51, db_test_0001.tbl_test_001.column52, db_test_0001.tbl_test_001.column53, db_test_0001.tbl_test_001.column54, db_test_0001.tbl_test_001.column55, db_test_0001.tbl_test_001.column56, db_test_0001.tbl_test_001.column57, db_test_0001.tbl_test_001.column58, db_test_0001.tbl_test_001.column59, db_test_0001.tbl_test_001.column60, db_test_0001.tbl_test_001.column61, db_test_0001.tbl_test_001.column62, db_test_0001.tbl_test_001.column63, db_test_0001.tbl_test_001.column64, db_test_0001.tbl_test_001.column65, db_test_0001.tbl_test_001.column66
	└─Limit      	root	
	  └─Point_Get	root	table:tbl_test_001, index:udx_column0_useridx_column1(column0, column1)";

fn generate_random_int() -> Vec<i32> {
    let mut rng = rand::rng();
    let arr1: [i32; 1000] = rng.random();
    arr1.to_vec()
}

fn generate_random_bigint() -> Vec<i64> {
    let mut rng = rand::rng();
    let arr1: [i64; 1000] = rng.random();
    arr1.to_vec()
}

fn generate_random_string(num_strings: i32, string_length: usize) -> Vec<String> {
    let random_strings: Vec<String> = (0..num_strings)
        .map(|_| {
            rand::thread_rng() // 获取线程局部的随机数生成器
                .sample_iter(&Alphanumeric) // 从 Alphanumeric 分布中创建迭代器
                .take(string_length) // 取指定长度的字符
                .map(char::from) // 将 u8 转换为 char
                .collect() // 收集成 String
        })
        .collect(); // 收集成 Vec<String>
    random_strings
}
fn generate_random_digest() -> Vec<String> {
    generate_random_string(100000, 64)
}

fn create_event_for_instance_partition(timestamp: i64, tidb_number: usize, tikv_number: usize, instance_part: usize) -> (Vec<Event>, Vec<u32>, Vec<u32>) {
    let mut events = vec![];
    let mut tidb_instance_partition_vec = vec![];
    let mut tikv_instance_partition_vec = vec![];
    for i in 0..tidb_number {
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();

        // Add metadata with Vector prefix (ensure all fields have values)
        log.insert("source_table", "instance_partition");
        log.insert("timestamps", LogValue::from(timestamp));
        log.insert("instance_type", "tidb");
        log.insert("instance", format!("127.0.1.{}", i));

        // Calculate CRC32 for (instance, instance_type)
        let instance_key = format!("127.0.1.{}_tidb", i);
        let mut hasher = Crc32Hasher::new();
        hasher.update(instance_key.as_bytes());
        let crc_value = hasher.finalize();

        // Calculate partition by taking modulo
        // Use max(1, partition_number) to avoid division by zero
        let partition_mod = if instance_part == 0 { 1 } else { instance_part };
        let calculated_partition = (crc_value % partition_mod as u32) as u32;
        tidb_instance_partition_vec.push(calculated_partition);
        log.insert("instance_partition_id", LogValue::from(calculated_partition));
        events.push(event);
    }
    for i in 0..tikv_number {
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();
        log.insert("source_table", "instance_partition");
        log.insert("timestamps", LogValue::from(timestamp));
        log.insert("instance_type", "tikv");
        log.insert("instance", format!("127.0.0.{}", i));
        // Calculate CRC32 for (instance, instance_type)
        let instance_key = format!("127.0.0.{}_tikv", i);
        let mut hasher = Crc32Hasher::new();
        hasher.update(instance_key.as_bytes());
        let crc_value = hasher.finalize();

        // Calculate partition by taking modulo
        // Use max(1, partition_number) to avoid division by zero
        let partition_mod = if instance_part == 0 { 1 } else { instance_part };
        let calculated_partition = (crc_value % partition_mod as u32) as u32;
        tikv_instance_partition_vec.push(calculated_partition);
        log.insert("instance_partition_id", LogValue::from(calculated_partition));
        events.push(event);
    }
    (events, tidb_instance_partition_vec, tikv_instance_partition_vec)
}

fn create_event_for_tidb_instance(index : usize) -> Event {
    let mut event = Event::Log(LogEvent::default());
    let log = event.as_mut_log();
    // Add metadata with Vector prefix (ensure all fields have values)
    log.insert("source_table", "instance");
    log.insert("timestamps", LogValue::from(chrono::Utc::now().timestamp()));
    log.insert("time", LogValue::from(chrono::Utc::now().timestamp()));
    log.insert("instance_type", "tidb");
    log.insert("instance", format!("10.2.12.{}", index));
    event
}
fn create_event_for_tikv_instance(index : usize) -> Event {
    let mut event = Event::Log(LogEvent::default());
    let log = event.as_mut_log();
    // Add metadata with Vector prefix (ensure all fields have values)
    log.insert("source_table", "instance");
    log.insert("timestamps", LogValue::from(chrono::Utc::now().timestamp()));
    log.insert("time", LogValue::from(chrono::Utc::now().timestamp()));
    log.insert("instance_type", "tikv");
    log.insert("instance", format!("10.2.12.{}", index));
    event
}

/// Create a Vector event from tidb sql meta
fn create_event_for_tidb_sql_plan_meta(sql_digest: &Vec<String>, plan_digest: &Vec<String>, random_str_vec: &Vec<String>, offset: usize) -> (Vec<Event>, Vec<Event>) {
    let mut sql_events = vec![];
    for index in 0..5000 {
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();

        // Add metadata with Vector prefix (ensure all fields have values)
        log.insert("source_table", "tidb_sql_meta");
        log.insert("sql_digest", LogValue::from(sql_digest[offset*5000+index].to_string()));
        log.insert("normalized_sql", LogValue::from(SQL_CONSTANT.to_string().replace("tbl_test_001", random_str_vec[offset*5000+index].as_str())));
        sql_events.push(event);
    }
    let mut plan_events = vec![];
    for index in 0..5000 {
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();

        // Add metadata with Vector prefix (ensure all fields have values)
        log.insert("source_table", "tidb_plan_meta");
        log.insert("plan_digest", LogValue::from(plan_digest[offset*5000+index].to_string()));
        log.insert("normalized_plan", LogValue::from(PLAN_CONSTANT.to_string().replace("tbl_test_001", random_str_vec[offset*5000+index].as_str())));
        plan_events.push(event);
    }    
    (sql_events, plan_events)
}
/// Create a Vector event from table data
fn create_event_for_tidb_sql(index: usize, timestamp: i64, sql_digest_vec: &Vec<String>, plan_digest_vec: &Vec<String>,
    cpu_time_vec: &Vec<i32>, tikv_exec_count_vec: &Vec<i64>,
    stmt_exec_count_vec: &Vec<i64>, stmt_duration_sum_vec: &Vec<i64>,
    stmt_duration_count_vec: &Vec<i64>, top_n: usize, instance_part: usize) -> Vec<Event> {
    let mut events = vec![];
    for i in 0..(top_n + top_n / 2) {
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();

        // Add metadata with Vector prefix (ensure all fields have values)
        log.insert("source_table", "tidb_topsql");
        log.insert("timestamps", LogValue::from(timestamp));
        log.insert("time", LogValue::from(timestamp));
        // Calculate datetime string: %Y-%m-%d %H where %H is time slot index (0-3)
        // Skip current event if timestamp conversion fails
        let dt = match DateTime::from_timestamp(timestamp, 0) {
            Some(dt) => dt,
            None => continue,
        };
        let naive_dt = dt.naive_utc();
        let date = naive_dt.date();
        let hour = naive_dt.hour();
        // Calculate time slot index: 0-6=0, 6-12=1, 12-18=2, 18-24=3
        let time_slot = (hour / 6) as u32;
        let datetime_str = format!("{} {}", date.format("%Y-%m-%d"), time_slot);
        log.insert("datetime", LogValue::from(datetime_str));
        log.insert("instance_type", "tidb");
        log.insert("instance", format!("127.0.1.{}", index));
        log.insert("instance_partition_id", LogValue::from(instance_part));
        log.insert("sql_digest", sql_digest_vec[i+index].clone());
        log.insert("plan_digest", plan_digest_vec[i+index].clone());
        log.insert("topsql_cpu_time_ms", LogValue::from(cpu_time_vec[i]));
        log.insert("topsql_stmt_exec_count", LogValue::from(stmt_exec_count_vec[i]));
        log.insert("topsql_stmt_duration_sum_ns", LogValue::from(stmt_duration_sum_vec[i]));
        log.insert("topsql_stmt_duration_count", LogValue::from(stmt_duration_count_vec[i]));
        let mut tikv_exec_count = BTreeMap::<KeyString, LogValue>::new();
        tikv_exec_count.insert(
            KeyString::from(format!("127.0.0.{}", index+i)),
            LogValue::from(tikv_exec_count_vec[i]),
        );
        tikv_exec_count.insert(
            KeyString::from(format!("127.0.0.{}", index+i+1)),
            LogValue::from(tikv_exec_count_vec[i]),
        );
        log.insert(
            "topsql_tikv_stmt_exec_count",
            LogValue::Object(tikv_exec_count),
        );
        events.push(event);
    }
    events
}

/// Create a Vector event from table data
fn create_event_for_tikv_sql(
    index: usize, timestamp: i64, sql_digest_vec: &Vec<String>, plan_digest_vec: &Vec<String>,
    cpu_time_vec: &Vec<i32>, read_keys_vec: &Vec<i32>,
    network_in_vec: &Vec<i64>, network_out_vec: &Vec<i64>,
    logical_read_vec: &Vec<i64>, logical_write_vec: &Vec<i64>, top_n: usize, instance_part: usize) -> Vec<Event> {
    let mut events = vec![];
    for i in 0..(top_n + top_n) {
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();

        // Add metadata with Vector prefix (ensure all fields have values)
        log.insert("source_table", "tikv_topsql");
        log.insert("timestamps", LogValue::from(timestamp));
        log.insert("time", LogValue::from(timestamp));
        // Calculate datetime string: %Y-%m-%d %H where %H is time slot index (0-3)
        // Skip current event if timestamp conversion fails
        let dt = match DateTime::from_timestamp(timestamp, 0) {
            Some(dt) => dt,
            None => continue,
        };
        let naive_dt = dt.naive_utc();
        let date = naive_dt.date();
        let hour = naive_dt.hour();
        // Calculate time slot index: 0-6=0, 6-12=1, 12-18=2, 18-24=3
        let time_slot = (hour / 6) as u32;
        let datetime_str = format!("{} {}", date.format("%Y-%m-%d"), time_slot);
        log.insert("datetime", LogValue::from(datetime_str));
        log.insert("instance_type", "tikv");
        log.insert("instance", format!("127.0.0.{}", index));
        log.insert("instance_partition_id", LogValue::from(instance_part));
        log.insert("sql_digest", sql_digest_vec[i+index].clone());
        log.insert("plan_digest", plan_digest_vec[i+index].clone());
        log.insert("topsql_cpu_time_ms", LogValue::from(cpu_time_vec[i]));
        log.insert("topsql_read_keys", LogValue::from(read_keys_vec[i]));
        log.insert("topsql_write_keys", LogValue::from(0));
        log.insert(
            "topsql_network_in_bytes",
            LogValue::from(network_in_vec[i]),
        );
        log.insert(
            "topsql_network_out_bytes",
            LogValue::from(network_out_vec[i]),
        );
        log.insert(
            "topsql_logical_read_bytes",
            LogValue::from(logical_read_vec[i]),
        );
        log.insert(
            "topsql_logical_write_bytes",
            LogValue::from(logical_write_vec[i]),
        );
        events.push(event);
    }    
    events
}

/// Create a Vector event from table data
fn create_event_for_tikv_region(
    index: usize, timestamp: i64, region_id_vec: &Vec<i32>,
    cpu_time_vec: &Vec<i32>, read_keys_vec: &Vec<i32>,
    network_in_vec: &Vec<i64>, network_out_vec: &Vec<i64>,
    logical_read_vec: &Vec<i64>, logical_write_vec: &Vec<i64>, top_n: usize, instance_part: usize) -> Vec<Event> {
    let mut events = vec![];
    for i in 0..(top_n + top_n) {
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();

        // Add metadata with Vector prefix (ensure all fields have values)
        log.insert("source_table", "tikv_topregion");
        log.insert("timestamps", LogValue::from(timestamp));
        log.insert("time", LogValue::from(timestamp));
        // Calculate datetime string: %Y-%m-%d %H where %H is time slot index (0-3)
        // Skip current event if timestamp conversion fails
        let dt = match DateTime::from_timestamp(timestamp, 0) {
            Some(dt) => dt,
            None => continue,
        };
        let naive_dt = dt.naive_utc();
        let date = naive_dt.date();
        let hour = naive_dt.hour();
        // Calculate time slot index: 0-6=0, 6-12=1, 12-18=2, 18-24=3
        let time_slot = (hour / 6) as u32;
        let datetime_str = format!("{} {}", date.format("%Y-%m-%d"), time_slot);
        log.insert("datetime", LogValue::from(datetime_str));
        log.insert("instance_type", "tikv");
        log.insert("instance", format!("127.0.0.{}", index));
        log.insert("instance_partition_id", LogValue::from(instance_part));
        log.insert("region_id", LogValue::from(region_id_vec[i]));
        log.insert("topsql_cpu_time_ms", LogValue::from(cpu_time_vec[i]));
        log.insert("topsql_read_keys", LogValue::from(read_keys_vec[i]));
        log.insert("topsql_write_keys", LogValue::from(0));
        log.insert(
            "topsql_network_in_bytes",
            LogValue::from(network_in_vec[i]),
        );
        log.insert(
            "topsql_network_out_bytes",
            LogValue::from(network_out_vec[i]),
        );
        log.insert(
            "topsql_logical_read_bytes",
            LogValue::from(logical_read_vec[i]),
        );
        log.insert(
            "topsql_logical_write_bytes",
            LogValue::from(logical_write_vec[i]),
        );
        events.push(event);
    }
    events
}

pub struct Controller {
    shutdown_notifier: ShutdownNotifier,
    shutdown_subscriber: ShutdownSubscriber,
    top_n: usize,
    downsampling_interval: u32,
    tidb_number: usize,
    tikv_number: usize,
    extra_column_number: u32,
    instance_part_number: usize,
    out: SourceSender,
}

impl Controller {
    pub async fn new(
        top_n: usize,
        downsampling_interval: u32,
        tidb_number: usize,
        tikv_number: usize,
        extra_column_number: u32,
        instance_part_number: usize,
        out: SourceSender,
    ) -> vector::Result<Self> {
        let (shutdown_notifier, shutdown_subscriber) = pair();
        Ok(Self {
            shutdown_notifier,
            shutdown_subscriber,
            top_n,
            downsampling_interval,
            tidb_number,
            tikv_number,
            extra_column_number,
            instance_part_number,
            out,
        })
    }

    pub async fn run(mut self, mut shutdown: ShutdownSignal) {
        tokio::select! {
            _ = self.run_loop() => {},
            _ = &mut shutdown => {},
        }

        info!("TopSQL PubSub Controller is shutting down.");
        self.shutdown_all_components().await;
    }

    async fn run_loop(&mut self) {
        let mut batch = vec![];
        let (mut tidb_events, tidb_instance_partition_vec, tikv_instance_partition_vec) = create_event_for_instance_partition(chrono::Utc::now().timestamp(), self.tidb_number, self.tikv_number, self.instance_part_number);
        batch.append(&mut tidb_events);
        if self.out.send_batch(batch).await.is_err() {
            info!(message = "Downstream is closed, stopping TopSQL source.");
            return;
        }
        let sql_random_vec = generate_random_string(100000, 10);
        let mut tick_stream = IntervalStream::new(time::interval(Duration::from_secs(1)));
        let mut worker_stream = IntervalStream::new(time::interval(Duration::from_secs(60)));
        let mut instance_stream = IntervalStream::new(time::interval(Duration::from_secs(30)));        
        let mut trigger_counter : u64 = 0;
        let mut instance_events_counter : u64 = 0;
        loop {
            tokio::select! {
                _ = worker_stream.next() => {
                    let timestamp = chrono::Utc::now().timestamp();
                    let sql_digest_vec = generate_random_digest();
                    let int_vec_1 = generate_random_int();
                    let int_vec_2 = generate_random_int();
                    let int_vec_3 = generate_random_int();
                    let bigint_vec_1 = generate_random_bigint();
                    let bigint_vec_2 = generate_random_bigint();
                    let bigint_vec_3 = generate_random_bigint();
                    let bigint_vec_4 = generate_random_bigint();                    
                    for index in 0..self.tidb_number {
                        let mut batch = vec![];
                        let (mut sql_events, mut plan_events) = create_event_for_tidb_sql_plan_meta(&sql_digest_vec, &sql_digest_vec, &sql_random_vec, index%20);
                        batch.append(sql_events.as_mut());
                        if self.out.send_batch(batch).await.is_err() {
                            info!(message = "Downstream is closed, stopping TopSQL source.");
                            break;
                        }
                        let mut batch = vec![];
                        batch.append(plan_events.as_mut());
                        if self.out.send_batch(batch).await.is_err() {
                            info!(message = "Downstream is closed, stopping TopSQL source.");
                            break;
                        }
                    }
                    let mut loop_count = 1;
                    if self.downsampling_interval != 0 {
                        loop_count = 60 / self.downsampling_interval;
                    }
                    for _ in 0..loop_count {
                        for index in 0..self.tidb_number {
                            let mut batch = vec![];
                            let mut tidb_events = create_event_for_tidb_sql(index, timestamp, &sql_digest_vec, &sql_digest_vec, &int_vec_1,
                                &bigint_vec_1, &bigint_vec_2, &bigint_vec_3, &bigint_vec_4, self.top_n, tidb_instance_partition_vec[index] as usize);
                            batch.append(tidb_events.as_mut());
                            if self.out.send_batch(batch).await.is_err() {
                                info!(message = "Downstream is closed, stopping TopSQL source.");
                                break;
                            }
                        }
                        for index in 0..self.tikv_number {
                            let mut batch = vec![];
                            batch.append(create_event_for_tikv_sql(index, timestamp, &sql_digest_vec, &sql_digest_vec, &int_vec_1,
                                &int_vec_2, &bigint_vec_1, &bigint_vec_2, &bigint_vec_3, &bigint_vec_4, self.top_n, tikv_instance_partition_vec[index] as usize).as_mut());
                            batch.append(create_event_for_tikv_region(index, timestamp, &int_vec_1,
                                &int_vec_2, &int_vec_3, &bigint_vec_1, &bigint_vec_2, &bigint_vec_3, &bigint_vec_4, self.top_n, tikv_instance_partition_vec[index] as usize).as_mut());
                            if self.out.send_batch(batch).await.is_err() {
                                info!(message = "Downstream is closed, stopping TopSQL source. {}",);
                                break;
                            }
                        }
                    }
                }
                _ = tick_stream.next() => tokio::time::sleep(Duration::from_millis(50)).await,
                _ = instance_stream.next() => {
                    trigger_counter += 1;
                    instance_events_counter += self.tidb_number as u64;
                    instance_events_counter += self.tikv_number as u64;
                    for index in 0..self.tidb_number {
                        let instance_event = create_event_for_tidb_instance(index);
                        if self.out.send_event(instance_event).await.is_err() {
                            info!(message = "Downstream is closed, stopping TopSQL source.");
                            break;
                        }
                    }
                    for index in 0..self.tikv_number {
                        let instance_event = create_event_for_tikv_instance(index);
                        if self.out.send_event(instance_event).await.is_err() {
                            info!(message = "Downstream is closed, stopping TopSQL source.");
                            break;
                        }
                    }
                    warn!(message = "Mocked TopSQL source sent {} times, sent {} data.", trigger_counter, instance_events_counter);
                }
            }
        };
    }

    async fn shutdown_all_components(mut self) {
        self.shutdown_notifier.shutdown();
        self.shutdown_notifier.wait_for_exit().await;
        info!(message = "All TopSQL sources have been shut down.");
    }
}
