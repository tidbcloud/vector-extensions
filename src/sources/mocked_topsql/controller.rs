use chrono::Utc;
use vector_lib::event::{Event, LogEvent, Value as LogValue};
use futures::StreamExt;
use tokio::time;
use tokio_stream::wrappers::IntervalStream;
use vector::shutdown::ShutdownSignal;
use crate::sources::mocked_topsql::shutdown::{pair, ShutdownNotifier};
use vector::SourceSender;
use std::time::Duration;
use rand::Rng;
use rand::distr::Alphanumeric;
use crate::sources::topsql_v2::upstream::consts::{
    LABEL_DATE, LABEL_ENCODED_NORMALIZED_PLAN, LABEL_INSTANCE_KEY,
    LABEL_NORMALIZED_PLAN, LABEL_NORMALIZED_SQL, LABEL_PLAN_DIGEST,
    LABEL_SQL_DIGEST, LABEL_SOURCE_TABLE, LABEL_TIMESTAMPS,
    LABEL_DB_NAME, LABEL_TABLE_NAME, LABEL_TABLE_ID, LABEL_TAG_LABEL, LABEL_REGION_ID,
    METRIC_NAME_CPU_TIME_MS, METRIC_NAME_NETWORK_IN_BYTES, METRIC_NAME_NETWORK_OUT_BYTES,
    METRIC_NAME_STMT_DURATION_COUNT, METRIC_NAME_STMT_DURATION_SUM_NS, METRIC_NAME_STMT_EXEC_COUNT,
    METRIC_NAME_READ_KEYS, METRIC_NAME_WRITE_KEYS,
    METRIC_NAME_LOGICAL_READ_BYTES, METRIC_NAME_LOGICAL_WRITE_BYTES,
    SOURCE_TABLE_TIDB_TOPSQL, SOURCE_TABLE_TOPSQL_PLAN_META, SOURCE_TABLE_TOPSQL_SQL_META,
    SOURCE_TABLE_TIKV_TOPSQL, SOURCE_TABLE_TIKV_TOPREGION,
    KV_TAG_LABEL_UNKNOWN,
};

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
    let mut rng = rand::rng();
    let mut random_strings = Vec::with_capacity(num_strings as usize);
    for _ in 0..num_strings {
        let s: String = (0..string_length)
            .map(|_| {
                let byte = rng.sample(&Alphanumeric);
                char::from(byte)
            })
            .collect();
        random_strings.push(s);
    }
    random_strings
}
fn generate_random_digest() -> Vec<String> {
    generate_random_string(8000, 64)
}

/// Generate a batch of random indices within the given range
fn generate_random_indices(count: usize, max: usize) -> Vec<usize> {
    let mut rng = rand::rng();
    (0..count)
        .map(|_| (rng.random::<u64>() as usize) % max)
        .collect()
}

/// Create a Vector event from tidb sql meta
/// Generates events using random indices from the 800k digest pool
fn create_event_for_tidb_sql_plan_meta(sql_digest: &Vec<String>, plan_digest: &Vec<String>, random_str_vec: &Vec<String>) -> (Vec<Event>, Vec<Event>) {
    let digest_count = sql_digest.len();
    // Batch generate random indices
    let sql_indices = generate_random_indices(5000, digest_count);
    let plan_indices = generate_random_indices(5000, digest_count);
    
    // Get timestamp and date once for all events
    let now = Utc::now();
    let timestamp = now.timestamp();
    let date_str = now.format("%Y-%m-%d").to_string();
    
    let mut sql_events = vec![];
    for digest_idx in sql_indices {
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();

        log.insert(LABEL_SOURCE_TABLE, SOURCE_TABLE_TOPSQL_SQL_META);
        log.insert(LABEL_SQL_DIGEST, sql_digest[digest_idx].clone());
        log.insert(LABEL_NORMALIZED_SQL, SQL_CONSTANT.to_string().replace("tbl_test_001", random_str_vec[digest_idx % random_str_vec.len()].as_str()));
        log.insert(LABEL_TIMESTAMPS, LogValue::from(timestamp));
        log.insert(LABEL_DATE, LogValue::from(date_str.clone()));
        sql_events.push(event);
    }
    let mut plan_events = vec![];
    for digest_idx in plan_indices {
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();

        log.insert(LABEL_SOURCE_TABLE, SOURCE_TABLE_TOPSQL_PLAN_META);
        log.insert(LABEL_PLAN_DIGEST, plan_digest[digest_idx].clone());
        log.insert(LABEL_NORMALIZED_PLAN, PLAN_CONSTANT.to_string().replace("tbl_test_001", random_str_vec[digest_idx % random_str_vec.len()].as_str()));
        // For encoded_normalized_plan, we'll use the same as normalized_plan for mock data
        log.insert(LABEL_ENCODED_NORMALIZED_PLAN, plan_digest[digest_idx].clone());
        log.insert(LABEL_TIMESTAMPS, LogValue::from(timestamp));
        log.insert(LABEL_DATE, LogValue::from(date_str.clone()));
        plan_events.push(event);
    }    
    (sql_events, plan_events)
}
/// Create a Vector event from table data
/// Uses random indices to select digests from the 800k pool
fn create_event_for_tidb_sql(index: usize, timestamp: i64, sql_digest_vec: &Vec<String>, plan_digest_vec: &Vec<String>,
    cpu_time_vec: &Vec<i32>, network_in_vec: &Vec<i64>, network_out_vec: &Vec<i64>,
    stmt_exec_count_vec: &Vec<i64>, stmt_duration_sum_vec: &Vec<i64>,
    stmt_duration_count_vec: &Vec<i64>, top_n: usize) -> Vec<Event> {
    let mut events = vec![];
    let instance = format!("127.0.1.{}", index);
    let instance_key = format!("topsql_tidb_{}", instance);
    let mut date = String::new();
    let digest_count = sql_digest_vec.len();
    let event_count = top_n + top_n / 2;
    // Batch generate random indices
    let digest_indices = generate_random_indices(event_count, digest_count);
    for (i, &digest_idx) in digest_indices.iter().enumerate() {
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();

        log.insert(LABEL_SOURCE_TABLE, SOURCE_TABLE_TIDB_TOPSQL);
        log.insert(LABEL_TIMESTAMPS, LogValue::from(timestamp));
        if date.is_empty() {
            date = chrono::DateTime::from_timestamp(timestamp, 0)
                .map(|dt| dt.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| "1970-01-01".to_string());
        }
        log.insert(LABEL_DATE, LogValue::from(date.clone()));
        log.insert(LABEL_INSTANCE_KEY, instance_key.clone());
        log.insert(LABEL_SQL_DIGEST, sql_digest_vec[digest_idx].clone());
        log.insert(LABEL_PLAN_DIGEST, plan_digest_vec[digest_idx].clone());
        log.insert(METRIC_NAME_CPU_TIME_MS, LogValue::from(cpu_time_vec[i] as u32));
        log.insert(METRIC_NAME_STMT_EXEC_COUNT, LogValue::from(stmt_exec_count_vec[i]));
        log.insert(METRIC_NAME_STMT_DURATION_SUM_NS, LogValue::from(stmt_duration_sum_vec[i]));
        log.insert(METRIC_NAME_STMT_DURATION_COUNT, LogValue::from(stmt_duration_count_vec[i]));
        log.insert(METRIC_NAME_NETWORK_IN_BYTES, LogValue::from(network_in_vec[i]));
        log.insert(METRIC_NAME_NETWORK_OUT_BYTES, LogValue::from(network_out_vec[i]));
        events.push(event);
    }
    events
}

/// Create a Vector event from table data
/// Uses random indices to select digests from the 800k pool
fn create_event_for_tikv_sql(
    index: usize, timestamp: i64, sql_digest_vec: &Vec<String>, plan_digest_vec: &Vec<String>,
    cpu_time_vec: &Vec<i32>, read_keys_vec: &Vec<i32>,
    network_in_vec: &Vec<i64>, network_out_vec: &Vec<i64>,
    logical_read_vec: &Vec<i64>, logical_write_vec: &Vec<i64>,
    db_name_vec: &Vec<String>, table_name_vec: &Vec<String>, table_id_vec: &Vec<i64>,
    top_n: usize) -> Vec<Event> {
    let mut events = vec![];
    let instance = format!("127.0.0.{}", index);
    let instance_key = format!("topsql_tikv_{}", instance);
    let mut date = String::new();
    let digest_count = sql_digest_vec.len();
    let event_count = top_n + top_n;
    // Batch generate random indices
    let digest_indices = generate_random_indices(event_count, digest_count);
    
    for (i, &digest_idx) in digest_indices.iter().enumerate() {
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();

        log.insert(LABEL_SOURCE_TABLE, SOURCE_TABLE_TIKV_TOPSQL);
        log.insert(LABEL_TIMESTAMPS, LogValue::from(timestamp));
        if date.is_empty() {
            date = chrono::DateTime::from_timestamp(timestamp, 0)
                .map(|dt| dt.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| "1970-01-01".to_string());
        }
        log.insert(LABEL_DATE, LogValue::from(date.clone()));
        log.insert(LABEL_INSTANCE_KEY, instance_key.clone());
        log.insert(LABEL_SQL_DIGEST, sql_digest_vec[digest_idx].clone());
        log.insert(LABEL_PLAN_DIGEST, plan_digest_vec[digest_idx].clone());
        log.insert(LABEL_TAG_LABEL, KV_TAG_LABEL_UNKNOWN);
        log.insert(LABEL_DB_NAME, db_name_vec[i+index].clone());
        log.insert(LABEL_TABLE_NAME, table_name_vec[i+index].clone());
        log.insert(LABEL_TABLE_ID, table_id_vec[i+index].to_string());
        log.insert(METRIC_NAME_CPU_TIME_MS, LogValue::from(cpu_time_vec[i] as u32));
        log.insert(METRIC_NAME_READ_KEYS, LogValue::from(read_keys_vec[i] as u32));
        log.insert(METRIC_NAME_WRITE_KEYS, LogValue::from(0u32));
        log.insert(METRIC_NAME_NETWORK_IN_BYTES, LogValue::from(network_in_vec[i]));
        log.insert(METRIC_NAME_NETWORK_OUT_BYTES, LogValue::from(network_out_vec[i]));
        log.insert(METRIC_NAME_LOGICAL_READ_BYTES, LogValue::from(logical_read_vec[i]));
        log.insert(METRIC_NAME_LOGICAL_WRITE_BYTES, LogValue::from(logical_write_vec[i]));
        events.push(event);
    }    
    events
}

/// Create a Vector event from table data
fn create_event_for_tikv_region(
    index: usize, timestamp: i64, region_id_vec: &Vec<i32>,
    cpu_time_vec: &Vec<i32>, read_keys_vec: &Vec<i32>,
    network_in_vec: &Vec<i64>, network_out_vec: &Vec<i64>,
    logical_read_vec: &Vec<i64>, logical_write_vec: &Vec<i64>, top_n: usize) -> Vec<Event> {
    let mut events = vec![];
    let instance = format!("127.0.0.{}", index);
    let instance_key = format!("topsql_tikv_{}", instance);
    let mut date = String::new();
    
    for i in 0..(top_n + top_n) {
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();

        log.insert(LABEL_SOURCE_TABLE, SOURCE_TABLE_TIKV_TOPREGION);
        log.insert(LABEL_TIMESTAMPS, LogValue::from(timestamp));
        if date.is_empty() {
            date = chrono::DateTime::from_timestamp(timestamp, 0)
                .map(|dt| dt.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| "1970-01-01".to_string());
        }
        log.insert(LABEL_DATE, LogValue::from(date.clone()));
        log.insert(LABEL_INSTANCE_KEY, instance_key.clone());
        log.insert(LABEL_REGION_ID, region_id_vec[i].to_string());
        log.insert(METRIC_NAME_CPU_TIME_MS, LogValue::from(cpu_time_vec[i] as u32));
        log.insert(METRIC_NAME_READ_KEYS, LogValue::from(read_keys_vec[i] as u32));
        log.insert(METRIC_NAME_WRITE_KEYS, LogValue::from(0u32));
        log.insert(METRIC_NAME_NETWORK_IN_BYTES, LogValue::from(network_in_vec[i]));
        log.insert(METRIC_NAME_NETWORK_OUT_BYTES, LogValue::from(network_out_vec[i]));
        log.insert(METRIC_NAME_LOGICAL_READ_BYTES, LogValue::from(logical_read_vec[i]));
        log.insert(METRIC_NAME_LOGICAL_WRITE_BYTES, LogValue::from(logical_write_vec[i]));
        events.push(event);
    }
    events
}

pub struct Controller {
    shutdown_notifier: ShutdownNotifier,
    top_n: usize,
    downsampling_interval: u32,
    tidb_number: usize,
    tikv_number: usize,
    out: SourceSender,
}

impl Controller {
    pub async fn new(
        top_n: usize,
        downsampling_interval: u32,
        tidb_number: usize,
        tikv_number: usize,
        out: SourceSender,
    ) -> vector::Result<Self> {
        let (shutdown_notifier, _shutdown_subscriber) = pair();
        Ok(Self {
            shutdown_notifier,
            top_n,
            downsampling_interval,
            tidb_number,
            tikv_number,
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
        // Generate 800k digests once and reuse throughout the loop
        let sql_digest_vec = generate_random_digest();
        let plan_digest_vec = generate_random_digest();
        let sql_random_vec = generate_random_string(100000, 10);
        let mut tick_stream = IntervalStream::new(time::interval(Duration::from_secs(1)));
        let mut worker_stream = IntervalStream::new(time::interval(Duration::from_secs(60)));
        loop {
            tokio::select! {
                _ = worker_stream.next() => {
                    let timestamp = chrono::Utc::now().timestamp();
                    let int_vec_1 = generate_random_int();
                    let int_vec_2 = generate_random_int();
                    let int_vec_3 = generate_random_int();
                    let bigint_vec_1 = generate_random_bigint();
                    let bigint_vec_2 = generate_random_bigint();
                    let bigint_vec_3 = generate_random_bigint();
                    let bigint_vec_4 = generate_random_bigint();
                    let bigint_vec_5 = generate_random_bigint();
                    // Generate random database names, table names, and table IDs
                    let db_name_vec = generate_random_string(100000, 16);
                    let table_name_vec = generate_random_string(100000, 16);
                    let table_id_vec = generate_random_bigint();                    
                    for _ in 0..self.tidb_number {
                        let mut batch = vec![];
                        let (mut sql_events, mut plan_events) = create_event_for_tidb_sql_plan_meta(&sql_digest_vec, &plan_digest_vec, &sql_random_vec);
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
                                &bigint_vec_1, &bigint_vec_2, &bigint_vec_3, &bigint_vec_4, &bigint_vec_5, self.top_n);
                            batch.append(&mut tidb_events);
                            if self.out.send_batch(batch).await.is_err() {
                                info!(message = "Downstream is closed, stopping TopSQL source.");
                                break;
                            }
                        }
                        for index in 0..self.tikv_number {
                            let mut batch = vec![];
                            batch.append(create_event_for_tikv_sql(index, timestamp, &sql_digest_vec, &sql_digest_vec, &int_vec_1,
                                &int_vec_2, &bigint_vec_1, &bigint_vec_2, &bigint_vec_3, &bigint_vec_4,
                                &db_name_vec, &table_name_vec, &table_id_vec, self.top_n).as_mut());
                            batch.append(create_event_for_tikv_region(index, timestamp, &int_vec_1,
                                &int_vec_2, &int_vec_3, &bigint_vec_1, &bigint_vec_2, &bigint_vec_3, &bigint_vec_4, self.top_n).as_mut());
                            if self.out.send_batch(batch).await.is_err() {
                                info!(message = "Downstream is closed, stopping TopSQL source. {}",);
                                break;
                            }
                        }
                    }
                }
                _ = tick_stream.next() => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        };
    }

    async fn shutdown_all_components(self) {
        self.shutdown_notifier.shutdown();
        self.shutdown_notifier.wait_for_exit().await;
        info!(message = "All TopSQL sources have been shut down.");
    }
}
