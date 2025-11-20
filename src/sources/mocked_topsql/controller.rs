use crate::sources::mocked_topsql::shutdown::{pair, ShutdownNotifier, ShutdownSubscriber};
use futures::StreamExt;
use rand::distr::Alphanumeric;
use rand::Rng;
use std::time::Duration;
use tokio::time;
use tokio_stream::wrappers::IntervalStream;
use vector::shutdown::ShutdownSignal;
use vector::SourceSender;
use vector_lib::event::{Event, LogEvent, Value as LogValue};

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
    let arr1: [i32; 100] = rng.random();
    arr1.to_vec()
}

fn generate_random_bigint() -> Vec<i64> {
    let mut rng = rand::rng();
    let arr1: [i64; 100] = rng.random();
    arr1.to_vec()
}

fn generate_random_string(num_strings: i32, string_length: usize) -> Vec<String> {
    let random_strings: Vec<String> = (0..num_strings)
        .map(|_| {
            rand::rng() // 获取线程局部的随机数生成器
                .sample_iter(&Alphanumeric) // 从 Alphanumeric 分布中创建迭代器
                .take(string_length) // 取指定长度的字符
                .map(char::from) // 将 u8 转换为 char
                .collect() // 收集成 String
        })
        .collect(); // 收集成 Vec<String>
    random_strings
}
fn generate_random_digest() -> Vec<String> {
    generate_random_string(100, 64)
}
/// Create a Vector event from table data
fn create_event_for_tidb_sql(_index: usize, timestamp: String) -> (Vec<Event>, Vec<Event>) {
    let mut events = vec![];
    let mut tikv_events = vec![];
    let mut schema_info = serde_json::Map::new();
    schema_info.insert(
        "sql_digest".into(),
        serde_json::json!({
            "mysql_type": "text",
            "is_nullable": false
        }),
    );
    schema_info.insert(
        "plan_digest".into(),
        serde_json::json!({
            "mysql_type": "text",
            "is_nullable": false
        }),
    );
    schema_info.insert(
        "sql".into(),
        serde_json::json!({
            "mysql_type": "text",
            "is_nullable": false
        }),
    );
    schema_info.insert(
        "plan".into(),
        serde_json::json!({
            "mysql_type": "text",
            "is_nullable": false
        }),
    );
    schema_info.insert(
        "cpu_time_ms".into(),
        serde_json::json!({
            "mysql_type": "int",
            "is_nullable": false
        }),
    );
    schema_info.insert(
        "stmt_exec_count".into(),
        serde_json::json!({
            "mysql_type": "bigint",
            "is_nullable": true
        }),
    );
    schema_info.insert(
        "stmt_duration_sum_ns".into(),
        serde_json::json!({
            "mysql_type": "bigint",
            "is_nullable": true
        }),
    );
    schema_info.insert(
        "stmt_duration_count".into(),
        serde_json::json!({
            "mysql_type": "bigint",
            "is_nullable": true
        }),
    );
    let mut tikv_schema_info = serde_json::Map::new();
    tikv_schema_info.insert(
        "sql_digest".into(),
        serde_json::json!({
            "mysql_type": "text",
            "is_nullable": false
        }),
    );
    tikv_schema_info.insert(
        "plan_digest".into(),
        serde_json::json!({
            "mysql_type": "text",
            "is_nullable": false
        }),
    );
    tikv_schema_info.insert(
        "sql".into(),
        serde_json::json!({
            "mysql_type": "text",
            "is_nullable": false
        }),
    );
    tikv_schema_info.insert(
        "plan".into(),
        serde_json::json!({
            "mysql_type": "text",
            "is_nullable": false
        }),
    );
    tikv_schema_info.insert(
        "stmt_exec_count".into(),
        serde_json::json!({
            "mysql_type": "bigint",
            "is_nullable": true
        }),
    );
    let sql_digest_vec = generate_random_digest();
    let plan_digest_vec = generate_random_digest();
    let sql_random_vec = generate_random_string(100, 10);
    let _plan_random_vec = generate_random_string(100, 10);
    let cpu_time_vec = generate_random_int();
    let stmt_exec_count_vec = generate_random_bigint();
    let stmt_duration_sum_vec = generate_random_bigint();
    let stmt_duration_count_vec = generate_random_bigint();
    for index in 0..100 {
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();

        // Add metadata with Vector prefix (ensure all fields have values)
        log.insert("_vector_table", "tidb_topsql");
        log.insert("_vector_source_table", "tidb_topsql");
        log.insert("_vector_source_schema", "test");
        log.insert("_vector_instance", format!("127.0.0.{}", index));
        log.insert("_vector_timestamp", timestamp.clone());
        log.insert(
            "_schema_metadata",
            serde_json::Value::Object(schema_info.clone()),
        );
        log.insert(
            "sql_digest",
            LogValue::from(sql_digest_vec[index].to_string()),
        );
        log.insert(
            "plan_digest",
            LogValue::from(plan_digest_vec[index].to_string()),
        );
        log.insert(
            "sql",
            LogValue::from(
                SQL_CONSTANT
                    .to_string()
                    .replace("tbl_test_001", sql_random_vec[index].as_str()),
            ),
        );
        log.insert(
            "plan",
            LogValue::from(
                PLAN_CONSTANT
                    .to_string()
                    .replace("tbl_test_001", sql_random_vec[index].as_str()),
            ),
        );
        log.insert("cpu_time_ms", LogValue::from(cpu_time_vec[index]));
        log.insert(
            "stmt_exec_count",
            LogValue::from(stmt_exec_count_vec[index]),
        );
        log.insert(
            "stmt_duration_sum_ns",
            LogValue::from(stmt_duration_sum_vec[index]),
        );
        log.insert(
            "stmt_duration_count",
            LogValue::from(stmt_duration_count_vec[index]),
        );
        events.push(event);

        let mut tikv_event = Event::Log(LogEvent::default());
        let tikv_log = tikv_event.as_mut_log();
        tikv_log.insert("_vector_table", "tikv_exec_count");
        tikv_log.insert("_vector_source_table", "tikv_exec_count");
        tikv_log.insert("_vector_source_schema", "test");
        tikv_log.insert("_vector_instance", format!("127.0.0.{}", index));
        tikv_log.insert("_vector_timestamp", timestamp.clone());
        tikv_log.insert(
            "_schema_metadata",
            serde_json::Value::Object(tikv_schema_info.clone()),
        );
        tikv_log.insert(
            "sql",
            LogValue::from(
                SQL_CONSTANT
                    .to_string()
                    .replace("tbl_test_001", sql_random_vec[index].as_str()),
            ),
        );
        tikv_log.insert(
            "plan",
            LogValue::from(
                PLAN_CONSTANT
                    .to_string()
                    .replace("tbl_test_001", sql_random_vec[index].as_str()),
            ),
        );
        tikv_log.insert("sql", LogValue::from(SQL_CONSTANT.to_string()));
        tikv_log.insert("plan", LogValue::from(PLAN_CONSTANT.to_string()));
        tikv_log.insert(
            "stmt_exec_count",
            LogValue::from(stmt_exec_count_vec[index]),
        );
        tikv_events.push(tikv_event);
    }
    (events, tikv_events)
}

/// Create a Vector event from table data
fn create_event_for_tikv_sql(_index: usize, timestamp: String) -> Vec<Event> {
    let mut events = vec![];
    let mut schema_info = serde_json::Map::new();
    schema_info.insert(
        "sql_digest".into(),
        serde_json::json!({
            "mysql_type": "text",
            "is_nullable": false
        }),
    );
    schema_info.insert(
        "plan_digest".into(),
        serde_json::json!({
            "mysql_type": "text",
            "is_nullable": false
        }),
    );
    schema_info.insert(
        "sql".into(),
        serde_json::json!({
            "mysql_type": "text",
            "is_nullable": false
        }),
    );
    schema_info.insert(
        "plan".into(),
        serde_json::json!({
            "mysql_type": "text",
            "is_nullable": false
        }),
    );
    schema_info.insert(
        "cpu_time_ms".into(),
        serde_json::json!({
            "mysql_type": "int",
            "is_nullable": false
        }),
    );
    schema_info.insert(
        "read_keys".into(),
        serde_json::json!({
            "mysql_type": "int",
            "is_nullable": false
        }),
    );
    schema_info.insert(
        "write_keys".into(),
        serde_json::json!({
            "mysql_type": "int",
            "is_nullable": false
        }),
    );
    schema_info.insert(
        "network_in_bytes".into(),
        serde_json::json!({
            "mysql_type": "bigint",
            "is_nullable": true
        }),
    );
    schema_info.insert(
        "network_out_bytes".into(),
        serde_json::json!({
            "mysql_type": "bigint",
            "is_nullable": true
        }),
    );
    schema_info.insert(
        "logical_io_read_bytes".into(),
        serde_json::json!({
            "mysql_type": "bigint",
            "is_nullable": true
        }),
    );
    schema_info.insert(
        "logical_io_write_bytes".into(),
        serde_json::json!({
            "mysql_type": "bigint",
            "is_nullable": true
        }),
    );
    let _sql_digest_vec = generate_random_digest();
    let _plan_digest_vec = generate_random_digest();
    let sql_random_vec = generate_random_string(100, 10);
    let _plan_random_vec = generate_random_string(100, 10);
    let cpu_time_vec = generate_random_int();
    let read_keys_vec = generate_random_int();
    let network_in_vec = generate_random_bigint();
    let network_out_vec = generate_random_bigint();
    let logical_read_vec = generate_random_bigint();
    let logical_write_vec = generate_random_bigint();
    for index in 0..100 {
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();

        // Add metadata with Vector prefix (ensure all fields have values)
        log.insert("_vector_table", "tikv_topsql");
        log.insert("_vector_source_table", "tikv_topsql");
        log.insert("_vector_source_schema", "test");
        log.insert("_vector_instance", format!("127.0.0.{}", index));
        log.insert("_vector_timestamp", timestamp.clone());
        log.insert(
            "_schema_metadata",
            serde_json::Value::Object(schema_info.clone()),
        );
        log.insert(
            "sql",
            LogValue::from(
                SQL_CONSTANT
                    .to_string()
                    .replace("tbl_test_001", sql_random_vec[index].as_str()),
            ),
        );
        log.insert(
            "plan",
            LogValue::from(
                PLAN_CONSTANT
                    .to_string()
                    .replace("tbl_test_001", sql_random_vec[index].as_str()),
            ),
        );
        log.insert("sql", LogValue::from(SQL_CONSTANT.to_string()));
        log.insert("plan", LogValue::from(PLAN_CONSTANT.to_string()));
        log.insert("cpu_time_ms", LogValue::from(cpu_time_vec[index]));
        log.insert("read_keys", LogValue::from(read_keys_vec[index]));
        log.insert("write_keys", LogValue::from(0));
        log.insert("network_in_bytes", LogValue::from(network_in_vec[index]));
        log.insert("network_out_bytes", LogValue::from(network_out_vec[index]));
        log.insert(
            "logical_io_read_bytes",
            LogValue::from(logical_read_vec[index]),
        );
        log.insert(
            "logical_io_write_bytes",
            LogValue::from(logical_write_vec[index]),
        );
        events.push(event);
    }
    events
}

/// Create a Vector event from table data
fn create_event_for_tikv_region(_index: usize, timestamp: String) -> Vec<Event> {
    let mut events = vec![];
    let mut schema_info = serde_json::Map::new();
    schema_info.insert(
        "region_id".into(),
        serde_json::json!({
            "mysql_type": "bigint",
            "is_nullable": false
        }),
    );
    schema_info.insert(
        "cpu_time_ms".into(),
        serde_json::json!({
            "mysql_type": "int",
            "is_nullable": false
        }),
    );
    schema_info.insert(
        "read_keys".into(),
        serde_json::json!({
            "mysql_type": "int",
            "is_nullable": false
        }),
    );
    schema_info.insert(
        "write_keys".into(),
        serde_json::json!({
            "mysql_type": "int",
            "is_nullable": false
        }),
    );
    schema_info.insert(
        "network_in_bytes".into(),
        serde_json::json!({
            "mysql_type": "bigint",
            "is_nullable": true
        }),
    );
    schema_info.insert(
        "network_out_bytes".into(),
        serde_json::json!({
            "mysql_type": "bigint",
            "is_nullable": true
        }),
    );
    schema_info.insert(
        "logical_io_read_bytes".into(),
        serde_json::json!({
            "mysql_type": "bigint",
            "is_nullable": true
        }),
    );
    schema_info.insert(
        "logical_io_write_bytes".into(),
        serde_json::json!({
            "mysql_type": "bigint",
            "is_nullable": true
        }),
    );
    let region_id_vec = generate_random_int();
    let cpu_time_vec = generate_random_int();
    let read_keys_vec = generate_random_int();
    let network_in_vec = generate_random_bigint();
    let network_out_vec = generate_random_bigint();
    let logical_read_vec = generate_random_bigint();
    let logical_write_vec = generate_random_bigint();
    for index in 0..100 {
        let mut event = Event::Log(LogEvent::default());
        let log = event.as_mut_log();

        // Add metadata with Vector prefix (ensure all fields have values)
        log.insert("_vector_table", "top_region");
        log.insert("_vector_source_table", "top_region");
        log.insert("_vector_source_schema", "test");
        log.insert("_vector_instance", format!("127.0.0.{}", index));
        log.insert("_vector_timestamp", timestamp.clone());
        log.insert(
            "_schema_metadata",
            serde_json::Value::Object(schema_info.clone()),
        );
        log.insert("region_id", LogValue::from(region_id_vec[index]));
        log.insert("cpu_time_ms", LogValue::from(cpu_time_vec[index]));
        log.insert("read_keys", LogValue::from(read_keys_vec[index]));
        log.insert("write_keys", LogValue::from(0));
        log.insert("network_in_bytes", LogValue::from(network_in_vec[index]));
        log.insert("network_out_bytes", LogValue::from(network_out_vec[index]));
        log.insert(
            "logical_io_read_bytes",
            LogValue::from(logical_read_vec[index]),
        );
        log.insert(
            "logical_io_write_bytes",
            LogValue::from(logical_write_vec[index]),
        );
        events.push(event);
    }
    events
}

pub struct Controller {
    shutdown_notifier: ShutdownNotifier,
    _shutdown_subscriber: ShutdownSubscriber,
    _top_n: usize,
    downsampling_interval: u32,
    tidb_number: usize,
    tikv_number: usize,
    _extra_column_number: u32,
    out: SourceSender,
}

impl Controller {
    pub async fn new(
        top_n: usize,
        downsampling_interval: u32,
        tidb_number: usize,
        tikv_number: usize,
        extra_column_number: u32,
        out: SourceSender,
    ) -> vector::Result<Self> {
        let (shutdown_notifier, shutdown_subscriber) = pair();
        Ok(Self {
            shutdown_notifier,
            _shutdown_subscriber: shutdown_subscriber,
            _top_n: top_n,
            downsampling_interval,
            tidb_number,
            tikv_number,
            _extra_column_number: extra_column_number,
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
        let mut tick_stream = IntervalStream::new(time::interval(Duration::from_secs(1)));
        let mut worker_stream = IntervalStream::new(time::interval(Duration::from_secs(60)));
        loop {
            tokio::select! {
                _ = worker_stream.next() => {
                    info!(message = "Mocked TopSQL source is generating data.");
                    let mut loop_count = 1;
                    if self.downsampling_interval != 0 {
                        loop_count = 60 / self.downsampling_interval;
                    }
                    let mut current_time = chrono::Utc::now();
                    for _ in 0..loop_count {
                        let current_time_str = current_time.to_rfc3339();
                        for index in 0..self.tikv_number {
                            let mut batch = vec![];
                            batch.append(create_event_for_tikv_sql(index, current_time_str.clone()).as_mut());
                            batch.append(create_event_for_tikv_region(index, current_time_str.clone()).as_mut());
                            if self.out.send_batch(batch).await.is_err() {
                                info!(message = "Downstream is closed, stopping TopSQL source. {}",);
                                break;
                            }
                        }
                        for index in 0..self.tidb_number {
                            let mut batch = vec![];
                            let (mut tidb_events, mut tikv_events) = create_event_for_tidb_sql(index, current_time_str.clone());
                            batch.append(tidb_events.as_mut());
                            if self.out.send_batch(batch).await.is_err() {
                                info!(message = "Downstream is closed, stopping TopSQL source.");
                                break;
                            }
                            let mut batch = vec![];
                            batch.append(tikv_events.as_mut());
                            if self.out.send_batch(batch).await.is_err() {
                                info!(message = "Downstream is closed, stopping TopSQL source.");
                                break;
                            }
                        }
                        if let Some(new_time) = current_time.checked_add_signed(chrono::Duration::seconds(self.downsampling_interval.into())) {
                            current_time = new_time;
                        }
                    }
                    info!(message = "Mocked TopSQL source sent data.");
                }
                _ = tick_stream.next() => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    }

    async fn shutdown_all_components(self) {
        self.shutdown_notifier.shutdown();
        self.shutdown_notifier.wait_for_exit().await;
        info!(message = "All TopSQL sources have been shut down.");
    }
}
