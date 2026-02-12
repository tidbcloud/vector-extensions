use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use metrics::counter;
use tokio::time::sleep;
use tracing::{error, info};
use vector::shutdown::ShutdownSignal;
use vector::SourceSender;
use bytes::Bytes;
use vector_lib::event::{Event, LogEvent, Value as LogValue};

use crate::sources::file_list::file_lister::{FileLister, FileMetadata};
use crate::sources::file_list::path_resolver::ListRequest;

pub struct Controller {
    file_lister: Arc<FileLister>,
    list_requests: Option<Vec<ListRequest>>,
    poll_interval: Option<Duration>,
    emit_metadata: bool,
    emit_content: bool,
    decompress_gzip: bool,
    out: SourceSender,
    shutdown: ShutdownSignal,
    #[allow(dead_code)]
    time_range_start: Option<DateTime<Utc>>,
    #[allow(dead_code)]
    time_range_end: Option<DateTime<Utc>>,
    #[allow(dead_code)]
    max_keys: usize,
}

impl Controller {
    /// Legacy: single prefix + pattern.
    pub fn new_legacy(
        endpoint: String,
        cloud_provider: String,
        region: Option<String>,
        prefix: String,
        pattern: Option<String>,
        time_range_start: Option<DateTime<Utc>>,
        time_range_end: Option<DateTime<Utc>>,
        max_keys: usize,
        poll_interval: Option<Duration>,
        emit_metadata: bool,
        emit_content: bool,
        decompress_gzip: bool,
        out: SourceSender,
        shutdown: ShutdownSignal,
    ) -> vector::Result<Self> {
        let file_lister = Arc::new(FileLister::new(
            endpoint,
            cloud_provider,
            region,
            prefix,
            pattern,
            time_range_start,
            time_range_end,
            max_keys,
        )?);
        Ok(Self {
            file_lister,
            list_requests: None,
            poll_interval,
            emit_metadata,
            emit_content,
            decompress_gzip,
            out,
            shutdown,
            time_range_start: None,
            time_range_end: None,
            max_keys: 0,
        })
    }

    /// New: resolve by data types (cluster_id + types + time); list_requests from path_resolver.
    pub fn new_with_requests(
        endpoint: String,
        cloud_provider: String,
        region: Option<String>,
        list_requests: Vec<ListRequest>,
        time_range_start: Option<DateTime<Utc>>,
        time_range_end: Option<DateTime<Utc>>,
        max_keys: usize,
        poll_interval: Option<Duration>,
        emit_metadata: bool,
        emit_content: bool,
        decompress_gzip: bool,
        out: SourceSender,
        shutdown: ShutdownSignal,
    ) -> vector::Result<Self> {
        let file_lister = Arc::new(FileLister::new(
            endpoint,
            cloud_provider,
            region,
            String::new(),
            None,
            time_range_start,
            time_range_end,
            max_keys,
        )?);
        Ok(Self {
            file_lister,
            list_requests: Some(list_requests),
            poll_interval,
            emit_metadata,
            emit_content,
            decompress_gzip,
            out,
            shutdown,
            time_range_start,
            time_range_end,
            max_keys,
        })
    }

    pub async fn run(mut self) -> Result<(), ()> {
        info!("FileList Controller starting (data types mode)...");

        loop {
            let events = match self.collect_events_by_requests().await {
                Ok(ev) => ev,
                Err(e) => {
                    error!("Error listing: {}", e);
                    if self.poll_interval.is_none() {
                        break;
                    }
                    sleep(self.poll_interval.unwrap_or_default()).await;
                    continue;
                }
            };
            if !events.is_empty() {
                if let Err(e) = self.out.send_batch(events).await {
                    error!("Failed to send events: {}", e);
                }
            }
            if self.poll_interval.is_none() {
                break;
            }
            let interval = self.poll_interval.unwrap();
            tokio::select! {
                _ = &mut self.shutdown => {
                    info!("Shutdown signal received");
                    break;
                }
                _ = sleep(interval) => {}
            }
        }
        info!("FileList Controller shutting down...");
        Ok(())
    }

    pub async fn run_legacy(mut self) -> Result<(), ()> {
        info!("FileList Controller starting (legacy prefix mode)...");
        loop {
            let (should_continue, events) = match self.collect_events_legacy().await {
                Ok(x) => x,
                Err(e) => {
                    error!("Error listing files: {}", e);
                    if self.poll_interval.is_none() {
                        break;
                    }
                    sleep(self.poll_interval.unwrap_or_default()).await;
                    continue;
                }
            };
            if !events.is_empty() {
                if let Err(e) = self.out.send_batch(events).await {
                    error!("Failed to send events: {}", e);
                }
            }
            if !should_continue {
                break;
            }
            if let Some(interval) = self.poll_interval {
                tokio::select! {
                    _ = &mut self.shutdown => {
                        info!("Shutdown signal received");
                        break;
                    }
                    _ = sleep(interval) => {}
                }
            } else {
                break;
            }
        }
        info!("FileList Controller shutting down...");
        Ok(())
    }

    async fn collect_events_legacy(&self) -> vector::Result<(bool, Vec<Event>)> {
        let files = self.file_lister.list_files().await?;
        if files.is_empty() {
            return Ok((self.poll_interval.is_some(), Vec::new()));
        }
        let events = if self.emit_content {
            self.emit_file_events_with_content(&files).await?
        } else {
            self.emit_file_events_to_vec(&files)?
        };
        counter!("file_list_files_found_total").increment(files.len() as u64);
        Ok((self.poll_interval.is_some(), events))
    }

    async fn collect_events_by_requests(&self) -> vector::Result<Vec<Event>> {
        let requests = self
            .list_requests
            .as_ref()
            .ok_or("list_requests is None")?;
        let mut all_events = Vec::new();

        for req in requests {
            match req {
                ListRequest::FileList(f) => {
                    let files = self
                        .file_lister
                        .list_files_at(&f.prefix, f.pattern.as_deref(), f.skip_time_filter)
                        .await?;
                    let n = files.len();
                    for file in &files {
                        let mut log_event = LogEvent::default();
                        log_event.insert("file_path", LogValue::Bytes(file.path.clone().into()));
                        log_event.insert("data_type", LogValue::Bytes(Bytes::from_static(b"file")));
                        if self.emit_metadata {
                            log_event.insert("file_size", LogValue::Integer(file.size as i64));
                            log_event.insert(
                                "last_modified",
                                LogValue::Bytes(file.last_modified.to_rfc3339().into()),
                            );
                            log_event.insert("bucket", LogValue::Bytes(file.bucket.clone().into()));
                            log_event.insert("full_path", LogValue::Bytes(file.full_path.clone().into()));
                        }
                        if self.emit_content {
                            match self.file_lister.get_file_bytes(&file.path, self.decompress_gzip).await {
                                Ok(content) => {
                                    let msg = String::from_utf8_lossy(&content).into_owned();
                                    log_event.insert("message", LogValue::Bytes(msg.into()));
                                }
                                Err(e) => {
                                    error!("file_list: failed to get content for {}: {}", file.path, e);
                                }
                            }
                        }
                        log_event.insert(
                            "@timestamp",
                            LogValue::Bytes(Utc::now().to_rfc3339().into()),
                        );
                        all_events.push(Event::Log(log_event));
                    }
                    counter!("file_list_files_found_total").increment(n as u64);
                }
                ListRequest::DeltaTable(d) => {
                    let paths = self
                        .file_lister
                        .list_delta_table_paths(&d.list_prefix, &d.table_subdir)
                        .await?;
                    let n = paths.len();
                    for path in &paths {
                        let mut log_event = LogEvent::default();
                        log_event.insert("file_path", LogValue::Bytes(path.clone().into()));
                        log_event.insert("data_type", LogValue::Bytes(Bytes::from_static(b"delta_table")));
                        log_event.insert(
                            "table_subdir",
                            LogValue::Bytes(d.table_subdir.clone().into()),
                        );
                        log_event.insert(
                            "@timestamp",
                            LogValue::Bytes(Utc::now().to_rfc3339().into()),
                        );
                        all_events.push(Event::Log(log_event));
                    }
                    counter!("file_list_files_found_total").increment(n as u64);
                }
                ListRequest::TopSql(t) => {
                    let paths = self
                        .file_lister
                        .list_topsql_instance_paths(&t.list_prefix)
                        .await?;
                    let n = paths.len();
                    for path in &paths {
                        let mut log_event = LogEvent::default();
                        log_event.insert("file_path", LogValue::Bytes(path.clone().into()));
                        log_event.insert("data_type", LogValue::Bytes(Bytes::from_static(b"delta_table")));
                        log_event.insert(
                            "table_subdir",
                            LogValue::Bytes(Bytes::from_static(b"topsql")),
                        );
                        log_event.insert(
                            "@timestamp",
                            LogValue::Bytes(Utc::now().to_rfc3339().into()),
                        );
                        all_events.push(Event::Log(log_event));
                    }
                    counter!("file_list_files_found_total").increment(n as u64);
                }
            }
        }

        Ok(all_events)
    }

    fn emit_file_events_to_vec(&self, files: &[FileMetadata]) -> vector::Result<Vec<Event>> {
        let mut events = Vec::new();
        for file in files {
            let mut log_event = LogEvent::default();
            if self.emit_metadata {
                log_event.insert("file_path", LogValue::Bytes(file.path.clone().into()));
                log_event.insert("file_size", LogValue::Integer(file.size as i64));
                log_event.insert(
                    "last_modified",
                    LogValue::Bytes(file.last_modified.to_rfc3339().into()),
                );
                log_event.insert("bucket", LogValue::Bytes(file.bucket.clone().into()));
                log_event.insert("full_path", LogValue::Bytes(file.full_path.clone().into()));
            } else {
                log_event.insert("file_path", LogValue::Bytes(file.path.clone().into()));
            }
            log_event.insert(
                "@timestamp",
                LogValue::Bytes(Utc::now().to_rfc3339().into()),
            );
            events.push(Event::Log(log_event));
        }
        Ok(events)
    }

    async fn emit_file_events_with_content(&self, files: &[FileMetadata]) -> vector::Result<Vec<Event>> {
        let mut events = Vec::new();
        for file in files {
            let mut log_event = LogEvent::default();
            log_event.insert("file_path", LogValue::Bytes(file.path.clone().into()));
            if self.emit_metadata {
                log_event.insert("file_size", LogValue::Integer(file.size as i64));
                log_event.insert(
                    "last_modified",
                    LogValue::Bytes(file.last_modified.to_rfc3339().into()),
                );
                log_event.insert("bucket", LogValue::Bytes(file.bucket.clone().into()));
                log_event.insert("full_path", LogValue::Bytes(file.full_path.clone().into()));
            }
            match self.file_lister.get_file_bytes(&file.path, self.decompress_gzip).await {
                Ok(content) => {
                    let msg = String::from_utf8_lossy(&content).into_owned();
                    log_event.insert("message", LogValue::Bytes(msg.into()));
                }
                Err(e) => {
                    error!("file_list: failed to get content for {}: {}", file.path, e);
                }
            }
            log_event.insert(
                "@timestamp",
                LogValue::Bytes(Utc::now().to_rfc3339().into()),
            );
            events.push(Event::Log(log_event));
        }
        Ok(events)
    }
}

// Controller doesn't need to implement Future directly
// Vector's Source trait handles the async execution
