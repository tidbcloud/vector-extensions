use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use regex::Regex;
use chrono::{DateTime, Utc};
use metrics::counter;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::sleep;
use tracing::{error, info};
use vector::shutdown::ShutdownSignal;
use vector::SourceSender;
use bytes::Bytes;
use vector_lib::event::{Event, LogEvent, Value as LogValue};

use crate::sources::file_list::checkpoint::Checkpoint;
use crate::sources::file_list::file_lister::{FileLister, FileMetadata};
use crate::sources::file_list::line_parser;
use crate::sources::file_list::path_resolver::ListRequest;
use crate::sources::file_list::EmitPerLineMode;
use tokio::sync::Mutex;

/// Build one LogEvent from a line (for emit_per_line streaming).
fn build_line_event(
    line: &str,
    file: &crate::sources::file_list::file_lister::FileMetadata,
    partition: Option<&(String, String)>,
    custom_line_regexes: Option<&[Regex]>,
    emit_metadata: bool,
) -> Event {
    let parsed = if let Some(regexes) = custom_line_regexes {
        line_parser::parse_line_with_regexes(line, regexes).unwrap_or_else(|| {
            let mut raw = std::collections::BTreeMap::new();
            raw.insert("message".to_string(), line.to_string());
            raw.insert("line_type".to_string(), line_parser::LINE_TYPE_RAW.to_string());
            raw
        })
    } else {
        let (_, fields) = line_parser::parse_line(line);
        fields
    };
    let mut log_event = LogEvent::default();
    log_event.insert("file_path", LogValue::Bytes(file.path.clone().into()));
    log_event.insert("data_type", LogValue::Bytes(Bytes::from_static(b"file")));
    if let Some((hour, comp)) = partition {
        log_event.insert("hour_partition", LogValue::Bytes(hour.clone().into()));
        log_event.insert("component", LogValue::Bytes(comp.clone().into()));
    }
    for (k, v) in &parsed {
        log_event.insert(k.as_str(), LogValue::Bytes(v.clone().into()));
    }
    if emit_metadata {
        log_event.insert("file_size", LogValue::Integer(file.size as i64));
        log_event.insert(
            "last_modified",
            LogValue::Bytes(file.last_modified.to_rfc3339().into()),
        );
        log_event.insert("bucket", LogValue::Bytes(file.bucket.clone().into()));
        log_event.insert("full_path", LogValue::Bytes(file.full_path.clone().into()));
    }
    log_event.insert(
        "@timestamp",
        LogValue::Bytes(Utc::now().to_rfc3339().into()),
    );
    Event::Log(log_event)
}

/// Parse raw_logs prefix "diagnosis/data/.../merged-logs/{YYYYMMDDHH}/{component}/" to (hour_partition, component).
fn parse_raw_logs_prefix(prefix: &str) -> Option<(String, String)> {
    let prefix = prefix.trim_end_matches('/');
    let parts: Vec<&str> = prefix.split('/').collect();
    // .../merged-logs/2026020411/loki => need merged-logs, then 10-digit, then component
    let merged_pos = parts.iter().position(|p| *p == "merged-logs")?;
    let hour = parts.get(merged_pos + 1).filter(|s| s.len() == 10 && s.chars().all(|c| c.is_ascii_digit()))?;
    let component = parts.get(merged_pos + 2)?;
    Some((hour.to_string(), component.to_string()))
}

pub struct Controller {
    file_lister: Arc<FileLister>,
    list_requests: Option<Vec<ListRequest>>,
    poll_interval: Option<Duration>,
    emit_metadata: bool,
    emit_content: bool,
    emit_per_line: EmitPerLineMode,
    stream_file_above_bytes: usize,
    custom_line_regexes: Option<Vec<Regex>>,
    decompress_gzip: bool,
    max_content_buffer_bytes: usize,
    stream_concurrency: usize,
    flush_after_each_file: bool,
    /// Checkpoint: completed prefix/unit keys so restart skips them (OOM recovery).
    checkpoint_path: Option<PathBuf>,
    checkpoint: Option<Arc<Mutex<Checkpoint>>>,
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
    /// True if this file should be read by streaming (per-line); false = whole file in one event.
    fn use_stream_for_file(&self, file: &FileMetadata) -> bool {
        match self.emit_per_line {
            EmitPerLineMode::Off => false,
            EmitPerLineMode::On => true,
            EmitPerLineMode::Auto => file.size > self.stream_file_above_bytes as u64,
        }
    }

    /// If checkpoint is enabled and this key is already completed, return true (caller should skip).
    async fn should_skip_checkpoint(&self, key: &str) -> bool {
        if let Some(ref cp) = self.checkpoint {
            if cp.lock().await.is_completed(key) {
                info!(key = %key, "file_list: skipping completed unit (checkpoint)");
                return true;
            }
        }
        false
    }

    /// Record key as completed and persist checkpoint (for OOM/restart recovery).
    async fn save_checkpoint_completed(&self, key: String) {
        if let (Some(ref path), Some(ref cp)) = (&self.checkpoint_path, &self.checkpoint) {
            let mut c = cp.lock().await;
            c.add_completed(key);
            if let Err(e) = c.save(path) {
                error!("file_list: failed to save checkpoint: {}", e);
            }
        }
    }
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
        emit_per_line: EmitPerLineMode,
        stream_file_above_bytes: usize,
        custom_line_regexes: Option<Vec<Regex>>,
        decompress_gzip: bool,
        max_content_buffer_bytes: usize,
        stream_concurrency: usize,
        flush_after_each_file: bool,
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
            emit_per_line,
            stream_file_above_bytes,
            custom_line_regexes,
            decompress_gzip,
            max_content_buffer_bytes,
            stream_concurrency,
            flush_after_each_file,
            checkpoint_path: None,
            checkpoint: None,
            out,
            shutdown,
            time_range_start: None,
            time_range_end: None,
            max_keys: 0,
        })
    }

    /// New: resolve by data types (cluster_id + types + time); list_requests from path_resolver.
    /// When checkpoint_path and checkpoint are Some, completed units are recorded for OOM/restart recovery.
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
        emit_per_line: EmitPerLineMode,
        stream_file_above_bytes: usize,
        custom_line_regexes: Option<Vec<Regex>>,
        decompress_gzip: bool,
        max_content_buffer_bytes: usize,
        stream_concurrency: usize,
        flush_after_each_file: bool,
        checkpoint_path: PathBuf,
        checkpoint: Arc<Mutex<Checkpoint>>,
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
            emit_per_line,
            stream_file_above_bytes,
            custom_line_regexes,
            decompress_gzip,
            max_content_buffer_bytes,
            stream_concurrency,
            flush_after_each_file,
            checkpoint_path: Some(checkpoint_path),
            checkpoint: Some(checkpoint),
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
            if let Err(e) = self.collect_events_by_requests().await {
                error!("Error listing: {}", e);
                if let (Some(ref path), Some(ref cp)) = (&self.checkpoint_path, &self.checkpoint) {
                    let mut c = cp.lock().await;
                    c.mark_error();
                    let _ = c.save(path);
                }
                if self.poll_interval.is_none() {
                    break;
                }
                sleep(self.poll_interval.unwrap_or_default()).await;
                continue;
            }
            if self.poll_interval.is_none() {
                info!("Oneshot mode (poll_interval_secs=0): file_list sync completed, exiting process");
                std::process::exit(0);
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
                if self.poll_interval.is_none() {
                    info!("Oneshot mode (poll_interval_secs=0): file_list sync completed, exiting process");
                    std::process::exit(0);
                }
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

    /// Collect events by processing each list request and send each batch to the sink immediately.
    /// This ensures all components (e.g. loki, operator, o11ydiagnosis-deltalake) get flushed to the
    /// sink incrementally, avoiding only the first component being written if the process is killed.
    async fn collect_events_by_requests(&mut self) -> vector::Result<()> {
        let requests = self
            .list_requests
            .as_ref()
            .ok_or("list_requests is None")?;

        for req in requests {
            let mut batch = Vec::new();
            let mut batch_bytes = 0usize;
            match req {
                ListRequest::FileList(f) => {
                    let key = f.prefix.clone();
                    if self.should_skip_checkpoint(&key).await {
                        continue;
                    }
                    let files = self
                        .file_lister
                        .list_files_at(&f.prefix, f.pattern.as_deref(), f.skip_time_filter)
                        .await?;
                    let partition = parse_raw_logs_prefix(&f.prefix);
                    if self.emit_content && self.emit_per_line == EmitPerLineMode::On {
                        info!(prefix = %f.prefix, file_count = files.len(), "processing files (streaming)");
                    }
                    if self.emit_content && self.emit_per_line == EmitPerLineMode::On && self.stream_concurrency > 1 {
                        // Parallel: one channel + batching task, N file tasks limited by semaphore.
                        let (tx, mut rx) = mpsc::channel::<(Option<Event>, usize)>(2048);
                        let mut out = self.out.clone();
                        let max_buf = self.max_content_buffer_bytes;
                        let flush_after_file = self.flush_after_each_file;
                        let batch_task = tokio::spawn(async move {
                            let mut batch = Vec::new();
                            let mut batch_bytes = 0usize;
                            while let Some((opt_ev, size)) = rx.recv().await {
                                if let Some(ev) = opt_ev {
                                    batch.push(ev);
                                    batch_bytes += size;
                                    if max_buf > 0 && batch_bytes >= max_buf {
                                        let to_send = std::mem::take(&mut batch);
                                        let n_ev = to_send.len();
                                        let n_bytes = batch_bytes;
                                        batch_bytes = 0;
                                        info!(events = n_ev, content_bytes = n_bytes, "file_list: flush batch reason=buffer_full (parallel), buffer cleared");
                                        let _ = out.send_batch(to_send).await;
                                    }
                                } else {
                                    if (size == 1 || (size == 0 && flush_after_file)) && !batch.is_empty() {
                                        let to_send = std::mem::take(&mut batch);
                                        let n_ev = to_send.len();
                                        let n_bytes = batch_bytes;
                                        batch_bytes = 0;
                                        info!(events = n_ev, content_bytes = n_bytes, "file_list: flush batch reason={} (parallel), buffer cleared", if size == 1 { "after_chunk" } else { "after_file" });
                                        let _ = out.send_batch(to_send).await;
                                    }
                                }
                            }
                            if !batch.is_empty() {
                                let n_ev = batch.len();
                                let n_bytes = batch_bytes;
                                info!(events = n_ev, content_bytes = n_bytes, "file_list: flush batch reason=end_remaining (parallel), buffer cleared");
                                let _ = out.send_batch(batch).await;
                            }
                        });
                        let sem = Arc::new(Semaphore::new(self.stream_concurrency));
                        let lister = self.file_lister.clone();
                        let decompress_gzip = self.decompress_gzip;
                        let partition_par = partition.clone();
                        let custom_regexes_par = self.custom_line_regexes.clone();
                        let emit_metadata_par = self.emit_metadata;
                        let mut handles = Vec::with_capacity(files.len());
                        for file in &files {
                            let file = file.clone();
                            let tx = tx.clone();
                            let permit = sem.clone().acquire_owned().await.map_err(|e| format!("semaphore: {}", e))?;
                            let lister = lister.clone();
                            let partition_c = partition_par.clone();
                            let custom_regexes_c = custom_regexes_par.clone();
                            handles.push(tokio::spawn(async move {
                                let _permit = permit;
                                        lister
                                            .stream_file_lines_send(
                                                &file.path,
                                                file.size,
                                                decompress_gzip,
                                                max_buf,
                                                &tx,
                                                |line| {
                                                    build_line_event(
                                                        &line,
                                                        &file,
                                                        partition_c.as_ref(),
                                                        custom_regexes_c.as_deref(),
                                                        emit_metadata_par,
                                                    )
                                                },
                                            )
                                            .await
                            }));
                        }
                        drop(tx);
                        for h in join_all(handles).await {
                            match h {
                                Ok(Ok(c)) => counter!("file_list_files_found_total").increment(c),
                                Ok(Err(e)) => error!("file_list: stream_file_lines_send error: {}", e),
                                Err(e) => error!("file_list: task join error: {}", e),
                            }
                        }
                        batch_task.await.map_err(|e| format!("batch task: {}", e))?;
                    } else {
                        for file in &files {
                            if self.emit_content && self.use_stream_for_file(file) {
                                let file = file.clone();
                                let partition_clone = partition.clone();
                                let custom_regexes = self.custom_line_regexes.as_deref();
                                let emit_metadata = self.emit_metadata;
                                match self
                                    .file_lister
                                    .stream_file_lines(
                                        &file.path,
                                        file.size,
                                        self.decompress_gzip,
                                        &mut batch,
                                        &mut batch_bytes,
                                        self.max_content_buffer_bytes,
                                        &mut self.out,
                                        |line| {
                                            build_line_event(
                                                &line,
                                                &file,
                                                partition_clone.as_ref(),
                                                custom_regexes,
                                                emit_metadata,
                                            )
                                        },
                                    )
                                    .await
                                {
                                    Ok(line_count) => {
                                        counter!("file_list_files_found_total").increment(line_count);
                                    }
                                    Err(e) => {
                                        error!("file_list: failed to stream {}: {}", file.path, e);
                                    }
                                }
                                if self.flush_after_each_file && !batch.is_empty() {
                                    let n_ev = batch.len();
                                    let n_bytes = batch_bytes;
                                    let to_send = std::mem::take(&mut batch);
                                    batch_bytes = 0;
                                    info!(path = %file.path, events = n_ev, content_bytes = n_bytes, "file_list: flush batch reason=after_file, buffer cleared");
                                    self.out.send_batch(to_send).await?;
                                } else if !batch.is_empty() {
                                    info!(path = %file.path, events = batch.len(), content_bytes = batch_bytes, "file_list: after file (no flush), buffer state");
                                }
                            } else {
                            let mut log_event = LogEvent::default();
                            log_event.insert("file_path", LogValue::Bytes(file.path.clone().into()));
                            log_event.insert("data_type", LogValue::Bytes(Bytes::from_static(b"file")));
                            if let Some((ref hour, ref comp)) = partition {
                                log_event.insert("hour_partition", LogValue::Bytes(hour.clone().into()));
                                log_event.insert("component", LogValue::Bytes(comp.clone().into()));
                            }
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
                                info!(path = %file.path, file_size = file.size, "file_list: downloading file (whole-file)");
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
                            batch.push(Event::Log(log_event));
                            counter!("file_list_files_found_total").increment(1);
                            }
                        }
                        if !self.flush_after_each_file && !batch.is_empty() {
                            let n_ev = batch.len();
                            let n_bytes = batch_bytes;
                            let to_send = std::mem::take(&mut batch);
                            info!(events = n_ev, content_bytes = n_bytes, "file_list: flush batch reason=end_of_list, buffer cleared");
                            self.out.send_batch(to_send).await?;
                        }
                    }
                    self.save_checkpoint_completed(key).await;
                }
                ListRequest::DeltaTable(d) => {
                    let key = format!("delta:{}:{}", d.list_prefix, d.table_subdir);
                    if self.should_skip_checkpoint(&key).await {
                        continue;
                    }
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
                        batch.push(Event::Log(log_event));
                    }
                    counter!("file_list_files_found_total").increment(n as u64);
                    self.save_checkpoint_completed(key).await;
                }
                ListRequest::TopSql(t) => {
                    let key = format!("topsql:{}", t.list_prefix);
                    if self.should_skip_checkpoint(&key).await {
                        continue;
                    }
                    let paths = self
                        .file_lister
                        .list_topsql_table_paths(&t.list_prefix)
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
                        batch.push(Event::Log(log_event));
                    }
                    counter!("file_list_files_found_total").increment(n as u64);
                    self.save_checkpoint_completed(key).await;
                }
                ListRequest::RawLogsDiscover(d) => {
                    for hour_prefix in &d.hour_prefixes {
                        let hour_partition = hour_prefix
                            .trim_end_matches('/')
                            .split('/')
                            .last()
                            .unwrap_or("unknown")
                            .to_string();
                        let components = self.file_lister.list_subdir_names(hour_prefix).await?;
                        for comp in &components {
                            let prefix = format!("{}{}/", hour_prefix, comp);
                            if self.should_skip_checkpoint(&prefix).await {
                                continue;
                            }
                            let files = self
                                .file_lister
                                .list_files_at(&prefix, Some("*.log"), true)
                                .await?;
                            let partition_raw = (hour_partition.clone(), comp.clone());
                            if self.emit_content && self.emit_per_line == EmitPerLineMode::On {
                                info!(prefix = %prefix, file_count = files.len(), "processing files (streaming)");
                            }
                            if self.emit_content && self.emit_per_line == EmitPerLineMode::On && self.stream_concurrency > 1 {
                                let (tx, mut rx) = mpsc::channel::<(Option<Event>, usize)>(2048);
                                let mut out = self.out.clone();
                                let max_buf = self.max_content_buffer_bytes;
                                let flush_after_file = self.flush_after_each_file;
                                let batch_task = tokio::spawn(async move {
                                    let mut batch = Vec::new();
                                    let mut batch_bytes = 0usize;
                                    while let Some((opt_ev, size)) = rx.recv().await {
                                        if let Some(ev) = opt_ev {
                                            batch.push(ev);
                                            batch_bytes += size;
                                            if max_buf > 0 && batch_bytes >= max_buf {
                                                let to_send = std::mem::take(&mut batch);
                                                let n_ev = to_send.len();
                                                let n_bytes = batch_bytes;
                                                batch_bytes = 0;
                                                info!(events = n_ev, content_bytes = n_bytes, "file_list: flush batch reason=buffer_full (parallel RawLogs), buffer cleared");
                                                let _ = out.send_batch(to_send).await;
                                            }
                                        } else {
                                            if (size == 1 || (size == 0 && flush_after_file)) && !batch.is_empty() {
                                                let to_send = std::mem::take(&mut batch);
                                                let n_ev = to_send.len();
                                                let n_bytes = batch_bytes;
                                                batch_bytes = 0;
                                                info!(events = n_ev, content_bytes = n_bytes, "file_list: flush batch reason={} (parallel RawLogs), buffer cleared", if size == 1 { "after_chunk" } else { "after_file" });
                                                let _ = out.send_batch(to_send).await;
                                            }
                                        }
                                    }
                                    if !batch.is_empty() {
                                        let n_ev = batch.len();
                                        let n_bytes = batch_bytes;
                                        info!(events = n_ev, content_bytes = n_bytes, "file_list: flush batch reason=end_remaining (parallel RawLogs), buffer cleared");
                                        let _ = out.send_batch(batch).await;
                                    }
                                });
                                let sem = Arc::new(Semaphore::new(self.stream_concurrency));
                                let lister = self.file_lister.clone();
                                let decompress_gzip = self.decompress_gzip;
                                let custom_regexes_par = self.custom_line_regexes.clone();
                                let emit_metadata_par = self.emit_metadata;
                                let mut handles = Vec::with_capacity(files.len());
                                for file in &files {
                                    let file = file.clone();
                                    let tx = tx.clone();
                                    let permit = sem.clone().acquire_owned().await.map_err(|e| format!("semaphore: {}", e))?;
                                    let lister = lister.clone();
                                    let partition_c = partition_raw.clone();
                                    let custom_regexes_c = custom_regexes_par.clone();
                                    let max_buf_raw = self.max_content_buffer_bytes;
                                    handles.push(tokio::spawn(async move {
                                        let _permit = permit;
                                        lister
                                            .stream_file_lines_send(
                                                &file.path,
                                                file.size,
                                                decompress_gzip,
                                                max_buf_raw,
                                                &tx,
                                                |line| {
                                                    build_line_event(
                                                        &line,
                                                        &file,
                                                        Some(&partition_c),
                                                        custom_regexes_c.as_deref(),
                                                        emit_metadata_par,
                                                    )
                                                },
                                            )
                                            .await
                                    }));
                                }
                                drop(tx);
                                for h in join_all(handles).await {
                                    match h {
                                        Ok(Ok(c)) => counter!("file_list_files_found_total").increment(c),
                                        Ok(Err(e)) => error!("file_list: stream_file_lines_send error: {}", e),
                                        Err(e) => error!("file_list: task join error: {}", e),
                                    }
                                }
                                batch_task.await.map_err(|e| format!("batch task: {}", e))?;
                            } else {
                                for file in &files {
                                    if self.emit_content && self.use_stream_for_file(file) {
                                        let file = file.clone();
                                        let partition_raw = (hour_partition.clone(), comp.clone());
                                        let custom_regexes = self.custom_line_regexes.as_deref();
                                        let emit_metadata = self.emit_metadata;
                                        match self
                                            .file_lister
                                            .stream_file_lines(
                                                &file.path,
                                                file.size,
                                                self.decompress_gzip,
                                                &mut batch,
                                                &mut batch_bytes,
                                                self.max_content_buffer_bytes,
                                                &mut self.out,
                                                |line| {
                                                    build_line_event(
                                                        &line,
                                                        &file,
                                                        Some(&partition_raw),
                                                        custom_regexes,
                                                        emit_metadata,
                                                    )
                                                },
                                            )
                                            .await
                                        {
                                            Ok(line_count) => {
                                                counter!("file_list_files_found_total").increment(line_count);
                                            }
                                            Err(e) => {
                                                error!("file_list: failed to stream {}: {}", file.path, e);
                                            }
                                        }
                                        if self.flush_after_each_file && !batch.is_empty() {
                                            let n_ev = batch.len();
                                            let n_bytes = batch_bytes;
                                            let to_send = std::mem::take(&mut batch);
                                            batch_bytes = 0;
                                            info!(path = %file.path, events = n_ev, content_bytes = n_bytes, "file_list: flush batch reason=after_file (RawLogs), buffer cleared");
                                            self.out.send_batch(to_send).await?;
                                        } else if !batch.is_empty() {
                                            info!(path = %file.path, events = batch.len(), content_bytes = batch_bytes, "file_list: after file (no flush, RawLogs), buffer state");
                                        }
                                    } else {
                                    let mut log_event = LogEvent::default();
                                    log_event.insert("file_path", LogValue::Bytes(file.path.clone().into()));
                                    log_event.insert("data_type", LogValue::Bytes(Bytes::from_static(b"file")));
                                    log_event.insert("hour_partition", LogValue::Bytes(hour_partition.clone().into()));
                                    log_event.insert("component", LogValue::Bytes(comp.clone().into()));
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
                                        info!(path = %file.path, file_size = file.size, "file_list: downloading file (whole-file)");
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
                                    batch.push(Event::Log(log_event));
                                    counter!("file_list_files_found_total").increment(1);
                                    }
                                }
                            }
                            if !batch.is_empty() {
                                self.out.send_batch(std::mem::take(&mut batch)).await?;
                                batch_bytes = 0;
                            }
                            self.save_checkpoint_completed(prefix.clone()).await;
                        }
                    }
                }
            }
            if !batch.is_empty() {
                self.out.send_batch(batch).await?;
            }
        }

        Ok(())
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
            info!(path = %file.path, file_size = file.size, "file_list: downloading file (whole-file)");
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
