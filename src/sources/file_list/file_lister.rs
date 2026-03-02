use std::collections::HashSet;
use std::io::{self, Read};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_compression::tokio::bufread::GzipDecoder;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use futures_util::stream::Stream;
use futures_util::{ready, StreamExt};
use object_store::{path::Path as ObjectStorePath, ObjectStore};
use regex::Regex;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::sync::mpsc;
use tokio_util::io::StreamReader;
use vector_lib::event::Event as VectorEvent;
use tracing::{error, info};
use url::Url;

use super::object_store_builder::build_object_store;

/// Coalesces small chunks from a stream into larger buffers (>= target bytes) so that
/// downstream readers (e.g. GzipDecoder) get fewer, larger reads and do fewer decompress cycles.
struct CoalesceStream<S> {
    inner: Pin<Box<S>>,
    target: usize,
    buf: BytesMut,
}

impl<S, E> Stream for CoalesceStream<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    type Item = Result<Bytes, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes, E>>> {
        let this = self.as_mut().get_mut();
        loop {
            if this.buf.len() >= this.target {
                let out = this.buf.split_to(this.target);
                return Poll::Ready(Some(Ok(Bytes::from(out))));
            }
            match ready!(Pin::new(&mut this.inner).poll_next(cx)) {
                Some(Ok(b)) => this.buf.extend_from_slice(&b),
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => {
                    if this.buf.is_empty() {
                        return Poll::Ready(None);
                    }
                    let out = this.buf.split();
                    return Poll::Ready(Some(Ok(Bytes::from(out))));
                }
            }
        }
    }
}

/// File metadata information
#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub path: String,
    pub size: u64,
    pub last_modified: DateTime<Utc>,
    pub bucket: String,
    pub full_path: String,
}

/// File lister for cloud storage
pub struct FileLister {
    object_store: Arc<dyn ObjectStore>,
    prefix: String,
    pattern: Option<Regex>,  // Compiled regex pattern
    time_range_start: Option<DateTime<Utc>>,
    time_range_end: Option<DateTime<Utc>>,
    max_keys: usize,
}

impl FileLister {
    pub fn new(
        endpoint: String,
        cloud_provider: String,
        region: Option<String>,
        prefix: String,
        pattern: Option<String>,
        time_range_start: Option<DateTime<Utc>>,
        time_range_end: Option<DateTime<Utc>>,
        max_keys: usize,
    ) -> vector::Result<Self> {
        info!(
            "Creating FileLister for endpoint: {}, provider: {}, prefix: {}",
            endpoint, cloud_provider, prefix
        );

        let object_store = build_object_store(&endpoint, &cloud_provider, region.as_deref())?;

        // Compile pattern to regex if provided
        let compiled_pattern = if let Some(ref pat) = pattern {
            Some(Self::compile_pattern(pat)?)
        } else {
            None
        };

        Ok(Self {
            object_store,
            prefix,
            pattern: compiled_pattern,
            time_range_start,
            time_range_end,
            max_keys,
        })
    }

    /// Compile pattern string to regex (public for use with list_files_at from path_resolver).
    pub fn compile_pattern(pattern: &str) -> vector::Result<Regex> {
        // Use a placeholder that won't be escaped, then substitute the real regex after escaping
        const PLACEHOLDER: &str = "__TEN_DIGITS_PLACEHOLDER__";
        let regex_str = pattern.replace("{YYYYMMDDHH}", PLACEHOLDER);

        // Replace * with .* for regex (escape other special chars)
        let mut escaped = String::new();
        let mut chars = regex_str.chars().peekable();
        while let Some(ch) = chars.next() {
            match ch {
                '*' => escaped.push_str(".*"),
                '?' => escaped.push_str("."),
                '.' | '+' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '^' | '$' | '\\' => {
                    escaped.push('\\');
                    escaped.push(ch);
                }
                _ => escaped.push(ch),
            }
        }
        escaped = escaped.replace(PLACEHOLDER, r"\d{10}");

        Regex::new(&format!("^{}$", escaped))
            .map_err(|e| format!("Invalid pattern '{}': {}", pattern, e).into())
    }

    /// List files matching the criteria (uses self.prefix and self.pattern).
    pub async fn list_files(&self) -> vector::Result<Vec<FileMetadata>> {
        self.list_files_at_impl(
            &self.prefix,
            self.pattern.as_ref(),
            false, // legacy path uses time filter
        )
        .await
    }

    /// List files at a specific prefix with optional pattern string (uses self time_range and max_keys).
    /// When `skip_time_filter` is true, last_modified is not filtered (e.g. for raw_logs hourly partitions).
    pub async fn list_files_at(
        &self,
        prefix: &str,
        pattern: Option<&str>,
        skip_time_filter: bool,
    ) -> vector::Result<Vec<FileMetadata>> {
        let compiled = pattern.map(Self::compile_pattern).transpose()?;
        self.list_files_at_impl(prefix, compiled.as_ref(), skip_time_filter)
            .await
    }

    async fn list_files_at_impl(
        &self,
        prefix: &str,
        pattern: Option<&Regex>,
        skip_time_filter: bool,
    ) -> vector::Result<Vec<FileMetadata>> {
        let prefix_path = ObjectStorePath::from(prefix.trim_end_matches('/'));
        
        info!("Listing files with prefix: {}", prefix);

        let mut files = Vec::new();
        let mut stream = self.object_store.list(Some(&prefix_path));

        while let Some(result) = stream.next().await {
            match result {
                Ok(meta) => {
                    let last_modified_dt = meta.last_modified;
                    
                    // Filter by time range (unless skip_time_filter, e.g. for raw_logs partitions)
                    if !skip_time_filter {
                        if let Some(start) = self.time_range_start {
                            if last_modified_dt < start {
                                continue;
                            }
                        }
                        if let Some(end) = self.time_range_end {
                            if last_modified_dt > end {
                                continue;
                            }
                        }
                    }

                    // Filter by pattern if provided
                    if let Some(pat) = pattern {
                        let path_str = meta.location.to_string();
                        if !pat.is_match(&path_str) {
                            continue;
                        }
                    }

                    // Extract bucket from path (for metadata)
                    let bucket = self.extract_bucket_from_path(&meta.location.to_string());

                    // Build full path (prefix + location)
                    let location_str = meta.location.to_string();
                    let full_path = if prefix.ends_with('/') {
                        format!("{}{}", prefix, location_str)
                    } else {
                        format!("{}/{}", prefix, location_str)
                    };

                    files.push(FileMetadata {
                        path: location_str,
                        size: meta.size as u64,
                        last_modified: last_modified_dt,
                        bucket,
                        full_path,
                    });

                    // Limit results
                    if files.len() >= self.max_keys {
                        break;
                    }
                }
                Err(e) => {
                    error!("Error listing file: {}", e);
                    // Continue with other files
                }
            }
        }

        info!("Found {} files matching criteria", files.len());
        for f in &files {
            info!(file_path = %f.path, file_size = f.size, "listed file");
        }
        Ok(files)
    }


    /// Extract bucket name from path
    fn extract_bucket_from_path(&self, path: &str) -> String {
        // Try to extract from URL-like paths
        if let Ok(url) = Url::parse(path) {
            if let Some(host) = url.host_str() {
                return host.to_string();
            }
        }
        
        // Fallback: extract from path segments
        path.split('/').next().unwrap_or("unknown").to_string()
    }

    /// List Delta Lake table root paths under list_prefix that contain table_subdir (e.g. "slowlogs").
    /// Returns unique paths like "deltalake/{project_id}/{uuid}/slowlogs".
    pub async fn list_delta_table_paths(
        &self,
        list_prefix: &str,
        table_subdir: &str,
    ) -> vector::Result<Vec<String>> {
        let prefix_path = ObjectStorePath::from(list_prefix.trim_end_matches('/'));
        let mut tables = HashSet::new();
        let mut stream = self.object_store.list(Some(&prefix_path));

        let marker = format!("/{}/", table_subdir);
        while let Some(result) = stream.next().await {
            match result {
                Ok(meta) => {
                    let loc = meta.location.to_string();
                    if let Some(idx) = loc.find(&marker) {
                        let table_path = format!("{}{}", &loc[..idx], table_subdir);
                        tables.insert(table_path);
                    }
                }
                Err(e) => {
                    error!("Error listing for delta tables: {}", e);
                }
            }
        }
        let mut out: Vec<_> = tables.into_iter().collect();
        out.sort();
        Ok(out)
    }

    /// List TopSQL instance paths under list_prefix (deltalake/org=X/cluster=Y/type=topsql_tidb/).
    /// Returns paths like "deltalake/org=X/cluster=Y/type=topsql_tidb/instance=db.tidb-0".
    pub async fn list_topsql_instance_paths(&self, list_prefix: &str) -> vector::Result<Vec<String>> {
        let prefix_path = ObjectStorePath::from(list_prefix.trim_end_matches('/'));
        let mut instances = HashSet::new();
        let mut stream = self.object_store.list(Some(&prefix_path));

        while let Some(result) = stream.next().await {
            match result {
                Ok(meta) => {
                    let loc = meta.location.to_string();
                    // location is like "instance=db.tidb-0/_delta_log/..." or "instance=db.tidb-0/part.parquet"
                    if let Some(inst) = loc.split('/').next() {
                        if inst.starts_with("instance=") {
                            let path = format!("{}/{}", list_prefix.trim_end_matches('/'), inst);
                            instances.insert(path);
                        }
                    }
                }
                Err(e) => {
                    error!("Error listing TopSQL instances: {}", e);
                }
            }
        }
        let mut out: Vec<_> = instances.into_iter().collect();
        out.sort();
        Ok(out)
    }

    /// List immediate subdirectory names under `prefix` (e.g. prefix "diagnosis/data/o11y/merged-logs/2026020411/"
    /// returns ["loki", "operator", "tidb", ...]). Uses list_with_delimiter to get common prefixes, then takes the last path segment of each.
    pub async fn list_subdir_names(&self, prefix: &str) -> vector::Result<Vec<String>> {
        let prefix_path = ObjectStorePath::from(prefix.trim_end_matches('/'));
        let result = self.object_store.list_with_delimiter(Some(&prefix_path)).await?;
        let mut names: Vec<String> = result
            .common_prefixes
            .iter()
            .filter_map(|p| {
                let s = p.to_string();
                s.trim_end_matches('/').split('/').last().map(|seg| seg.to_string())
            })
            .collect();
        names.sort();
        Ok(names)
    }

    /// Gzip magic bytes: 1f 8b (RFC 1952).
    const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

    /// Map object_store error to io::Error for StreamReader.
    fn map_store_err(e: object_store::Error) -> io::Error {
        io::Error::new(io::ErrorKind::Other, e.to_string())
    }

    /// Chunk size for streaming read: 16 MiB. BufReader capacities use this so each read_buf gets ~16 MiB
    /// (default BufReader is only 8 KB, which made each read tiny and slowed S3 streaming).
    const STREAM_READ_CHUNK_BYTES: usize = 16 * 1024 * 1024;

    /// Coalesce target: accumulate network chunks until at least this many bytes (2 MiB) before
    /// feeding to StreamReader. object_store/HTTP often yield small chunks (e.g. 64 KB); without
    /// coalescing we do "read small -> decompress small" every time and network stays idle during
    /// decompress. With coalescing we pass ~2 MiB compressed per read, so fewer decompress cycles.
    const STREAM_COALESCE_TARGET_BYTES: usize = 2 * 1024 * 1024;

    /// Build a stream that coalesces small Bytes into larger chunks (>= STREAM_COALESCE_TARGET_BYTES)
    /// so that each read from StreamReader gets more compressed data and we do fewer decompress cycles.
    fn coalesce_stream<S, E>(
        stream: S,
        target: usize,
    ) -> CoalesceStream<S>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
    {
        CoalesceStream {
            inner: Box::pin(stream),
            target,
            buf: BytesMut::new(),
        }
    }

    /// Stream file content in chunks (16 MiB per read), split by newlines, and process each line.
    /// Uses object_store's into_stream() and (when decompress_gzip) async GzipDecoder.
    /// For each line calls `on_line` to build an event; pushes to `batch`. When
    /// `batch_bytes` reaches `max_buffer_bytes`, sends the batch via `out` to avoid OOM.
    pub async fn stream_file_lines<F, O>(
        &self,
        path: &str,
        decompress_gzip: bool,
        batch: &mut Vec<O>,
        batch_bytes: &mut usize,
        max_buffer_bytes: usize,
        out: &mut vector::SourceSender,
        mut on_line: F,
    ) -> vector::Result<u64>
    where
        F: FnMut(String) -> O,
        O: Into<vector_lib::event::Event>,
    {
        let loc = ObjectStorePath::from(path.to_string());
        let get_result = self.object_store.get(&loc).await?;
        let mut stream = get_result.into_stream();

        let first = match stream.next().await {
            Some(Ok(b)) if !b.is_empty() => b,
            Some(Ok(_)) => {
                info!(path = %path, "streaming file (empty)");
                return Ok(0);
            }
            Some(Err(e)) => return Err(Self::map_store_err(e).into()),
            None => {
                info!(path = %path, "streaming file (empty)");
                return Ok(0);
            }
        };

        info!(path = %path, "streaming file started");
        let path_looks_gzip = path.ends_with(".gz") || path.ends_with(".log.gz");
        let content_looks_gzip = first.as_ref().starts_with(&Self::GZIP_MAGIC);
        let use_gzip = decompress_gzip && (path_looks_gzip || content_looks_gzip);

        let rest = stream.map(|r| r.map_err(Self::map_store_err));
        let full_stream = futures::stream::iter(std::iter::once(Ok(first))).chain(rest);
        let coalesced = Self::coalesce_stream(full_stream, Self::STREAM_COALESCE_TARGET_BYTES);
        let reader = StreamReader::new(coalesced);
        // Large buffer so we pull multi-MB from S3 per read (default BufReader is 8 KB).
        let buf_reader = BufReader::with_capacity(Self::STREAM_READ_CHUNK_BYTES, reader);

        let mut count = 0u64;
        let mut remainder = BytesMut::new();

        if use_gzip {
            let decoder = GzipDecoder::new(buf_reader);
            // Large buffer so each read_buf gets multi-MB decoded data (default is 8 KB).
            let mut decoded = BufReader::with_capacity(Self::STREAM_READ_CHUNK_BYTES, decoder);
            loop {
                let mut chunk = BytesMut::with_capacity(Self::STREAM_READ_CHUNK_BYTES);
                let n = decoded
                    .read_buf(&mut chunk)
                    .await
                    .map_err(|e| format!("stream read: {}", e))?;
                if n == 0 {
                    break;
                }
                let mut full = BytesMut::new();
                full.extend_from_slice(&remainder);
                full.extend_from_slice(&chunk);
                remainder.clear();
                let slice = full.as_ref();
                let last_nl = slice.iter().rposition(|&b| b == b'\n');
                let (complete, rest_slice) = if let Some(i) = last_nl {
                    (&slice[..=i], &slice[i + 1..])
                } else {
                    remainder.extend_from_slice(slice);
                    continue;
                };
                remainder.extend_from_slice(rest_slice);
                let text = String::from_utf8_lossy(complete);
                for line in text.lines() {
                    let line_str = line.trim_end_matches('\r');
                    let event = on_line(line_str.to_string());
                    *batch_bytes += line_str.len();
                    batch.push(event);
                    count += 1;
                    if max_buffer_bytes > 0 && *batch_bytes >= max_buffer_bytes {
                        let sent_bytes = *batch_bytes;
                        let to_send: Vec<vector_lib::event::Event> = batch.drain(..).map(Into::into).collect();
                        *batch_bytes = 0;
                        info!(path = %path, events = to_send.len(), content_bytes = sent_bytes, "file_list: flush batch reason=buffer_full, buffer cleared");
                        let _ = out.send_batch(to_send).await;
                    }
                }
                if max_buffer_bytes == 0 && !batch.is_empty() {
                    let sent_bytes = *batch_bytes;
                    let to_send: Vec<vector_lib::event::Event> = batch.drain(..).map(Into::into).collect();
                    *batch_bytes = 0;
                    info!(path = %path, events = to_send.len(), content_bytes = sent_bytes, "file_list: flush batch reason=after_chunk, buffer cleared");
                    let _ = out.send_batch(to_send).await;
                }
            }
            if !remainder.is_empty() {
                let text = String::from_utf8_lossy(&remainder);
                let line_str = text.trim_end_matches('\n').trim_end_matches('\r');
                if !line_str.is_empty() {
                    let event = on_line(line_str.to_string());
                    *batch_bytes += line_str.len();
                    batch.push(event);
                    count += 1;
                    if max_buffer_bytes > 0 && *batch_bytes >= max_buffer_bytes {
                        let sent_bytes = *batch_bytes;
                        let to_send: Vec<vector_lib::event::Event> = batch.drain(..).map(Into::into).collect();
                        *batch_bytes = 0;
                        info!(path = %path, events = to_send.len(), content_bytes = sent_bytes, "file_list: flush batch reason=buffer_full, buffer cleared");
                        let _ = out.send_batch(to_send).await;
                    }
                }
                if max_buffer_bytes == 0 && !batch.is_empty() {
                    let sent_bytes = *batch_bytes;
                    let to_send: Vec<vector_lib::event::Event> = batch.drain(..).map(Into::into).collect();
                    *batch_bytes = 0;
                    info!(path = %path, events = to_send.len(), content_bytes = sent_bytes, "file_list: flush batch reason=after_chunk, buffer cleared");
                    let _ = out.send_batch(to_send).await;
                }
            }
        } else {
            let mut decoded = buf_reader;
            loop {
                let mut chunk = BytesMut::with_capacity(Self::STREAM_READ_CHUNK_BYTES);
                let n = decoded
                    .read_buf(&mut chunk)
                    .await
                    .map_err(|e| format!("stream read: {}", e))?;
                if n == 0 {
                    break;
                }
                let mut full = BytesMut::new();
                full.extend_from_slice(&remainder);
                full.extend_from_slice(&chunk);
                remainder.clear();
                let slice = full.as_ref();
                let last_nl = slice.iter().rposition(|&b| b == b'\n');
                let (complete, rest_slice) = if let Some(i) = last_nl {
                    (&slice[..=i], &slice[i + 1..])
                } else {
                    remainder.extend_from_slice(slice);
                    continue;
                };
                remainder.extend_from_slice(rest_slice);
                let text = String::from_utf8_lossy(complete);
                for line in text.lines() {
                    let line_str = line.trim_end_matches('\r');
                    let event = on_line(line_str.to_string());
                    *batch_bytes += line_str.len();
                    batch.push(event);
                    count += 1;
                    if max_buffer_bytes > 0 && *batch_bytes >= max_buffer_bytes {
                        let sent_bytes = *batch_bytes;
                        let to_send: Vec<vector_lib::event::Event> = batch.drain(..).map(Into::into).collect();
                        *batch_bytes = 0;
                        info!(path = %path, events = to_send.len(), content_bytes = sent_bytes, "file_list: flush batch reason=buffer_full, buffer cleared");
                        let _ = out.send_batch(to_send).await;
                    }
                }
                if max_buffer_bytes == 0 && !batch.is_empty() {
                    let sent_bytes = *batch_bytes;
                    let to_send: Vec<vector_lib::event::Event> = batch.drain(..).map(Into::into).collect();
                    *batch_bytes = 0;
                    info!(path = %path, events = to_send.len(), content_bytes = sent_bytes, "file_list: flush batch reason=after_chunk, buffer cleared");
                    let _ = out.send_batch(to_send).await;
                }
            }
            if !remainder.is_empty() {
                let text = String::from_utf8_lossy(&remainder);
                let line_str = text.trim_end_matches('\n').trim_end_matches('\r');
                if !line_str.is_empty() {
                    let event = on_line(line_str.to_string());
                    *batch_bytes += line_str.len();
                    batch.push(event);
                    count += 1;
                    if max_buffer_bytes > 0 && *batch_bytes >= max_buffer_bytes {
                        let sent_bytes = *batch_bytes;
                        let to_send: Vec<vector_lib::event::Event> = batch.drain(..).map(Into::into).collect();
                        *batch_bytes = 0;
                        info!(path = %path, events = to_send.len(), content_bytes = sent_bytes, "file_list: flush batch reason=buffer_full, buffer cleared");
                        let _ = out.send_batch(to_send).await;
                    }
                }
                if max_buffer_bytes == 0 && !batch.is_empty() {
                    let sent_bytes = *batch_bytes;
                    let to_send: Vec<vector_lib::event::Event> = batch.drain(..).map(Into::into).collect();
                    *batch_bytes = 0;
                    info!(path = %path, events = to_send.len(), content_bytes = sent_bytes, "file_list: flush batch reason=after_chunk, buffer cleared");
                    let _ = out.send_batch(to_send).await;
                }
            }
        }
        info!(path = %path, lines = count, "streaming file finished");
        Ok(count)
    }

    /// Like `stream_file_lines` but sends each event as `(Some(Event), byte_size)` to `tx`.
    /// Sends `(None, 1)` after each 16 MiB chunk when max_buffer_bytes == 0 (flush per chunk).
    /// Sends `(None, 0)` at end of file. Used for parallel processing.
    pub async fn stream_file_lines_send<F, O>(
        &self,
        path: &str,
        decompress_gzip: bool,
        max_buffer_bytes: usize,
        tx: &mpsc::Sender<(Option<VectorEvent>, usize)>,
        mut on_line: F,
    ) -> vector::Result<u64>
    where
        F: FnMut(String) -> O,
        O: Into<VectorEvent>,
    {
        let loc = ObjectStorePath::from(path.to_string());
        let get_result = self.object_store.get(&loc).await?;
        let mut stream = get_result.into_stream();
        let first = match stream.next().await {
            Some(Ok(b)) if !b.is_empty() => b,
            Some(Ok(_)) => {
                info!(path = %path, "streaming file (empty)");
                let _ = tx.send((None, 0)).await;
                return Ok(0);
            }
            Some(Err(e)) => return Err(Self::map_store_err(e).into()),
            None => {
                info!(path = %path, "streaming file (empty)");
                let _ = tx.send((None, 0)).await;
                return Ok(0);
            }
        };
        info!(path = %path, "streaming file started");
        let path_looks_gzip = path.ends_with(".gz") || path.ends_with(".log.gz");
        let content_looks_gzip = first.as_ref().starts_with(&Self::GZIP_MAGIC);
        let use_gzip = decompress_gzip && (path_looks_gzip || content_looks_gzip);
        let rest = stream.map(|r| r.map_err(Self::map_store_err));
        let full_stream = futures::stream::iter(std::iter::once(Ok(first))).chain(rest);
        let coalesced = Self::coalesce_stream(full_stream, Self::STREAM_COALESCE_TARGET_BYTES);
        let reader = StreamReader::new(coalesced);
        let buf_reader = BufReader::with_capacity(Self::STREAM_READ_CHUNK_BYTES, reader);
        let mut count = 0u64;
        let mut remainder = BytesMut::new();
        if use_gzip {
            let decoder = GzipDecoder::new(buf_reader);
            let mut decoded = BufReader::with_capacity(Self::STREAM_READ_CHUNK_BYTES, decoder);
            loop {
                let mut chunk = BytesMut::with_capacity(Self::STREAM_READ_CHUNK_BYTES);
                let n = decoded.read_buf(&mut chunk).await.map_err(|e| format!("stream read: {}", e))?;
                if n == 0 {
                    break;
                }
                let mut full = BytesMut::new();
                full.extend_from_slice(&remainder);
                full.extend_from_slice(&chunk);
                remainder.clear();
                let slice = full.as_ref();
                let last_nl = slice.iter().rposition(|&b| b == b'\n');
                let (complete, rest_slice) = if let Some(i) = last_nl {
                    (&slice[..=i], &slice[i + 1..])
                } else {
                    remainder.extend_from_slice(slice);
                    continue;
                };
                remainder.extend_from_slice(rest_slice);
                let text = String::from_utf8_lossy(complete);
                for line in text.lines() {
                    let line_str = line.trim_end_matches('\r');
                    let event = on_line(line_str.to_string()).into();
                    tx.send((Some(event), line_str.len()))
                        .await
                        .map_err(|e| format!("channel closed: {}", e))?;
                    count += 1;
                }
                if max_buffer_bytes == 0 {
                    tx.send((None, 1)).await.map_err(|e| format!("channel closed: {}", e))?;
                }
            }
            if !remainder.is_empty() {
                let text = String::from_utf8_lossy(&remainder);
                let line_str = text.trim_end_matches('\n').trim_end_matches('\r');
                if !line_str.is_empty() {
                    let event = on_line(line_str.to_string()).into();
                    tx.send((Some(event), line_str.len()))
                        .await
                        .map_err(|e| format!("channel closed: {}", e))?;
                    count += 1;
                }
            }
        } else {
            let mut decoded = buf_reader;
            loop {
                let mut chunk = BytesMut::with_capacity(Self::STREAM_READ_CHUNK_BYTES);
                let n = decoded.read_buf(&mut chunk).await.map_err(|e| format!("stream read: {}", e))?;
                if n == 0 {
                    break;
                }
                let mut full = BytesMut::new();
                full.extend_from_slice(&remainder);
                full.extend_from_slice(&chunk);
                remainder.clear();
                let slice = full.as_ref();
                let last_nl = slice.iter().rposition(|&b| b == b'\n');
                let (complete, rest_slice) = if let Some(i) = last_nl {
                    (&slice[..=i], &slice[i + 1..])
                } else {
                    remainder.extend_from_slice(slice);
                    continue;
                };
                remainder.extend_from_slice(rest_slice);
                let text = String::from_utf8_lossy(complete);
                for line in text.lines() {
                    let line_str = line.trim_end_matches('\r');
                    let event = on_line(line_str.to_string()).into();
                    tx.send((Some(event), line_str.len()))
                        .await
                        .map_err(|e| format!("channel closed: {}", e))?;
                    count += 1;
                }
                if max_buffer_bytes == 0 {
                    tx.send((None, 1)).await.map_err(|e| format!("channel closed: {}", e))?;
                }
            }
            if !remainder.is_empty() {
                let text = String::from_utf8_lossy(&remainder);
                let line_str = text.trim_end_matches('\n').trim_end_matches('\r');
                if !line_str.is_empty() {
                    let event = on_line(line_str.to_string()).into();
                    tx.send((Some(event), line_str.len()))
                        .await
                        .map_err(|e| format!("channel closed: {}", e))?;
                    count += 1;
                }
            }
        }
        info!(path = %path, lines = count, "streaming file finished");
        tx.send((None, 0)).await.map_err(|e| format!("channel closed: {}", e))?;
        Ok(count)
    }

    /// Download file bytes from object store. When `decompress_gzip` is true, decompress if either
    /// the path ends with .gz/.log.gz or the content starts with gzip magic (1f 8b), so that
    /// misnamed or extension-less gzip content is still decompressed.
    /// Prefer stream_file_lines for large files to avoid OOM.
    pub async fn get_file_bytes(
        &self,
        path: &str,
        decompress_gzip: bool,
    ) -> vector::Result<Bytes> {
        let loc = ObjectStorePath::from(path.to_string());
        let get_result = self.object_store.get(&loc).await?;
        let raw = get_result.bytes().await?;
        let path_looks_gzip = path.ends_with(".gz") || path.ends_with(".log.gz");
        let content_looks_gzip = raw.as_ref().starts_with(&Self::GZIP_MAGIC);
        if decompress_gzip && (path_looks_gzip || content_looks_gzip) {
            let mut decoder = GzDecoder::new(raw.as_ref());
            let mut out = Vec::new();
            decoder
                .read_to_end(&mut out)
                .map_err(|e| format!("gzip decompress failed: {}", e))?;
            Ok(Bytes::from(out))
        } else {
            Ok(raw)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_matching() {
        // Create a minimal FileLister for testing pattern matching
        // Note: This test doesn't actually use object_store, just tests the pattern logic
        use std::sync::Arc;
        use object_store::memory::InMemory;
        
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // Test pattern compilation
        let pattern1 = FileLister::compile_pattern("{YYYYMMDDHH}/*.log").unwrap();
        assert!(pattern1.is_match("2026010804/file.log"));
        assert!(!pattern1.is_match("20260108045/file.log")); // 11 digits, should not match
        
        let pattern2 = FileLister::compile_pattern("*.log.gz").unwrap();
        assert!(pattern2.is_match("path/file.log.gz"));
        assert!(!pattern2.is_match("path/file.log")); // Missing .gz
    }
}
