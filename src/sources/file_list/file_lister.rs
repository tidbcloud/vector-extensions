use std::collections::HashSet;
use std::io::Read;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use futures::StreamExt;
use object_store::{path::Path as ObjectStorePath, ObjectStore};
use regex::Regex;
use tracing::{error, info};
use url::Url;

use super::object_store_builder::build_object_store;

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

    /// Download file bytes from object store. When `decompress_gzip` is true, decompress if either
    /// the path ends with .gz/.log.gz or the content starts with gzip magic (1f 8b), so that
    /// misnamed or extension-less gzip content is still decompressed.
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
