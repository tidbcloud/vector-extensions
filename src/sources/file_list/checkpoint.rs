//! Checkpoint for file_list source: record completed prefixes/units so that after OOM restart
//! we skip already-processed work and resume from the next unit.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// Checkpoint structure: set of completed unit keys (e.g. prefix or "delta:...").
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Keys that have been fully processed (e.g. S3 prefix for raw_logs, or "delta:..." for delta table).
    #[serde(default)]
    pub completed_keys: HashSet<String>,

    /// Status: running, finished, error
    #[serde(default = "default_status")]
    pub status: String,
}

fn default_status() -> String {
    "running".to_string()
}

impl Default for Checkpoint {
    fn default() -> Self {
        Self {
            completed_keys: HashSet::new(),
            status: "running".to_string(),
        }
    }
}

impl Checkpoint {
    /// Load checkpoint from file.
    pub fn load(checkpoint_path: &Path) -> vector::Result<Self> {
        if !checkpoint_path.exists() {
            info!("file_list: checkpoint file does not exist, starting fresh");
            return Ok(Self::default());
        }

        match fs::read_to_string(checkpoint_path) {
            Ok(content) => {
                match serde_json::from_str::<Checkpoint>(&content) {
                    Ok(checkpoint) => {
                        info!(
                            "file_list: loaded checkpoint: {} completed keys, status={}",
                            checkpoint.completed_keys.len(),
                            checkpoint.status
                        );
                        Ok(checkpoint)
                    }
                    Err(e) => {
                        warn!(
                            "file_list: failed to parse checkpoint file: {}. Starting fresh.",
                            e
                        );
                        Ok(Self::default())
                    }
                }
            }
            Err(e) => {
                warn!(
                    "file_list: failed to read checkpoint file: {}. Starting fresh.",
                    e
                );
                Ok(Self::default())
            }
        }
    }

    /// Save checkpoint to file.
    pub fn save(&self, checkpoint_path: &Path) -> vector::Result<()> {
        if let Some(parent) = checkpoint_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create checkpoint directory: {}", e))?;
        }

        let content = serde_json::to_string_pretty(self)
            .map_err(|e| format!("Failed to serialize checkpoint: {}", e))?;

        fs::write(checkpoint_path, content)
            .map_err(|e| format!("Failed to write checkpoint file: {}", e))?;

        Ok(())
    }

    /// Path for checkpoint file given data_dir and endpoint (e.g. s3://bucket).
    pub fn get_path(data_dir: &Path, endpoint: &str) -> PathBuf {
        let safe = endpoint
            .replace("://", "_")
            .replace('/', "_")
            .replace(':', "_")
            .replace('.', "_");
        data_dir.join(format!("file_list_{}.json", safe))
    }

    /// True if this unit key was already completed.
    pub fn is_completed(&self, key: &str) -> bool {
        self.completed_keys.contains(key)
    }

    /// Mark a unit as completed and return self for chaining (caller should save).
    pub fn add_completed(&mut self, key: String) {
        self.completed_keys.insert(key);
    }

    /// Mark as error (e.g. after OOM or fatal error).
    pub fn mark_error(&mut self) {
        self.status = "error".to_string();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_default_checkpoint() {
        let cp = Checkpoint::default();
        assert!(cp.completed_keys.is_empty());
        assert_eq!(cp.status, "running");
    }

    #[test]
    fn test_add_and_is_completed() {
        let mut cp = Checkpoint::default();
        assert!(!cp.is_completed("key1"));
        cp.add_completed("key1".to_string());
        assert!(cp.is_completed("key1"));
        assert!(!cp.is_completed("key2"));
    }

    #[test]
    fn test_save_and_load() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("checkpoint.json");
        let mut cp = Checkpoint::default();
        cp.add_completed("prefix1".to_string());
        cp.add_completed("prefix2".to_string());
        cp.save(&path).unwrap();

        let loaded = Checkpoint::load(&path).unwrap();
        assert!(loaded.is_completed("prefix1"));
        assert!(loaded.is_completed("prefix2"));
        assert!(!loaded.is_completed("prefix3"));
        assert_eq!(loaded.status, "running");
    }

    #[test]
    fn test_load_nonexistent() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("nonexistent.json");
        let loaded = Checkpoint::load(&path).unwrap();
        assert!(loaded.completed_keys.is_empty());
        assert_eq!(loaded.status, "running");
    }

    #[test]
    fn test_load_corrupted_json() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("corrupted.json");
        fs::write(&path, "not valid json!!!").unwrap();
        let loaded = Checkpoint::load(&path).unwrap();
        assert!(loaded.completed_keys.is_empty());
        assert_eq!(loaded.status, "running");
    }

    #[test]
    fn test_mark_error() {
        let mut cp = Checkpoint::default();
        assert_eq!(cp.status, "running");
        cp.mark_error();
        assert_eq!(cp.status, "error");
    }

    #[test]
    fn test_get_path_sanitizes_url() {
        let data_dir = Path::new("/tmp/data");
        let path = Checkpoint::get_path(data_dir, "s3://my-bucket/path/to");
        let name = path.file_name().unwrap().to_string_lossy();
        assert!(name.starts_with("file_list_"));
        assert!(name.ends_with(".json"));
        assert!(!name.contains("://"));
    }

    #[test]
    fn test_save_creates_parent_dirs() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("nested").join("dir").join("checkpoint.json");
        let cp = Checkpoint::default();
        cp.save(&path).unwrap();
        assert!(path.exists());
    }
}
