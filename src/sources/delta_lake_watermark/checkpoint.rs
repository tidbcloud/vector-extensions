use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// Checkpoint structure for tracking sync progress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Last processed watermark (timestamp)
    pub last_watermark: Option<String>,

    /// Last processed unique ID (for handling same timestamp records)
    pub last_processed_id: Option<String>,

    /// Status: running, finished, error
    pub status: String,
}

impl Default for Checkpoint {
    fn default() -> Self {
        Self {
            last_watermark: None,
            last_processed_id: None,
            status: "running".to_string(),
        }
    }
}

impl Checkpoint {
    /// Load checkpoint from file
    pub fn load(checkpoint_path: &Path) -> vector::Result<Self> {
        if !checkpoint_path.exists() {
            info!("Checkpoint file does not exist, starting fresh");
            return Ok(Self::default());
        }

        match fs::read_to_string(checkpoint_path) {
            Ok(content) => {
                match serde_json::from_str::<Checkpoint>(&content) {
                    Ok(checkpoint) => {
                        info!(
                            "Loaded checkpoint: watermark={:?}, status={}",
                            checkpoint.last_watermark, checkpoint.status
                        );
                        Ok(checkpoint)
                    }
                    Err(e) => {
                        warn!("Failed to parse checkpoint file: {}. Starting fresh.", e);
                        Ok(Self::default())
                    }
                }
            }
            Err(e) => {
                warn!("Failed to read checkpoint file: {}. Starting fresh.", e);
                Ok(Self::default())
            }
        }
    }

    /// Save checkpoint to file
    pub fn save(&self, checkpoint_path: &Path) -> vector::Result<()> {
        // Ensure parent directory exists
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

    /// Get checkpoint file path for a given endpoint
    pub fn get_path(data_dir: &Path, endpoint: &str) -> PathBuf {
        // Create a safe filename from endpoint
        let safe_endpoint = endpoint
            .replace("://", "_")
            .replace("/", "_")
            .replace(":", "_")
            .replace(".", "_");
        data_dir.join(format!("delta_lake_watermark_{}.json", safe_endpoint))
    }

    /// Update watermark
    pub fn update_watermark(&mut self, watermark: String, unique_id: Option<String>) {
        self.last_watermark = Some(watermark);
        self.last_processed_id = unique_id;
    }

    /// Mark as finished
    /// Note: Currently not used in controller (end_time removed), but kept for API completeness
    #[allow(dead_code)]
    pub fn mark_finished(&mut self) {
        self.status = "finished".to_string();
    }

    /// Mark as error
    pub fn mark_error(&mut self) {
        self.status = "error".to_string();
    }

    /// Get last watermark as DateTime
    pub fn last_watermark_datetime(&self) -> Option<DateTime<Utc>> {
        self.last_watermark.as_ref().and_then(|w| {
            DateTime::parse_from_rfc3339(w)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // TC-004: Test checkpoint creation
    #[test]
    fn test_checkpoint_default() {
        let checkpoint = Checkpoint::default();
        assert_eq!(checkpoint.status, "running");
        assert!(checkpoint.last_watermark.is_none());
        assert!(checkpoint.last_processed_id.is_none());
    }

    // TC-005: Test checkpoint save/load
    #[test]
    fn test_checkpoint_save_load() {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_path = temp_dir.path().join("test_checkpoint.json");

        let mut checkpoint = Checkpoint::default();
        checkpoint.update_watermark("2026-01-01T00:00:00Z".to_string(), Some("id-001".to_string()));

        checkpoint.save(&checkpoint_path).unwrap();
        assert!(checkpoint_path.exists());

        let loaded = Checkpoint::load(&checkpoint_path).unwrap();
        assert_eq!(loaded.last_watermark, Some("2026-01-01T00:00:00Z".to_string()));
        assert_eq!(loaded.last_processed_id, Some("id-001".to_string()));
        assert_eq!(loaded.status, "running");
    }

    // TC-006: Test checkpoint update
    #[test]
    fn test_checkpoint_update_watermark() {
        let mut checkpoint = Checkpoint::default();
        
        // Update with timestamp only
        checkpoint.update_watermark("2026-01-01T00:00:00Z".to_string(), None);
        assert_eq!(checkpoint.last_watermark, Some("2026-01-01T00:00:00Z".to_string()));
        assert_eq!(checkpoint.last_processed_id, None);

        // Update with timestamp and unique_id
        checkpoint.update_watermark("2026-01-02T00:00:00Z".to_string(), Some("id-002".to_string()));
        assert_eq!(checkpoint.last_watermark, Some("2026-01-02T00:00:00Z".to_string()));
        assert_eq!(checkpoint.last_processed_id, Some("id-002".to_string()));
    }

    // TC-007: Test checkpoint status transitions
    #[test]
    fn test_checkpoint_status_transitions() {
        let mut checkpoint = Checkpoint::default();
        assert_eq!(checkpoint.status, "running");

        checkpoint.mark_finished();
        assert_eq!(checkpoint.status, "finished");

        checkpoint.mark_error();
        assert_eq!(checkpoint.status, "error");
    }

    // TC-008: Test checkpoint path generation
    #[test]
    fn test_checkpoint_path_generation() {
        let data_dir = PathBuf::from("/tmp/checkpoints");
        
        // Test S3 endpoint
        let endpoint1 = "s3://my-bucket/path/to/table";
        let path1 = Checkpoint::get_path(&data_dir, endpoint1);
        assert!(path1.to_string_lossy().contains("delta_lake_watermark"));
        assert!(path1.to_string_lossy().contains("s3_my-bucket_path_to_table"));

        // Test GCS endpoint
        let endpoint2 = "gs://my-bucket/path/to/table";
        let path2 = Checkpoint::get_path(&data_dir, endpoint2);
        assert!(path2.to_string_lossy().contains("delta_lake_watermark"));
        assert!(path2.to_string_lossy().contains("gs_my-bucket_path_to_table"));

        // Test with special characters
        let endpoint3 = "s3://bucket.with.dots/path:with:colons";
        let path3 = Checkpoint::get_path(&data_dir, endpoint3);
        assert!(!path3.to_string_lossy().contains("://"));
        assert!(!path3.to_string_lossy().contains(":"));
    }

    // TC-009: Test checkpoint load from non-existent file
    #[test]
    fn test_checkpoint_load_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_path = temp_dir.path().join("nonexistent.json");

        let loaded = Checkpoint::load(&checkpoint_path).unwrap();
        assert_eq!(loaded.status, "running");
        assert!(loaded.last_watermark.is_none());
    }

    // TC-010: Test checkpoint load from corrupted file
    #[test]
    fn test_checkpoint_load_corrupted() {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_path = temp_dir.path().join("corrupted.json");

        // Write invalid JSON
        std::fs::write(&checkpoint_path, "invalid json content").unwrap();

        let loaded = Checkpoint::load(&checkpoint_path).unwrap();
        // Should return default checkpoint on error
        assert_eq!(loaded.status, "running");
        assert!(loaded.last_watermark.is_none());
    }

    // TC-011: Test last_watermark_datetime conversion
    #[test]
    fn test_last_watermark_datetime() {
        use chrono::{Datelike, Timelike};
        
        let mut checkpoint = Checkpoint::default();
        
        // Test None watermark
        assert!(checkpoint.last_watermark_datetime().is_none());

        // Test valid timestamp
        checkpoint.update_watermark("2026-01-01T12:00:00Z".to_string(), None);
        let dt = checkpoint.last_watermark_datetime().unwrap();
        assert_eq!(dt.year(), 2026);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 1);
        assert_eq!(dt.hour(), 12);

        // Test invalid timestamp format (should return None)
        checkpoint.update_watermark("invalid-timestamp".to_string(), None);
        assert!(checkpoint.last_watermark_datetime().is_none());
    }
}
