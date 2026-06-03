/// Returns true when a Delta Lake write error likely reflects a stale table log view,
/// for example after external compaction removed older `_delta_log` JSON files.
pub fn is_stale_delta_log_error(error_msg: &str) -> bool {
    error_msg.contains("log segment")
        || error_msg.contains("Invalid table version")
        || error_msg.contains("not found")
        || error_msg.contains("No such file or directory")
        || error_msg.contains("Kernel error")
        || error_msg.contains("No table metadata or protocol found in delta log")
        || error_msg.contains("Expected ordered contiguous commit files")
}

#[cfg(test)]
mod tests {
    use super::is_stale_delta_log_error;

    #[test]
    fn detects_missing_delta_log_file() {
        let msg = "Kernel error: File not found: deltalake/org=1/_delta_log/00000000000000020406.json";
        assert!(is_stale_delta_log_error(msg));
    }

    #[test]
    fn detects_kernel_error_without_file_not_found() {
        let msg = "Kernel error: No table metadata or protocol found in delta log.";
        assert!(is_stale_delta_log_error(msg));
    }

    #[test]
    fn ignores_unrelated_errors() {
        assert!(!is_stale_delta_log_error("permission denied"));
    }
}
