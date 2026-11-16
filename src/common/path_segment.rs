//! Shared validation for destination path segments (object keys / Delta table URIs).

/// Reject path segments that could escape a configured prefix via `..` or separators.
///
/// Used wherever event- or topology-derived strings are spliced into object keys or
/// Hive-style path segments (`org=`, `cluster=`, `component=`, `instance=`).
pub fn is_safe_path_segment(segment: &str) -> bool {
    if segment.is_empty() || segment.contains('\0') {
        return false;
    }
    if segment.contains('/') || segment.contains('\\') {
        return false;
    }
    if segment == "." || segment == ".." || segment.contains("..") {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::is_safe_path_segment;

    #[test]
    fn accepts_normal_segments() {
        assert!(is_safe_path_segment("tidb"));
        assert!(is_safe_path_segment("2026010804"));
        assert!(is_safe_path_segment("persisted_statements_summary"));
        assert!(is_safe_path_segment("1372813089209061633"));
    }

    #[test]
    fn rejects_traversal_and_separators() {
        assert!(!is_safe_path_segment(""));
        assert!(!is_safe_path_segment(".."));
        assert!(!is_safe_path_segment("../etc"));
        assert!(!is_safe_path_segment("foo/bar"));
        assert!(!is_safe_path_segment("foo\\bar"));
        assert!(!is_safe_path_segment("a..b"));
        assert!(!is_safe_path_segment("/othertenant"));
    }
}
