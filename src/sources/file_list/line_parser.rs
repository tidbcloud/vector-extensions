//! Parse log lines: built-in (Python logging + HTTP access) or user-provided regex with named capture groups.
//!
//! - Built-in Python: `2026-02-04 11:40:12,114 [slowlogconverter] [INFO] [Memory] message`
//! - Built-in HTTP:  `10.1.103.150 - - [04/Feb/2026 11:40:17] "GET /metrics HTTP/1.1" 200 -`
//! - Custom: user supplies regex(es) with named groups, e.g. `(?P<timestamp>\d{4}-\d{2}-\d{2}) (?P<level>\w+) (?P<msg>.*)`

use std::collections::BTreeMap;
use regex::Regex;

/// Line type for downstream filtering.
pub const LINE_TYPE_PYTHON: &str = "python_logging";
pub const LINE_TYPE_HTTP: &str = "http_access";
pub const LINE_TYPE_CUSTOM: &str = "custom";
pub const LINE_TYPE_RAW: &str = "raw";

/// Parsed fields (key -> value). Keys match what we insert into LogEvent.
pub type ParsedFields = BTreeMap<String, String>;

lazy_static::lazy_static! {
    /// Python logging: 2026-02-04 11:40:12,114 [slowlogconverter] [INFO] [Memory] msg
    /// Group 1: timestamp, 2: logger, 3: level, 4: optional tag, 5: message
    static ref RE_PYTHON: Regex = Regex::new(
        r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) \[([^\]]+)\] \[([^\]]+)\]\s*(?:\[([^\]]*)\]\s*)?(.*)$"
    ).expect("python log regex");

    /// HTTP access: 10.1.103.150 - - [04/Feb/2026 11:40:17] "GET /metrics HTTP/1.1" 200 -
    static ref RE_HTTP: Regex = Regex::new(
        r#"^(\S+) - - \[([^\]]+)\] "(\S+) ([^"]*) (\S+)" (\d+) (\S*).*$"#
    ).expect("http access regex");
}

/// Parse one log line into structured fields. Always sets "message" to the raw line.
/// Returns (line_type, parsed_fields). Fields use the same names as LogEvent keys.
pub fn parse_line(line: &str) -> (&'static str, ParsedFields) {
    let line = line.trim();
    let mut out = ParsedFields::new();
    out.insert("message".to_string(), line.to_string());

    if line.is_empty() {
        out.insert("line_type".to_string(), LINE_TYPE_RAW.to_string());
        return (LINE_TYPE_RAW, out);
    }

    if let Some(caps) = RE_PYTHON.captures(line) {
        out.insert("line_type".to_string(), LINE_TYPE_PYTHON.to_string());
        out.insert("log_timestamp".to_string(), caps.get(1).map(|m| m.as_str().to_string()).unwrap_or_default());
        out.insert("logger".to_string(), caps.get(2).map(|m| m.as_str().to_string()).unwrap_or_default());
        out.insert("level".to_string(), caps.get(3).map(|m| m.as_str().to_string()).unwrap_or_default());
        out.insert("tag".to_string(), caps.get(4).map(|m| m.as_str().to_string()).unwrap_or_default());
        if let Some(m) = caps.get(5) {
            out.insert("message_body".to_string(), m.as_str().trim().to_string());
        }
        return (LINE_TYPE_PYTHON, out);
    }

    if let Some(caps) = RE_HTTP.captures(line) {
        out.insert("line_type".to_string(), LINE_TYPE_HTTP.to_string());
        out.insert("client_ip".to_string(), caps.get(1).map(|m| m.as_str().to_string()).unwrap_or_default());
        out.insert("request_date".to_string(), caps.get(2).map(|m| m.as_str().to_string()).unwrap_or_default());
        out.insert("method".to_string(), caps.get(3).map(|m| m.as_str().to_string()).unwrap_or_default());
        out.insert("path".to_string(), caps.get(4).map(|m| m.as_str().to_string()).unwrap_or_default());
        out.insert("protocol".to_string(), caps.get(5).map(|m| m.as_str().to_string()).unwrap_or_default());
        out.insert("status".to_string(), caps.get(6).map(|m| m.as_str().to_string()).unwrap_or_default());
        out.insert("response_size".to_string(), caps.get(7).map(|m| m.as_str().to_string()).unwrap_or_default());
        return (LINE_TYPE_HTTP, out);
    }

    out.insert("line_type".to_string(), LINE_TYPE_RAW.to_string());
    (LINE_TYPE_RAW, out)
}

/// Parse one log line using only user-provided regexes (with named capture groups).
/// Tries each regex in order; on first match, returns fields from named groups + "message" (raw line) + "line_type"="custom".
/// Returns None if no regex matches.
pub fn parse_line_with_regexes(line: &str, regexes: &[Regex]) -> Option<ParsedFields> {
    let line = line.trim();
    let mut out = ParsedFields::new();
    out.insert("message".to_string(), line.to_string());

    for re in regexes {
        if let Some(caps) = re.captures(line) {
            for name in re.capture_names().flatten() {
                if let Some(m) = caps.name(name) {
                    out.insert(name.to_string(), m.as_str().to_string());
                }
            }
            out.insert("line_type".to_string(), LINE_TYPE_CUSTOM.to_string());
            return Some(out);
        }
    }
    None
}

/// Compile a list of regex strings. Each must have at least one named capture group `(?P<name>...)`.
/// Returns error if any string is invalid or has no named groups.
pub fn compile_line_parse_regexes(regex_strs: &[String]) -> vector::Result<Vec<Regex>> {
    let mut out = Vec::with_capacity(regex_strs.len());
    for (i, s) in regex_strs.iter().enumerate() {
        let re = Regex::new(s).map_err(|e| format!("line_parse_regexes[{}] invalid: {}", i, e))?;
        if !re.capture_names().any(|n| n.is_some()) {
            return Err(format!(
                "line_parse_regexes[{}] has no named capture groups; use (?P<name>...)",
                i
            )
            .into());
        }
        out.push(re);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_python_line() {
        let line = "2026-02-04 11:40:12,114 [slowlogconverter] [INFO] [Memory] typing_extensions._TypedDictMeta: 451 objects, 0.73 MB";
        let (t, f) = parse_line(line);
        assert_eq!(t, LINE_TYPE_PYTHON);
        assert_eq!(f.get("log_timestamp").map(String::as_str), Some("2026-02-04 11:40:12,114"));
        assert_eq!(f.get("logger").map(String::as_str), Some("slowlogconverter"));
        assert_eq!(f.get("level").map(String::as_str), Some("INFO"));
        assert_eq!(f.get("tag").map(String::as_str), Some("Memory"));
        assert!(f.get("message_body").map(|s| s.contains("typing_extensions")).unwrap_or(false));
    }

    #[test]
    fn test_http_line() {
        let line = r#"10.1.103.150 - - [04/Feb/2026 11:40:17] "GET /metrics HTTP/1.1" 200 -"#;
        let (t, f) = parse_line(line);
        assert_eq!(t, LINE_TYPE_HTTP);
        assert_eq!(f.get("client_ip").map(String::as_str), Some("10.1.103.150"));
        assert_eq!(f.get("method").map(String::as_str), Some("GET"));
        assert_eq!(f.get("path").map(String::as_str), Some("/metrics"));
        assert_eq!(f.get("status").map(String::as_str), Some("200"));
    }

    #[test]
    fn test_parse_line_with_regexes() {
        let re = Regex::new(r"^(?P<ts>\d{4}-\d{2}-\d{2}) (?P<level>\w+): (?P<msg>.*)$").unwrap();
        let regexes = [re];
        let line = "2026-02-04 INFO: hello world";
        let f = parse_line_with_regexes(line, &regexes).unwrap();
        assert_eq!(f.get("line_type").map(String::as_str), Some(LINE_TYPE_CUSTOM));
        assert_eq!(f.get("ts").map(String::as_str), Some("2026-02-04"));
        assert_eq!(f.get("level").map(String::as_str), Some("INFO"));
        assert_eq!(f.get("msg").map(String::as_str), Some("hello world"));
        assert_eq!(f.get("message").map(String::as_str), Some(line));
    }

    #[test]
    fn test_compile_line_parse_regexes() {
        let valid = vec![r"(?P<a>.)".to_string()];
        assert!(compile_line_parse_regexes(&valid).is_ok());
        let no_names = vec!["(.)".to_string()];
        assert!(compile_line_parse_regexes(&no_names).is_err());
        let invalid = vec!["[".to_string()];
        assert!(compile_line_parse_regexes(&invalid).is_err());
    }
}
