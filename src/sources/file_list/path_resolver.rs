//! Path resolution for known o11y data types. Paths are fixed in code so users
//! only need to specify cluster_id, types, and time range.

use chrono::{DateTime, Datelike, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Known data types with fixed path conventions (bucket-relative).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataTypeKind {
    /// Gzip-compressed raw logs under diagnosis/data/{cluster_id}/merged-logs/{YYYYMMDDHH}/{component}/
    /// e.g. diagnosis/data/10324983984131567830/merged-logs/2026010804/tidb/db-*-tidb-0.log
    RawLogs,

    /// Delta Lake slowlog table: deltalake/{project_id}/{uuid}/slowlogs/
    Slowlog,

    /// Delta Lake sqlstatement table: deltalake/{project_id}/{uuid}/sqlstatement/
    SqlStatement,

    /// Delta Lake TopSQL per instance: deltalake/org={project_id}/cluster={cluster_id}/type=topsql_tidb/instance=*/
    TopSql,

    /// Conprof pprof compressed files: 0/{project_id}/{conprof_org_id}/{cluster_id}/profiles/*.log.gz
    Conprof,
}

impl fmt::Display for DataTypeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RawLogs => write!(f, "raw_logs"),
            Self::Slowlog => write!(f, "slowlog"),
            Self::SqlStatement => write!(f, "sql_statement"),
            Self::TopSql => write!(f, "top_sql"),
            Self::Conprof => write!(f, "conprof"),
        }
    }
}

/// A single file-list request: prefix + optional glob pattern.
#[derive(Debug, Clone)]
pub struct FileListRequest {
    pub prefix: String,
    pub pattern: Option<String>,
    /// When true, do not filter by last_modified (e.g. for raw_logs hourly partitions already encode time).
    pub skip_time_filter: bool,
}

/// A delta table path to emit (no file listing, just the table root path).
#[derive(Debug, Clone)]
pub struct DeltaTableRequest {
    /// Prefix to list under to discover table paths (e.g. deltalake/{project_id}/)
    pub list_prefix: String,
    /// Subdir name that identifies the table (e.g. "slowlogs", "sqlstatement")
    pub table_subdir: String,
}

/// TopSQL: list instance=* under type=topsql_tidb and emit each instance path.
#[derive(Debug, Clone)]
pub struct TopSqlListRequest {
    /// Prefix: deltalake/org={project_id}/cluster={cluster_id}/type=topsql_tidb/
    pub list_prefix: String,
}

/// When raw_log_components is not set: discover components by listing each hour prefix at runtime.
#[derive(Debug, Clone)]
pub struct RawLogsDiscoverRequest {
    /// One prefix per hour, e.g. "diagnosis/data/o11y/merged-logs/2026020411/"
    pub hour_prefixes: Vec<String>,
}

/// Resolved request: either list files (prefix+pattern), list delta tables, or discover raw_log components.
#[derive(Debug, Clone)]
pub enum ListRequest {
    FileList(FileListRequest),
    DeltaTable(DeltaTableRequest),
    TopSql(TopSqlListRequest),
    /// Raw_logs with components to be discovered by listing each hour prefix (when raw_log_components not specified).
    RawLogsDiscover(RawLogsDiscoverRequest),
}

/// Resolve list requests for the given types, cluster_id, project_id, and time range.
/// When types contains raw_logs: if `raw_log_components` is set (non-empty), use those; otherwise emit RawLogsDiscover so the runtime lists each hour prefix to discover component subdirs (all components).
pub fn resolve_requests(
    cluster_id: &str,
    project_id: Option<&str>,
    conprof_org_id: Option<&str>,
    types: &[DataTypeKind],
    time_start: Option<DateTime<Utc>>,
    time_end: Option<DateTime<Utc>>,
    raw_log_components: Option<&[String]>,
) -> vector::Result<Vec<ListRequest>> {
    let mut out = Vec::new();

    for &t in types {
        match t {
            DataTypeKind::RawLogs => {
                let (start, end) = match (time_start, time_end) {
                    (Some(s), Some(e)) => (s, e),
                    _ => {
                        return Err("raw_logs requires start_time and end_time".into());
                    }
                };
                let hour_prefixes: Vec<String> = hourly_range(start, end)
                    .map(|dt| {
                        let part = format!(
                            "{:04}{:02}{:02}{:02}",
                            dt.year(),
                            dt.month(),
                            dt.day(),
                            dt.hour()
                        );
                        format!("diagnosis/data/{}/merged-logs/{}/", cluster_id, part)
                    })
                    .collect();

                if let Some(c) = raw_log_components {
                    if !c.is_empty() {
                        for comp in c {
                            for prefix in &hour_prefixes {
                                out.push(ListRequest::FileList(FileListRequest {
                                    prefix: format!("{}{}/", prefix, comp),
                                    pattern: Some("*.log".to_string()),
                                    skip_time_filter: true,
                                }));
                            }
                        }
                        continue;
                    }
                }
                // Not specified => discover components by list at runtime
                out.push(ListRequest::RawLogsDiscover(RawLogsDiscoverRequest {
                    hour_prefixes,
                }));
            }

            DataTypeKind::Slowlog => {
                let pid = project_id
                    .filter(|s| !s.is_empty())
                    .ok_or("slowlog requires project_id")?;
                out.push(ListRequest::DeltaTable(DeltaTableRequest {
                    list_prefix: format!("deltalake/{}/", pid),
                    table_subdir: "slowlogs".to_string(),
                }));
            }

            DataTypeKind::SqlStatement => {
                let pid = project_id
                    .filter(|s| !s.is_empty())
                    .ok_or("sql_statement requires project_id")?;
                out.push(ListRequest::DeltaTable(DeltaTableRequest {
                    list_prefix: format!("deltalake/{}/", pid),
                    table_subdir: "sqlstatement".to_string(),
                }));
            }

            DataTypeKind::TopSql => {
                let pid = project_id
                    .filter(|s| !s.is_empty())
                    .ok_or("top_sql requires project_id")?;
                out.push(ListRequest::TopSql(TopSqlListRequest {
                    list_prefix: format!(
                        "deltalake/org={}/cluster={}/type=topsql_tidb/",
                        pid, cluster_id
                    ),
                }));
            }

            DataTypeKind::Conprof => {
                let pid = project_id
                    .filter(|s| !s.is_empty())
                    .ok_or("conprof requires project_id")?;
                let org = conprof_org_id.filter(|s| !s.is_empty()).unwrap_or(pid);
                let prefix = format!("0/{}/{}/{}/profiles/", pid, org, cluster_id);
                out.push(ListRequest::FileList(FileListRequest {
                    prefix,
                    pattern: Some("*.log.gz".to_string()),
                    skip_time_filter: false,
                }));
            }
        }
    }

    Ok(out)
}

/// Generate hourly timestamps in [start, end] (inclusive).
fn hourly_range(mut start: DateTime<Utc>, end: DateTime<Utc>) -> impl Iterator<Item = DateTime<Utc>> {
    // Truncate to hour
    start = start
        .with_minute(0)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();
    let end_hr = end
        .with_minute(0)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();

    std::iter::from_fn(move || {
        if start <= end_hr {
            let cur = start;
            start = start + chrono::Duration::hours(1);
            Some(cur)
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raw_logs_requires_time() {
        let r = resolve_requests(
            "10324983984131567830",
            None,
            None,
            &[DataTypeKind::RawLogs],
            None,
            None,
            None,
        );
        assert!(r.is_err());
    }

    #[test]
    fn test_slowlog_requires_project_id() {
        let r = resolve_requests(
            "c1",
            None,
            None,
            &[DataTypeKind::Slowlog],
            None,
            None,
            None,
        );
        assert!(r.is_err());
    }

    #[test]
    fn test_conprof_prefix() {
        let start = DateTime::parse_from_rfc3339("2026-01-08T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2026-01-08T01:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let r = resolve_requests(
            "10324983984131567830",
            Some("1372813089209061633"),
            Some("1372813089454544954"),
            &[DataTypeKind::Conprof],
            Some(start),
            Some(end),
            None,
        )
        .unwrap();
        assert_eq!(r.len(), 1);
        match &r[0] {
            ListRequest::FileList(f) => {
                assert_eq!(
                    f.prefix,
                    "0/1372813089209061633/1372813089454544954/10324983984131567830/profiles/"
                );
                assert_eq!(f.pattern.as_deref(), Some("*.log.gz"));
            }
            _ => panic!("expected FileList"),
        }
    }

    #[test]
    fn test_raw_logs_hourly_partitions() {
        let start = DateTime::parse_from_rfc3339("2026-01-08T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2026-01-08T02:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let r = resolve_requests(
            "10324983984131567830",
            None,
            None,
            &[DataTypeKind::RawLogs],
            Some(start),
            Some(end),
            None,
        )
        .unwrap();
        assert_eq!(r.len(), 1);
        match &r[0] {
            ListRequest::RawLogsDiscover(d) => {
                assert_eq!(d.hour_prefixes.len(), 3); // 00, 01, 02
                assert!(d.hour_prefixes[0].contains("2026010800"));
                assert!(d.hour_prefixes[0].contains("diagnosis/data/10324983984131567830/merged-logs/"));
            }
            _ => panic!("expected RawLogsDiscover when raw_log_components not set"),
        }
    }

    #[test]
    fn test_raw_logs_with_explicit_components() {
        let start = DateTime::parse_from_rfc3339("2026-01-08T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2026-01-08T01:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let comps = vec!["loki".to_string(), "operator".to_string()];
        let r = resolve_requests(
            "10324983984131567830",
            None,
            None,
            &[DataTypeKind::RawLogs],
            Some(start),
            Some(end),
            Some(&comps),
        )
        .unwrap();
        assert_eq!(r.len(), 2 * 2); // 2 hours × 2 components
        match &r[0] {
            ListRequest::FileList(f) => {
                assert!(f.prefix.contains("loki"));
                assert!(f.prefix.contains("2026010800"));
            }
            _ => panic!("expected FileList"),
        }
    }

    #[test]
    fn test_topsql_prefix() {
        let r = resolve_requests(
            "10324983984131567830",
            Some("1372813089209061633"),
            None,
            &[DataTypeKind::TopSql],
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(r.len(), 1);
        match &r[0] {
            ListRequest::TopSql(t) => {
                assert_eq!(
                    t.list_prefix,
                    "deltalake/org=1372813089209061633/cluster=10324983984131567830/type=topsql_tidb/"
                );
            }
            _ => panic!("expected TopSql"),
        }
    }
}
