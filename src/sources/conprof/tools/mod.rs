mod jeprof_native;

use std::path::Path;
use std::process::Stdio;
use tokio::{io::AsyncWriteExt, process::Command};
use vector::tls::TlsConfig;

use reqwest::Client;

const JEPROF: &[u8] = include_bytes!("jeprof");

/// Host characters safe for shelling out through bundled jeprof `ShellEscape`.
/// Rejects quote/metacharacters that `http::Uri` still accepts in authority.
fn host_is_safe_for_jeprof(host: &str) -> bool {
    !host.is_empty()
        && host.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | ':' | '[' | ']')
        })
}

/// Validate a remote profile URL before handing it to Perl/jeprof `system()`.
fn validate_jeprof_fetch_url(url: &str) -> Result<(), String> {
    let uri: http::Uri = url
        .parse()
        .map_err(|e| format!("jeprof url is not a valid URI: {e}"))?;
    match uri.scheme_str() {
        Some("http") | Some("https") => {}
        other => {
            return Err(format!(
                "jeprof url scheme must be http or https, got {:?}",
                other
            ))
        }
    }
    // Older `http::Uri` has no user_info(); reject credentials-in-authority via `@`.
    if url.contains('@') {
        return Err("jeprof url must not contain userinfo".to_string());
    }
    let host = uri
        .host()
        .filter(|h| !h.is_empty())
        .ok_or_else(|| "jeprof url missing host".to_string())?;
    if !host_is_safe_for_jeprof(host) {
        return Err(format!(
            "jeprof url host contains characters unsafe for shell fetch: {host}"
        ));
    }
    Ok(())
}

fn path_as_os_str(path: &Path, label: &str) -> Result<String, String> {
    let s = path
        .to_str()
        .ok_or_else(|| format!("TLS {label} path is not valid UTF-8"))?;
    if s.is_empty() || s.chars().any(|c| c.is_control()) {
        return Err(format!("TLS {label} path is empty or contains control chars"));
    }
    Ok(s.to_owned())
}

/// Fetches jeprof "symbolized raw" profile via Perl script (same as `jeprof --raw <url>`).
/// The script GETs the heap URL, parses the profile to get PCs, POSTs to /pprof/symbol,
/// then outputs symbol header + raw heap body. Result is self-contained for offline analysis.
pub async fn fetch_raw(url: String, tls: Option<TlsConfig>) -> Result<Vec<u8>, String> {
    validate_jeprof_fetch_url(&url)?;

    let mut jeprof = Command::new("perl");
    if let Some(tls) = tls {
        let crt = tls
            .crt_file
            .as_ref()
            .ok_or_else(|| "TLS crt_file required for jeprof fetch".to_string())?;
        let key = tls
            .key_file
            .as_ref()
            .ok_or_else(|| "TLS key_file required for jeprof fetch".to_string())?;
        let ca = tls
            .ca_file
            .as_ref()
            .ok_or_else(|| "TLS ca_file required for jeprof fetch".to_string())?;
        // Discrete env vars avoid whitespace-splitting cert paths in URL_FETCHER.
        jeprof
            .env("JEPROF_CURL_CERT", path_as_os_str(crt, "crt_file")?)
            .env("JEPROF_CURL_KEY", path_as_os_str(key, "key_file")?)
            .env("JEPROF_CURL_CACERT", path_as_os_str(ca, "ca_file")?);
    }
    let mut jeprof = jeprof
        .args(["/dev/stdin", "--raw", &url])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn jeprof fail: {}", e))?;
    let mut stdin = jeprof
        .stdin
        .take()
        .ok_or_else(|| "jeprof stdin missing".to_string())?;
    stdin
        .write_all(JEPROF)
        .await
        .map_err(|e| format!("failed to write jeprof script: {e}"))?;
    drop(stdin);
    let output = jeprof
        .wait_with_output()
        .await
        .map_err(|e| format!("jeprof: {}", e))?;
    if !output.status.success() {
        let stderr = std::str::from_utf8(&output.stderr).unwrap_or("invalid utf8");
        return Err(format!("jeprof stderr: {:?}", stderr));
    }
    Ok(output.stdout)
}

/// Fetches jeprof "symbolized raw" profile natively (same output as Perl `jeprof --raw <url>`).
/// GET heap -> parse PCs -> POST /pprof/symbol, GET /pprof/cmdline -> build symbol header + raw body.
pub async fn fetch_raw_native(client: &Client, url: &str) -> Result<Vec<u8>, String> {
    jeprof_native::fetch_raw_symbolized(client, url).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_jeprof_constant_exists() {
        // Test that JEPROF constant is not empty
        assert!(!JEPROF.is_empty());
    }

    #[test]
    fn rejects_jeprof_url_with_shell_metacharacters_in_host() {
        // http::Uri allows these; ShellEscape without /g would let them break quoting.
        let url = "http://evil'';'x'$(id).example:20180/debug/pprof/heap";
        let err = validate_jeprof_fetch_url(url).unwrap_err();
        assert!(err.contains("unsafe for shell fetch"));
    }

    #[test]
    fn accepts_normal_jeprof_urls() {
        assert!(validate_jeprof_fetch_url("http://127.0.0.1:20180/debug/pprof/heap").is_ok());
        assert!(validate_jeprof_fetch_url("https://tikv-0.tikv-peer:20180/debug/pprof/heap?debug=1").is_ok());
        assert!(validate_jeprof_fetch_url("http://[::1]:20180/debug/pprof/heap").is_ok());
    }

    #[tokio::test]
    async fn test_fetch_raw_without_tls() {
        // This test will fail if perl is not available or jeprof script is invalid
        // But we can test the function structure
        let result = fetch_raw("http://127.0.0.1:8080/debug/pprof/heap".to_string(), None).await;
        // We expect this to fail because there's no real server, but function should be callable
        let _ = result;
    }

    #[tokio::test]
    async fn test_fetch_raw_with_tls() {
        let temp_dir = TempDir::new().unwrap();
        let ca_file = temp_dir.path().join("ca.crt");
        let crt_file = temp_dir.path().join("client.crt");
        let key_file = temp_dir.path().join("client.key");

        fs::write(&ca_file, "ca content").unwrap();
        fs::write(&crt_file, "cert content").unwrap();
        fs::write(&key_file, "key content").unwrap();

        let tls_config = Some(TlsConfig {
            ca_file: Some(ca_file),
            crt_file: Some(crt_file),
            key_file: Some(key_file),
            ..Default::default()
        });

        // This test will fail if perl is not available or jeprof script is invalid
        let result = fetch_raw(
            "https://127.0.0.1:8080/debug/pprof/heap".to_string(),
            tls_config,
        )
        .await;
        // We expect this to fail because there's no real server, but function should be callable
        let _ = result;
    }

    #[tokio::test]
    async fn test_fetch_raw_with_tls_partial() {
        let temp_dir = TempDir::new().unwrap();
        let ca_file = temp_dir.path().join("ca.crt");
        let crt_file = temp_dir.path().join("client.crt");
        let key_file = temp_dir.path().join("client.key");
        fs::write(&ca_file, "ca content").unwrap();
        fs::write(&crt_file, "cert content").unwrap();
        fs::write(&key_file, "key content").unwrap();

        let tls_config = Some(TlsConfig {
            ca_file: Some(ca_file),
            crt_file: Some(crt_file),
            key_file: Some(key_file),
            ..Default::default()
        });

        let result = fetch_raw(
            "https://127.0.0.1:8080/debug/pprof/heap".to_string(),
            tls_config,
        )
        .await;
        let _ = result;
    }

    #[tokio::test]
    async fn fetch_raw_rejects_unsafe_url_before_spawn() {
        let err = fetch_raw(
            "http://a'b'c'd.example:20180/debug/pprof/heap".to_string(),
            None,
        )
        .await
        .unwrap_err();
        assert!(err.contains("unsafe for shell fetch"));
    }
}
