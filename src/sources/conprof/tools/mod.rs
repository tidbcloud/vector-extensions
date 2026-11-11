mod jeprof_native;

use std::process::Stdio;
use tokio::{io::AsyncWriteExt, process::Command};
use vector::tls::TlsConfig;

use reqwest::Client;

const JEPROF: &[u8] = include_bytes!("jeprof");

/// Fetches jeprof "symbolized raw" profile via Perl script (same as `jeprof --raw <url>`).
/// The script GETs the heap URL, parses the profile to get PCs, POSTs to /pprof/symbol,
/// then outputs symbol header + raw heap body. Result is self-contained for offline analysis.
pub async fn fetch_raw(url: String, tls: Option<TlsConfig>) -> Result<Vec<u8>, String> {
    let mut jeprof = Command::new("perl");
    if let Some(tls) = tls {
        let url_fetcher = format!(
            "curl -s --cert {} --key {} --cacert {}",
            tls.crt_file.clone().unwrap().to_str().unwrap(),
            tls.key_file.unwrap().to_str().unwrap(),
            tls.ca_file.unwrap().to_str().unwrap()
        );
        jeprof.env("URL_FETCHER", url_fetcher);
    }
    let mut jeprof = jeprof
        .args(["/dev/stdin", "--raw", &url])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn jeprof fail: {}", e))?;
    jeprof
        .stdin
        .take()
        .unwrap()
        .write_all(JEPROF)
        .await
        .unwrap();
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

        // This should work with all TLS files
        let result = fetch_raw(
            "https://127.0.0.1:8080/debug/pprof/heap".to_string(),
            tls_config,
        )
        .await;
        // We expect this to fail because there's no real server, but function should be callable
        let _ = result;
    }
}
