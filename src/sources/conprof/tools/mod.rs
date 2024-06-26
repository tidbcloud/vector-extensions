use std::process::Stdio;
use tokio::{io::AsyncWriteExt, process::Command};
use vector::tls::TlsConfig;

const JEPROF: &[u8] = include_bytes!("jeprof");

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
