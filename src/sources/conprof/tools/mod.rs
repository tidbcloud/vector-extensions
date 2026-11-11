mod jeprof_native;

use reqwest::Client;

/// Fetches jeprof "symbolized raw" profile natively (same output as historical Perl `jeprof --raw <url>`).
/// GET heap -> parse PCs -> POST /pprof/symbol, GET /pprof/cmdline -> build symbol header + raw body.
pub async fn fetch_raw_native(client: &Client, url: &str) -> Result<Vec<u8>, String> {
    jeprof_native::fetch_raw_symbolized(client, url).await
}
