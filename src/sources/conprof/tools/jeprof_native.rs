//! Native (Rust) implementation of jeprof --raw for remote heap profiles.
//! Produces the same output as the Perl script: symbol header + raw heap body.

use std::collections::{BTreeSet, HashMap};
use std::str;

use reqwest::Client;

/// Address length in hex nibbles (16 = 64-bit, 8 = 32-bit). Match jeprof default.
const ADDRESS_LENGTH: usize = 16;

/// Normalize hex address to fixed width (strip 0x and leading zeros, then pad to ADDRESS_LENGTH).
fn hex_extend(addr: &str) -> Option<String> {
    let s = addr.trim_start_matches("0x").trim_start_matches('0');
    if s.is_empty() {
        return Some("0".repeat(ADDRESS_LENGTH));
    }
    if s.chars().any(|c| !c.is_ascii_hexdigit()) {
        return None;
    }
    if s.len() > ADDRESS_LENGTH {
        return Some(s.to_string());
    }
    let zeros = ADDRESS_LENGTH - s.len();
    Some("0".repeat(zeros) + s)
}

/// Subtract 1 from address (for FixCallerAddresses: return address -> call site).
fn address_sub_one(hex_addr: &str) -> Option<String> {
    let s = hex_addr.trim_start_matches("0x").trim_start_matches('0');
    let mask = if ADDRESS_LENGTH >= 16 {
        u64::MAX
    } else {
        (1u64 << (ADDRESS_LENGTH * 4)) - 1
    };
    if s.is_empty() {
        return Some(format!("{:0width$x}", 0u64.wrapping_sub(1) & mask, width = ADDRESS_LENGTH));
    }
    let v = u64::from_str_radix(s, 16).ok()?;
    let r = v.wrapping_sub(1) & mask;
    Some(format!("{:0width$x}", r, width = ADDRESS_LENGTH))
}

/// Parse pprof heap profile text format and collect unique PCs (call sites).
/// Lines: optional % commands, then header "heap profile: ...", then
///   "\s*(\d+):\s*(\d+)\s*\[\s*(\d+):\s*(\d+)\]\s*@\s*(.*)" with addresses after @.
/// FixCallerAddresses: subtract 1 from each address except the first.
/// Returns sorted unique PCs as 0-padded hex strings (no 0x prefix, for consistent ordering).
fn parse_heap_profile_for_pcs(body: &[u8]) -> Option<Vec<String>> {
    let text = str::from_utf8(body).ok()?;
    let mut pcs: BTreeSet<String> = BTreeSet::new();
    let mut past_header = false;

    for line in text.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        if line.starts_with('%') {
            continue;
        }
        if !past_header {
            if line.starts_with("heap profile:") || line.starts_with("heap ") {
                past_header = true;
            }
            continue;
        }
        if line.starts_with("MAPPED_LIBRARIES:") || line.starts_with("--- Memory map:") {
            break;
        }
        // Match: optional whitespace, count1: bytes1 [ count2: bytes2 ] @ addr1 addr2 ...
        let rest = line.trim_start();
        let at_pos = rest.find(" @ ")?;
        let stack_part = rest.get(at_pos + 3..)?.trim();
        if stack_part.is_empty() {
            continue;
        }
        let addrs: Vec<&str> = stack_part.split_whitespace().collect();
        if addrs.is_empty() {
            continue;
        }
        for (i, addr) in addrs.iter().enumerate() {
            let extended = hex_extend(addr)?;
            let fixed = if i == 0 {
                extended
            } else {
                address_sub_one(&extended).unwrap_or(extended)
            };
            pcs.insert(fixed);
        }
    }

    if pcs.is_empty() {
        return None;
    }
    Some(pcs.into_iter().collect())
}

/// Build base URL from heap URL (strip last path segment). E.g. http://host/debug/pprof/heap -> http://host/debug/pprof
fn base_url_from_heap_url(heap_url: &str) -> &str {
    heap_url.rsplit_once('/').map(|(base, _)| base).unwrap_or(heap_url)
}

/// Fetch symbol names for given PCs via POST /pprof/symbol. Body: 0xaddr1+0xaddr2+... (sorted).
/// Response: first line "num_symbols: N", then "0x<addr> <symbol>" per line.
async fn fetch_symbols(
    client: &Client,
    base_url: &str,
    pcs: &[String],
) -> Result<HashMap<String, String>, String> {
    let post_body: String = pcs
        .iter()
        .map(|pc| format!("0x{}", pc))
        .collect::<Vec<_>>()
        .join("+");
    let symbol_url = format!("{}/symbol", base_url);
    let resp = client
        .post(&symbol_url)
        .body(post_body)
        .send()
        .await
        .map_err(|e| format!("symbol POST failed: {}", e))?;
    if !resp.status().is_success() {
        return Err(format!(
            "symbol endpoint returned {}",
            resp.status()
        ));
    }
    let text = resp
        .text()
        .await
        .map_err(|e| format!("symbol response read: {}", e))?;
    let mut map = HashMap::new();
    for line in text.lines() {
        let line = line.trim_end_matches('\r').trim();
        if line.starts_with("num_symbols:") || line.is_empty() {
            continue;
        }
        if line.starts_with("---") {
            break;
        }
        if let Some(rest) = line.strip_prefix("0x") {
            let mut it = rest.splitn(2, |c: char| c.is_whitespace());
            let addr = it.next().unwrap_or("").trim_start_matches('0');
            let symbol = it.next().unwrap_or("").trim();
            if !addr.is_empty() {
                if let Some(key) = hex_extend(addr) {
                    map.insert(key, symbol.to_string());
                }
            }
        }
    }
    Ok(map)
}

/// Fetch program name via GET /pprof/cmdline. Returns first line, NUL and newline stripped.
async fn fetch_cmdline(client: &Client, base_url: &str) -> Result<String, String> {
    let url = format!("{}/cmdline", base_url);
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("cmdline GET failed: {}", e))?;
    if !resp.status().is_success() {
        return Ok("(unknown)".to_string());
    }
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| format!("cmdline read: {}", e))?;
    let s = String::from_utf8_lossy(&bytes);
    let first_line = s.lines().next().unwrap_or("(unknown)");
    let name = first_line.split('\0').next().unwrap_or("(unknown)");
    Ok(name.trim().to_string())
}

/// Build full jeprof --raw output: --- symbol, binary=..., symbol table, ---, --- heap, raw body.
fn build_symbolized_output(
    program_name: &str,
    pcs: &[String],
    symbol_map: &HashMap<String, String>,
    raw_body: &[u8],
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"--- symbol\n");
    out.extend_from_slice(b"binary=");
    out.extend_from_slice(program_name.as_bytes());
    out.push(b'\n');
    for pc in pcs {
        let sym = symbol_map
            .get(pc)
            .map(|s| s.as_str())
            .unwrap_or("0x");
        out.extend_from_slice(b"0x");
        out.extend_from_slice(pc.as_bytes());
        out.push(b' ');
        out.extend_from_slice(sym.as_bytes());
        out.push(b'\n');
    }
    out.extend_from_slice(b"---\n");
    out.extend_from_slice(b"--- heap\n");
    out.extend_from_slice(raw_body);
    out
}

/// Full native jeprof --raw flow: GET heap -> parse PCs -> fetch symbols + cmdline -> build output.
/// If profile is binary or parsing yields no PCs, returns raw body only (no symbol header).
pub async fn fetch_raw_symbolized(
    client: &Client,
    heap_url: &str,
) -> Result<Vec<u8>, String> {
    let body = client
        .get(heap_url)
        .send()
        .await
        .map_err(|e| format!("http request failed: {}", e))?;
    if !body.status().is_success() {
        return Err(format!(
            "pprof endpoint returned {}: {}",
            body.status(),
            body.text().await.unwrap_or_default()
        ));
    }
    let raw_body = body
        .bytes()
        .await
        .map_err(|e| format!("read response body: {}", e))?
        .to_vec();

    let pcs = match parse_heap_profile_for_pcs(&raw_body) {
        Some(p) => p,
        None => {
            return Ok(raw_body);
        }
    };

    let base_url = base_url_from_heap_url(heap_url);
    let symbol_map = match fetch_symbols(client, base_url, &pcs).await {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(message = "jeprof native: symbol fetch failed, returning raw body", %e);
            return Ok(raw_body);
        }
    };
    let program_name = fetch_cmdline(client, base_url)
        .await
        .unwrap_or_else(|_| "(unknown)".to_string());

    Ok(build_symbolized_output(
        &program_name,
        &pcs,
        &symbol_map,
        &raw_body,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hex_extend() {
        assert_eq!(hex_extend("0x1234").unwrap(), "0000000000001234");
        assert_eq!(hex_extend("1234").unwrap(), "0000000000001234");
        assert_eq!(hex_extend("0").unwrap(), "0000000000000000");
    }

    #[test]
    fn test_parse_heap_profile_for_pcs() {
        let body = b"heap profile: 1: 2 [ 3: 4] @ heapprofile
    1: 1024 [ 1: 1024] @ 0x12345 0x67890 0xabc
";
        let pcs = parse_heap_profile_for_pcs(body).unwrap();
        assert!(!pcs.is_empty());
        assert!(pcs.iter().any(|s| s.contains("12345") || s.ends_with("12345")));
    }

    #[test]
    fn test_base_url_from_heap_url() {
        assert_eq!(
            base_url_from_heap_url("http://host:8080/debug/pprof/heap"),
            "http://host:8080/debug/pprof"
        );
    }
}
