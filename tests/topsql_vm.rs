// The test cases are run in the CI pipeline and cannot run individually,
// they depend on the full launch of various components like the tidb cluster,
// the victoriametrics, the vector as an agent between them and so on.
//
// Please run `make test-integration` to set up environment and run the test cases.

use std::future::Future;
use std::time::Duration;

use hyper::client::HttpConnector;
use hyper::Client;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

const VM_BASE_URL: &str = "http://127.0.0.1:8428/prometheus/api/v1";

#[tokio::test]
async fn topsql_vm() {
    let client = Client::new();

    // wait for setup
    wait_until(
        retry_with_interval(
            || async {
                let body = request(&client, format!("{}/label/__name__/values", VM_BASE_URL))
                    .await
                    .unwrap();

                Ok(body.contains("topsql_instance")
                    && body.contains("topsql_sql_meta")
                    && body.contains("topsql_plan_meta")
                    && body.contains("topsql_cpu_time_ms")
                    && body.contains("topsql_read_keys")
                    && body.contains("topsql_stmt_exec_count")
                    && body.contains("topsql_stmt_duration_sum_ns")
                    && body.contains("topsql_stmt_duration_count"))
            },
            Duration::from_secs(10),
        ),
        Duration::from_secs(120),
    )
    .await
    .expect("Timeout")
    .unwrap();

    // wait for data
    tokio::time::sleep(Duration::from_secs(120)).await;

    // check instances
    let body = request(
        &client,
        build_url(
            format!("{}/query", VM_BASE_URL),
            &[("query", "topsql_instance")],
        ),
    )
    .await
    .unwrap();
    assert!(body.contains(
        r#"{"__name__":"topsql_instance","instance":"127.0.0.1:10080","instance_type":"tidb"}"#
    ));
    assert!(body.contains(
        r#"{"__name__":"topsql_instance","instance":"127.0.0.1:20160","instance_type":"tikv"}"#
    ));

    // check tops
    let cases = vec![
        // args: top, metric, instance, instance_type
        (5, "topsql_cpu_time_ms", "127.0.0.1:10080", "tidb"),
        (5, "topsql_stmt_exec_count", "127.0.0.1:10080", "tidb"),
        (5, "topsql_stmt_duration_count", "127.0.0.1:10080", "tidb"),
        (5, "topsql_stmt_duration_sum_ns", "127.0.0.1:10080", "tidb"),
        (5, "topsql_cpu_time_ms", "127.0.0.1:20160", "tikv"),
        (5, "topsql_stmt_exec_count", "127.0.0.1:20160", "tikv"),
        (5, "topsql_read_keys", "127.0.0.1:20160", "tikv"),
    ];
    for (top, metric, instance, instance_type) in cases {
        check_top(&client, top, metric, instance, instance_type).await;
    }
}

async fn check_top(
    client: &Client<HttpConnector>,
    k: u32,
    metric: &str,
    instance: &str,
    instance_type: &str,
) {
    let url = top_url(k, metric, instance, instance_type);
    let body = request(client, url).await.unwrap();
    let sql_digests = extract_sql_digests(&body);

    let meta_url = meta_url(&sql_digests);
    let body = request(client, meta_url).await.unwrap();
    assert!(all_point_get(&body, k));
}

fn top_url(k: u32, metric: &str, instance: &str, instance_type: &str) -> String {
    build_url(
        format!("{}/query", VM_BASE_URL),
        &[(
            "query",
            &format!(
                "topk({}, sum_over_time({}{{instance=\"{}\", instance_type=\"{}\"}})[300s])",
                k, metric, instance, instance_type
            ),
        )],
    )
}

fn meta_url(digests: &str) -> String {
    build_url(
        format!("{}/query", VM_BASE_URL),
        &[(
            "query",
            &format!("topsql_sql_meta{{sql_digest=~\"{}\"}}", digests),
        )],
    )
}

fn extract_sql_digests(body: &str) -> String {
    lazy_static::lazy_static! {
        static ref RE: regex::Regex = regex::Regex::new(r#""sql_digest":"([^"]*)""#).unwrap();
    }

    RE.captures_iter(body)
        .map(|cap| cap.get(1).unwrap().as_str())
        .collect::<Vec<_>>()
        .join("|")
}

fn all_point_get(body: &str, count: u32) -> bool {
    lazy_static::lazy_static! {
        static ref EXTRA_RE: regex::Regex = regex::Regex::new(r#""normalized_sql":"([^"]*)""#).unwrap();
        static ref MATCH_RE: regex::Regex = regex::Regex::new("select `c` from `sbtest\\d+` where `id` = ?").unwrap();
    }

    let mut matched_count = 0;
    for cap in EXTRA_RE.captures_iter(body) {
        let sql = cap.get(1).unwrap().as_str();
        if !MATCH_RE.is_match(sql) {
            return false;
        }
        matched_count += 1;
    }
    matched_count == count
}

async fn request(client: &Client<HttpConnector>, url: String) -> Result<String, Error> {
    let url = url.parse()?;
    let response = client.get(url).await?;
    let (_, body) = response.into_parts();
    let bytes = hyper::body::to_bytes(body).await.unwrap();
    let result = String::from_utf8(bytes.into_iter().collect()).unwrap();
    Ok(result)
}

async fn retry_with_interval<F, Fut>(mut f: F, interval: Duration) -> Result<(), Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool, Error>>,
{
    let mut interval = tokio::time::interval(interval);
    loop {
        match f().await {
            Ok(v) if v => return Ok(()),
            Err(e) => return Err(e),
            Ok(_) => {
                interval.tick().await;
            }
        }
    }
}

async fn wait_until<F, T>(f: F, timeout: Duration) -> Option<T>
where
    F: Future<Output = T>,
{
    let mut f = Box::pin(f);
    let mut timeout = Box::pin(tokio::time::sleep(timeout));
    loop {
        tokio::select! {
            res = &mut f => {
                return Some(res);
            }
            _ = &mut timeout => {
                return None;
            }
        }
    }
}

fn build_url(url: String, queries: &[(&str, &str)]) -> String {
    let mut url = url::Url::parse(&url).unwrap();
    for (k, v) in queries {
        url.query_pairs_mut().append_pair(k, v);
    }
    url.into()
}
