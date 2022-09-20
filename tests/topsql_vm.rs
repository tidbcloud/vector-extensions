// The test cases are run in the CI pipeline and cannot run individually,
// they depend on the full launch of various components like the tidb cluster,
// the victoriametrics, the vector as an agent between them and so on.
//
// Please run `make test-integration` to set up environment and run the test cases.

use hyper::client::HttpConnector;
use hyper::Client;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

const VM_BASE_URL: &str = "http://127.0.0.1:8428/prometheus/api/v1";

#[tokio::test]
async fn topsql_vm() {
    let client = Client::new();

    // assert series
    let body = request(&client, format!("{}/label/__name__/values", VM_BASE_URL))
        .await
        .unwrap();
    assert!(body.contains("topsql_instance"));
    assert!(body.contains("topsql_sql_meta"));
    assert!(body.contains("topsql_plan_meta"));
    assert!(body.contains("topsql_cpu_time_ms"));
    assert!(body.contains("topsql_read_keys"));
    assert!(body.contains("topsql_stmt_exec_count"));
    assert!(body.contains("topsql_stmt_duration_sum_ns"));
    assert!(body.contains("topsql_stmt_duration_count"));

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
    check_all_point_get(&body, k);
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

fn check_all_point_get(body: &str, count: u32) {
    lazy_static::lazy_static! {
        static ref EXTRA_RE: regex::Regex = regex::Regex::new(r#""normalized_sql":"([^"]*)""#).unwrap();
        static ref MATCH_RE: regex::Regex = regex::Regex::new("select `c` from `sbtest\\d+` where `id` = ?").unwrap();
    }

    let mut matched_count = 0;
    for cap in EXTRA_RE.captures_iter(body) {
        let sql = cap.get(1).unwrap().as_str();
        assert!(
            MATCH_RE.is_match(sql),
            "unexpected sql: {}, body: {}",
            sql,
            body
        );
        matched_count += 1;
    }

    assert_eq!(matched_count, count, "body: {}", body);
}

async fn request(client: &Client<HttpConnector>, url: String) -> Result<String, Error> {
    let url = url.parse()?;
    let response = client.get(url).await?;
    let (_, body) = response.into_parts();
    let bytes = hyper::body::to_bytes(body).await.unwrap();
    let result = String::from_utf8(bytes.into_iter().collect()).unwrap();
    Ok(result)
}

fn build_url(url: String, queries: &[(&str, &str)]) -> String {
    let mut url = url::Url::parse(&url).unwrap();
    for (k, v) in queries {
        url.query_pairs_mut().append_pair(k, v);
    }
    url.into()
}
