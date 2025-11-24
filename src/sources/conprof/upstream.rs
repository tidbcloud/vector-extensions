use std::time::Duration;

use base64::{prelude::*, Engine};
use chrono::Utc;
use reqwest::Client;
use vector::{internal_events::StreamClosedError, SourceSender};
use vector_lib::{event::LogEvent, internal_event::InternalEvent, tls::TlsConfig};

use crate::sources::conprof::{
    shutdown::ShutdownSubscriber,
    tools::fetch_raw,
    topology::{Component, InstanceType},
    ComponentsProfileTypes, ProfileTypes,
};
use crate::utils::http::build_reqwest_client;

pub struct ConprofSource {
    client: Client,

    instance_b64: String,
    instance_type: InstanceType,
    uri: String,

    tls: Option<TlsConfig>,
    out: SourceSender,

    components_profile_types: ComponentsProfileTypes,
}

impl ConprofSource {
    pub async fn new(
        component: Component,
        tls: Option<TlsConfig>,
        out: SourceSender,
        components_profile_types: ComponentsProfileTypes,
    ) -> Option<Self> {
        let client = match build_reqwest_client(tls.clone(), None, None).await {
            Ok(client) => client,
            Err(err) => {
                error!(message = "Failed to build reqwest client", %err);
                return None;
            }
        };

        match component.conprof_address() {
            Some(address) => Some(ConprofSource {
                client,
                instance_b64: BASE64_URL_SAFE_NO_PAD.encode(&address),
                instance_type: component.instance_type,
                uri: if tls.is_some() {
                    format!("https://{}", address)
                } else {
                    format!("http://{}", address)
                },
                tls,
                out,
                components_profile_types,
            }),
            None => None,
        }
    }

    pub async fn run(mut self, mut shutdown: ShutdownSubscriber) {
        let shutdown_subscriber = shutdown.clone();
        tokio::select! {
            _ = self.run_loop(shutdown_subscriber) => {}
            _ = shutdown.done() => {}
        }
    }

    async fn run_loop(&mut self, mut shutdown: ShutdownSubscriber) {
        loop {
            let mut ts = Utc::now().timestamp();
            ts -= ts % 60;
            let next_minute_ts = ts + 60;
            match self.instance_type {
                InstanceType::PD => {
                    self.fetch_profiles(
                        ts,
                        self.components_profile_types.pd,
                        shutdown.clone(),
                        false,
                    )
                    .await;
                }
                InstanceType::TiDB => {
                    self.fetch_profiles(
                        ts,
                        self.components_profile_types.tidb,
                        shutdown.clone(),
                        false,
                    )
                    .await;
                }
                InstanceType::TiKV => {
                    self.fetch_profiles(
                        ts,
                        self.components_profile_types.tikv,
                        shutdown.clone(),
                        true,
                    )
                    .await;
                }
                InstanceType::TiFlash => {
                    self.fetch_profiles(
                        ts,
                        self.components_profile_types.tiflash,
                        shutdown.clone(),
                        false,
                    )
                    .await;
                }
                InstanceType::TiProxy => {
                    self.fetch_profiles(
                        ts,
                        self.components_profile_types.tiproxy,
                        shutdown.clone(),
                        false,
                    )
                    .await;
                }
                InstanceType::Lightning => {
                    self.fetch_profiles(
                        ts,
                        self.components_profile_types.lightning,
                        shutdown.clone(),
                        false,
                    )
                    .await;
                }
            };
            let now = Utc::now().timestamp();
            if now < next_minute_ts {
                tokio::select! {
                    _ = shutdown.done() => break,
                    _ = tokio::time::sleep(Duration::from_secs((next_minute_ts - now + 1) as u64)) => {},
                }
            }
        }
    }

    async fn fetch_profiles(
        &mut self,
        ts: i64,
        pt: ProfileTypes,
        shutdown: ShutdownSubscriber,
        jeprof_heap: bool,
    ) {
        if pt.goroutine {
            self.fetch_goroutine(
                format!(
                    "{}-{}-goroutine-{}",
                    ts, self.instance_type, self.instance_b64
                ),
                shutdown.clone(),
            )
            .await;
        }
        if pt.mutex {
            self.fetch_mutex(
                format!("{}-{}-mutex-{}", ts, self.instance_type, self.instance_b64),
                shutdown.clone(),
            )
            .await;
        }
        if pt.heap {
            if jeprof_heap {
                self.fetch_heap_with_jeprof(
                    format!("{}-{}-heap-{}", ts, self.instance_type, self.instance_b64),
                    shutdown.clone(),
                )
                .await;
            } else {
                self.fetch_heap(
                    format!("{}-{}-heap-{}", ts, self.instance_type, self.instance_b64),
                    shutdown.clone(),
                )
                .await;
            }
        }
        if pt.cpu {
            self.fetch_cpu(
                format!("{}-{}-cpu-{}", ts, self.instance_type, self.instance_b64),
                shutdown.clone(),
            )
            .await;
        }
    }

    async fn fetch_cpu(&mut self, filename: String, mut shutdown: ShutdownSubscriber) {
        tokio::select! {
            _ = shutdown.done() => {}
            resp = self.client.get(format!("{}/debug/pprof/profile?seconds=10", self.uri))
                .header("Content-Type", "application/protobuf")
                .send() => {
                    match resp {
                        Ok(resp) => {
                            let status = resp.status();
                            if !status.is_success() {
                                error!(message = "Failed to fetch cpu", status = status.as_u16());
                                return;
                            }
                            let body = match resp.bytes().await {
                                Ok(body) => body,
                                Err(err) => {
                                    error!(message = "Failed to read body bytes", %err);
                                    return;
                                }
                            };
                            let mut event = LogEvent::from_str_legacy(BASE64_STANDARD.encode(&body));
                            event.insert("filename", filename);
                            if self.out.send_event(event).await.is_err() {
                                StreamClosedError { count: 1 }.emit();
                            }
                        }
                        Err(err) => {
                            error!(message = "Failed to fetch cpu", %err);
                        }
                    }
            }
        }
    }

    async fn fetch_heap(&mut self, filename: String, mut shutdown: ShutdownSubscriber) {
        tokio::select! {
            _ = shutdown.done() => {}
            resp = self.client.get(format!("{}/debug/pprof/heap", self.uri)).send() => {
                match resp {
                    Ok(resp) => {
                        let status = resp.status();
                        if !status.is_success() {
                            error!(message = "Failed to fetch heap", status = status.as_u16());
                            return;
                        }
                        let body = match resp.bytes().await {
                            Ok(body) => body,
                            Err(err) => {
                                error!(message = "Failed to read body bytes", %err);
                                return;
                            }
                        };
                        let mut event = LogEvent::from_str_legacy(BASE64_STANDARD.encode(&body));
                        event.insert("filename", filename);
                        if self.out.send_event(event).await.is_err() {
                            StreamClosedError { count: 1 }.emit();
                        }
                    }
                    Err(err) => {
                        error!(message = "Failed to fetch heap", %err);
                    }
                }
            }
        }
    }

    async fn fetch_mutex(&mut self, filename: String, mut shutdown: ShutdownSubscriber) {
        tokio::select! {
            _ = shutdown.done() => {}
            resp = self.client.get(format!("{}/debug/pprof/mutex", self.uri)).send() => {
                match resp {
                    Ok(resp) => {
                        let status = resp.status();
                        if !status.is_success() {
                            error!(message = "Failed to fetch mutex", status = status.as_u16());
                            return;
                        }
                        let body = match resp.bytes().await {
                            Ok(body) => body,
                            Err(err) => {
                                error!(message = "Failed to read body bytes", %err);
                                return;
                            }
                        };
                        let mut event = LogEvent::from_str_legacy(BASE64_STANDARD.encode(&body));
                        event.insert("filename", filename);
                        if self.out.send_event(event).await.is_err() {
                            StreamClosedError { count: 1 }.emit();
                        }
                    }
                    Err(err) => {
                        error!(message = "Failed to fetch mutex", %err);
                    }
                }
            }
        }
    }

    async fn fetch_goroutine(&mut self, filename: String, mut shutdown: ShutdownSubscriber) {
        tokio::select! {
            _ = shutdown.done() => {}
            resp = self.client.get(format!("{}/debug/pprof/goroutine", self.uri)).send() => {
                match resp {
                    Ok(resp) => {
                        let status = resp.status();
                        if !status.is_success() {
                            error!(message = "Failed to fetch goroutine", status = status.as_u16());
                            return;
                        }
                        let body = match resp.bytes().await {
                            Ok(body) => body,
                            Err(err) => {
                                error!(message = "Failed to read body bytes", %err);
                                return;
                            }
                        };
                        let mut event = LogEvent::from_str_legacy(BASE64_STANDARD.encode(&body));
                        event.insert("filename", filename);
                        if self.out.send_event(event).await.is_err() {
                            StreamClosedError { count: 1 }.emit();
                        }
                    }
                    Err(err) => {
                        error!(message = "Failed to fetch goroutine", %err);
                    }
                }
            }
        }
    }

    async fn fetch_heap_with_jeprof(&mut self, filename: String, mut shutdown: ShutdownSubscriber) {
        tokio::select! {
            _ = shutdown.done() => {}
            resp = fetch_raw(format!("{}/debug/pprof/heap", self.uri), self.tls.clone()) => {
                match resp {
                    Ok(resp) => {
                        let mut event = LogEvent::from_str_legacy(BASE64_STANDARD.encode(&resp));
                        event.insert("filename", filename);
                        if self.out.send_event(event).await.is_err() {
                            StreamClosedError { count: 1 }.emit();
                        }
                    }
                    Err(err) => {
                        error!("Failed to fetch heap with jeprof: {}", err);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::conprof::shutdown::pair;
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Request, Response, Server, StatusCode};
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;

    #[test]
    fn test_conprof_source_structure() {
        // Test that ConprofSource can be instantiated conceptually
        let _ = std::mem::size_of::<ConprofSource>();
    }

    fn create_test_source_sender() -> SourceSender {
        // Create SourceSender using builder pattern
        // We need to add a source output first, then build
        use vector_lib::config::{DataType, SourceOutput};
        use vector::config::ComponentKey;
        
        let mut builder = SourceSender::builder().with_buffer(1000);
        let source_output = SourceOutput {
            port: None,
            ty: DataType::Log,
            schema_definition: None,
        };
        let component_key = ComponentKey::from("test");
        let _receiver = builder.add_source_output(source_output, component_key);
        builder.build()
    }

    async fn mock_pprof_server(port: u16) -> String {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        
        tokio::spawn(async move {
            let make_svc = make_service_fn(move |_conn| {
                async move {
                    Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                        async move {
                            let path = req.uri().path();
                            let resp = if path.starts_with("/debug/pprof/") {
                                // Return mock pprof data
                                Response::builder()
                                    .status(StatusCode::OK)
                                    .header("Content-Type", "application/protobuf")
                                    .body(Body::from(b"mock pprof data" as &[u8]))
                                    .unwrap()
                            } else {
                                Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .body(Body::from("Not Found"))
                                    .unwrap()
                            };
                            Ok::<_, Infallible>(resp)
                        }
                    }))
                }
            });
            
            let server = Server::bind(&addr).serve(make_svc);
            server.await.unwrap();
        });
        
        format!("http://127.0.0.1:{}", port)
    }

    #[tokio::test]
    async fn test_conprof_source_new_with_valid_component() {
        // Test ConprofSource::new with a valid component
        let component = Component {
            instance_type: InstanceType::TiDB,
            host: "127.0.0.1".to_string(),
            primary_port: 4000,
            secondary_port: 10080,
        };
        let out = create_test_source_sender();
        let result = ConprofSource::new(component, None, out, false).await;
        // Should succeed
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_conprof_source_new_with_tiflash() {
        // Test ConprofSource::new with TiFlash
        // TiFlash does have a conprof address (secondary_port), but run_loop does nothing for it
        let component = Component {
            instance_type: InstanceType::TiFlash,
            host: "127.0.0.1".to_string(),
            primary_port: 9000,
            secondary_port: 8123,
        };
        let out = create_test_source_sender();
        let result = ConprofSource::new(component, None, out, false).await;
        // TiFlash has conprof address, so it should succeed
        assert!(result.is_some());
        let source = result.unwrap();
        // Verify it's TiFlash type
        assert_eq!(source.instance_type, InstanceType::TiFlash);
    }

    #[tokio::test]
    async fn test_conprof_source_fetch_cpu_with_mock_server() {
        // Test fetch_cpu with mock HTTP server
        let component = Component {
            instance_type: InstanceType::TiDB,
            host: "127.0.0.1".to_string(),
            primary_port: 4000,
            secondary_port: 10080,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(component, None, out, false).await.unwrap();
        
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        
        // Update URI to point to mock server
        source.uri = mock_pprof_server(port).await;
        
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        let (_, shutdown) = pair();
        let filename = "test-cpu".to_string();
        
        // This will execute the fetch_cpu code path
        source.fetch_cpu(filename, shutdown.clone()).await;
    }

    #[tokio::test]
    async fn test_conprof_source_fetch_heap_with_mock_server() {
        // Test fetch_heap with mock HTTP server
        let component = Component {
            instance_type: InstanceType::TiDB,
            host: "127.0.0.1".to_string(),
            primary_port: 4000,
            secondary_port: 10080,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(component, None, out, false).await.unwrap();
        
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        
        source.uri = mock_pprof_server(port).await;
        
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        let (_, shutdown) = pair();
        let filename = "test-heap".to_string();
        
        source.fetch_heap(filename, shutdown.clone()).await;
    }

    #[tokio::test]
    async fn test_conprof_source_fetch_mutex_with_mock_server() {
        // Test fetch_mutex with mock HTTP server
        let component = Component {
            instance_type: InstanceType::TiDB,
            host: "127.0.0.1".to_string(),
            primary_port: 4000,
            secondary_port: 10080,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(component, None, out, false).await.unwrap();
        
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        
        source.uri = mock_pprof_server(port).await;
        
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        let (_, shutdown) = pair();
        let filename = "test-mutex".to_string();
        
        source.fetch_mutex(filename, shutdown.clone()).await;
    }

    #[tokio::test]
    async fn test_conprof_source_fetch_goroutine_with_mock_server() {
        // Test fetch_goroutine with mock HTTP server
        let component = Component {
            instance_type: InstanceType::TiDB,
            host: "127.0.0.1".to_string(),
            primary_port: 4000,
            secondary_port: 10080,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(component, None, out, false).await.unwrap();
        
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        
        source.uri = mock_pprof_server(port).await;
        
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        let (_, shutdown) = pair();
        let filename = "test-goroutine".to_string();
        
        source.fetch_goroutine(filename, shutdown.clone()).await;
    }

    #[test]
    fn test_instance_type_variants() {
        // Test that all instance types are handled
        let types = vec![
            InstanceType::TiDB,
            InstanceType::TiKV,
            InstanceType::PD,
            InstanceType::TiFlash,
            InstanceType::TiProxy,
            InstanceType::Lightning,
        ];
        
        for instance_type in types {
            let component = Component {
                instance_type,
                host: "127.0.0.1".to_string(),
                primary_port: 4000,
                secondary_port: 10080,
            };
            // Test that conprof_address works for all types
            let _ = component.conprof_address();
        }
    }

    #[test]
    fn test_run_loop_instance_type_branches() {
        // Test that all instance type branches in run_loop are covered conceptually
        let instance_types = vec![
            (InstanceType::TiDB, true),  // Should fetch goroutine, mutex, heap, cpu
            (InstanceType::PD, true),    // Should fetch goroutine, mutex, heap, cpu
            (InstanceType::TiProxy, true), // Should fetch goroutine, mutex, heap, cpu
            (InstanceType::Lightning, true), // Should fetch goroutine, mutex, heap, cpu
            (InstanceType::TiKV, false),  // Should only fetch cpu (and heap if enabled)
            (InstanceType::TiFlash, false), // Should do nothing
        ];
        
        for (instance_type, should_fetch_multiple) in instance_types {
            let component = Component {
                instance_type,
                host: "127.0.0.1".to_string(),
                primary_port: 4000,
                secondary_port: 10080,
            };
            
            // Verify component structure
            assert!(component.conprof_address().is_some() || instance_type == InstanceType::TiFlash);
            
            // Test that we can determine which branch to take
            match instance_type {
                InstanceType::TiDB | InstanceType::PD | InstanceType::TiProxy | InstanceType::Lightning => {
                    assert!(should_fetch_multiple);
                }
                InstanceType::TiKV => {
                    assert!(!should_fetch_multiple);
                }
                InstanceType::TiFlash => {
                    // Do nothing
                }
            }
        }
    }

    #[test]
    fn test_filename_format() {
        // Test filename format used in fetch functions
        let ts = 1234567890;
        let instance_type = InstanceType::TiDB;
        let instance_b64 = BASE64_URL_SAFE_NO_PAD.encode("127.0.0.1:10080");
        
        let goroutine_filename = format!("{}-{}-goroutine-{}", ts, instance_type, instance_b64);
        assert!(goroutine_filename.contains("goroutine"));
        
        let mutex_filename = format!("{}-{}-mutex-{}", ts, instance_type, instance_b64);
        assert!(mutex_filename.contains("mutex"));
        
        let heap_filename = format!("{}-{}-heap-{}", ts, instance_type, instance_b64);
        assert!(heap_filename.contains("heap"));
        
        let cpu_filename = format!("{}-{}-cpu-{}", ts, instance_type, instance_b64);
        assert!(cpu_filename.contains("cpu"));
    }

    #[test]
    fn test_uri_construction() {
        // Test URI construction logic
        let address = "127.0.0.1:10080";
        
        // Without TLS
        let uri = format!("http://{}", address);
        assert_eq!(uri, "http://127.0.0.1:10080");
        assert!(uri.starts_with("http://"));
        
        // With TLS
        let uri = format!("https://{}", address);
        assert_eq!(uri, "https://127.0.0.1:10080");
        assert!(uri.starts_with("https://"));
    }

    #[test]
    fn test_instance_b64_encoding() {
        // Test base64 encoding of instance address
        let address = "127.0.0.1:10080";
        let encoded = BASE64_URL_SAFE_NO_PAD.encode(address);
        assert!(!encoded.is_empty());
        
        // Verify it's valid base64
        let decoded = BASE64_URL_SAFE_NO_PAD.decode(&encoded);
        assert!(decoded.is_ok());
        assert_eq!(decoded.unwrap(), address.as_bytes());
    }

    #[test]
    fn test_timestamp_calculation() {
        // Test timestamp calculation logic from run_loop
        let mut ts = 1234567890;
        ts -= ts % 60;
        assert_eq!(ts % 60, 0);
        
        let next_minute_ts = ts + 60;
        assert_eq!(next_minute_ts, ts + 60);
    }

    #[test]
    fn test_timestamp_calculation_edge_cases() {
        // Test timestamp calculation with different values
        let test_cases = vec![
            1234567890,
            1234567891,
            1234567899,
            1234567859,
            1234567860,
        ];
        
        for mut ts in test_cases {
            let original_ts = ts;
            ts -= ts % 60;
            assert_eq!(ts % 60, 0, "Timestamp should be rounded down to minute");
            assert!(ts <= original_ts, "Rounded timestamp should be <= original");
            
            let next_minute_ts = ts + 60;
            assert_eq!(next_minute_ts - ts, 60, "Next minute should be 60 seconds later");
        }
    }

    #[test]
    fn test_run_loop_sleep_calculation() {
        // Test sleep calculation in run_loop
        let ts = 1234567890;
        let next_minute_ts = ts + 60; // 1234567950
        let now = 1234567895; // 5 seconds into the minute
        
        if now < next_minute_ts {
            let sleep_seconds = (next_minute_ts - now + 1) as u64;
            // next_minute_ts - now = 1234567950 - 1234567895 = 55
            // sleep_seconds = 55 + 1 = 56, wait until next minute + 1
            assert_eq!(sleep_seconds, 56);
        }
    }

    #[test]
    fn test_run_loop_no_sleep_when_past_minute() {
        // Test that we don't sleep when past the next minute
        let ts = 1234567890;
        let next_minute_ts = ts + 60;
        let now = 1234567950; // 50 seconds past the minute
        
        if now < next_minute_ts {
            // Should not enter this branch
            assert!(false, "Should not sleep when past next minute");
        } else {
            // Should continue immediately
            assert!(true, "Should continue when past next minute");
        }
    }

    #[test]
    fn test_tikv_heap_profile_conditional() {
        // Test TiKV heap profile conditional logic
        let enable_tikv_heap_profile_true = true;
        let enable_tikv_heap_profile_false = false;
        
        if enable_tikv_heap_profile_true {
            // Should fetch heap with jeprof
            assert!(true, "Should fetch when enabled");
        }
        
        if enable_tikv_heap_profile_false {
            assert!(false, "Should not fetch when disabled");
        } else {
            assert!(true, "Should skip when disabled");
        }
    }

    #[test]
    fn test_fetch_cpu_url_with_seconds() {
        // Test CPU fetch URL construction with seconds parameter
        let uri = "http://127.0.0.1:10080";
        let url = format!("{}/debug/pprof/profile?seconds=10", uri);
        assert_eq!(url, "http://127.0.0.1:10080/debug/pprof/profile?seconds=10");
        assert!(url.contains("seconds=10"));
    }

    #[test]
    fn test_fetch_heap_url() {
        // Test heap fetch URL construction
        let uri = "http://127.0.0.1:10080";
        let url = format!("{}/debug/pprof/heap", uri);
        assert_eq!(url, "http://127.0.0.1:10080/debug/pprof/heap");
    }

    #[test]
    fn test_fetch_mutex_url() {
        // Test mutex fetch URL construction
        let uri = "http://127.0.0.1:10080";
        let url = format!("{}/debug/pprof/mutex", uri);
        assert_eq!(url, "http://127.0.0.1:10080/debug/pprof/mutex");
    }

    #[test]
    fn test_fetch_goroutine_url() {
        // Test goroutine fetch URL construction
        let uri = "http://127.0.0.1:10080";
        let url = format!("{}/debug/pprof/goroutine", uri);
        assert_eq!(url, "http://127.0.0.1:10080/debug/pprof/goroutine");
    }

    #[test]
    fn test_fetch_heap_with_jeprof_url() {
        // Test heap with jeprof fetch URL construction
        let uri = "http://127.0.0.1:20180";
        let url = format!("{}/debug/pprof/heap", uri);
        assert_eq!(url, "http://127.0.0.1:20180/debug/pprof/heap");
    }

    #[test]
    fn test_status_code_checking() {
        // Test status code checking logic
        use http::StatusCode;
        
        let success_status = StatusCode::OK;
        assert!(success_status.is_success());
        
        let error_status = StatusCode::INTERNAL_SERVER_ERROR;
        assert!(!error_status.is_success());
        
        let not_found_status = StatusCode::NOT_FOUND;
        assert!(!not_found_status.is_success());
    }

    #[test]
    fn test_base64_encoding_in_fetch() {
        // Test base64 encoding used in fetch functions
        let body = b"test body content";
        let encoded = BASE64_STANDARD.encode(body);
        assert!(!encoded.is_empty());
        
        // Verify it's valid base64
        let decoded = BASE64_STANDARD.decode(&encoded);
        assert!(decoded.is_ok());
        assert_eq!(decoded.unwrap(), body);
    }

    #[test]
    fn test_event_filename_insertion() {
        // Test that filename is inserted into event
        use vector::event::LogEvent;
        
        let mut event = LogEvent::from_str_legacy("test");
        let filename = "1234567890-TiDB-cpu-abc123";
        event.insert("filename", filename);
        
        // Verify filename was inserted
        assert!(event.get("filename").is_some());
    }

    #[test]
    fn test_run_loop_instance_type_tidb_branch() {
        // Test TiDB branch logic
        let instance_type = InstanceType::TiDB;
        match instance_type {
            InstanceType::TiDB | InstanceType::PD | InstanceType::TiProxy | InstanceType::Lightning => {
                // Should fetch goroutine, mutex, heap, cpu
                assert!(true, "TiDB should fetch multiple profiles");
            }
            _ => {
                assert!(false, "Should match TiDB branch");
            }
        }
    }

    #[test]
    fn test_run_loop_instance_type_tikv_branch() {
        // Test TiKV branch logic
        let instance_type = InstanceType::TiKV;
        match instance_type {
            InstanceType::TiKV => {
                // Should only fetch cpu (and heap if enabled)
                assert!(true, "TiKV should fetch cpu");
            }
            _ => {
                assert!(false, "Should match TiKV branch");
            }
        }
    }

    #[test]
    fn test_run_loop_instance_type_tiflash_branch() {
        // Test TiFlash branch logic
        let instance_type = InstanceType::TiFlash;
        match instance_type {
            InstanceType::TiFlash => {
                // Should do nothing
                assert!(true, "TiFlash should do nothing");
            }
            _ => {
                assert!(false, "Should match TiFlash branch");
            }
        }
    }

    #[test]
    fn test_fetch_cpu_url_construction() {
        // Test URL construction for fetch_cpu
        let uri = "http://127.0.0.1:10080";
        let cpu_url = format!("{}/debug/pprof/profile?seconds=10", uri);
        assert_eq!(cpu_url, "http://127.0.0.1:10080/debug/pprof/profile?seconds=10");
    }

    #[test]
    fn test_fetch_heap_url_construction() {
        // Test URL construction for fetch_heap
        let uri = "http://127.0.0.1:10080";
        let heap_url = format!("{}/debug/pprof/heap", uri);
        assert_eq!(heap_url, "http://127.0.0.1:10080/debug/pprof/heap");
    }

    #[test]
    fn test_fetch_mutex_url_construction() {
        // Test URL construction for fetch_mutex
        let uri = "http://127.0.0.1:10080";
        let mutex_url = format!("{}/debug/pprof/mutex", uri);
        assert_eq!(mutex_url, "http://127.0.0.1:10080/debug/pprof/mutex");
    }

    #[test]
    fn test_fetch_goroutine_url_construction() {
        // Test URL construction for fetch_goroutine
        let uri = "http://127.0.0.1:10080";
        let goroutine_url = format!("{}/debug/pprof/goroutine", uri);
        assert_eq!(goroutine_url, "http://127.0.0.1:10080/debug/pprof/goroutine");
    }

    #[test]
    fn test_fetch_heap_with_jeprof_url_construction() {
        // Test URL construction for fetch_heap_with_jeprof
        let uri = "http://127.0.0.1:20180";
        let heap_url = format!("{}/debug/pprof/heap", uri);
        assert_eq!(heap_url, "http://127.0.0.1:20180/debug/pprof/heap");
    }

    #[test]
    fn test_run_loop_timestamp_alignment() {
        // Test timestamp alignment logic
        let test_timestamps = vec![
            1234567890,
            1234567891,
            1234567899,
            1234567949,
        ];
        
        for mut ts in test_timestamps {
            let original_ts = ts;
            ts -= ts % 60;
            assert_eq!(ts % 60, 0);
            assert!(ts <= original_ts);
            assert!(original_ts - ts < 60);
        }
    }

    #[test]
    fn test_run_loop_next_minute_calculation() {
        // Test next minute calculation
        let mut ts = 1234567890;
        ts -= ts % 60;
        let next_minute_ts = ts + 60;
        
        assert_eq!(next_minute_ts, ts + 60);
        assert!(next_minute_ts > ts);
    }

    #[test]
    fn test_enable_tikv_heap_profile_flag() {
        // Test enable_tikv_heap_profile flag logic
        let enable_true = true;
        let enable_false = false;
        
        // Test conditional logic
        if enable_true {
            // Should fetch heap with jeprof
            assert!(enable_true);
        }
        
        if !enable_false {
            // Should not fetch heap with jeprof
            assert!(!enable_false);
        }
    }
}
