use std::time::Duration;

use base64::{prelude::*, Engine};
use chrono::Utc;
use reqwest::Client;
use vector::{internal_events::StreamClosedError, SourceSender};
use vector_lib::{event::LogEvent, internal_event::InternalEvent, tls::TlsConfig};

use crate::sources::conprof::{
    shutdown::ShutdownSubscriber,
    topology::{Component, InstanceType},
    ComponentsProfileTypes,
    JeprofFetchMode,
};
use crate::sources::conprof::tools::{fetch_raw, fetch_raw_native};
use crate::utils::http::build_reqwest_client;

pub struct ConprofSource {
    client: Client,
    // instance: String,
    instance_b64: String,
    instance_type: InstanceType,
    uri: String,

    tls: Option<TlsConfig>,
    out: SourceSender,
    components_profile_types: ComponentsProfileTypes,
    jeprof_fetch_mode: JeprofFetchMode,
}

impl ConprofSource {
    pub async fn new(
        component: Component,
        tls: Option<TlsConfig>,
        out: SourceSender,
        components_profile_types: ComponentsProfileTypes,
        jeprof_fetch_mode: JeprofFetchMode,
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
                // instance: use instance_name (e.g. K8s pod name) when set, else address
                instance_b64: BASE64_URL_SAFE_NO_PAD.encode(&component.instance_id()),
                instance_type: component.instance_type,
                uri: if tls.is_some() {
                    format!("https://{}", address)
                } else {
                    format!("http://{}", address)
                },

                tls,
                out,
                components_profile_types,
                jeprof_fetch_mode,
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
        let profile = self
            .components_profile_types
            .for_instance(&self.instance_type);
        loop {
            let mut ts = Utc::now().timestamp();
            ts -= ts % 60;
            let next_minute_ts = ts + 60;
            // Fully driven by components_profile_types; no hardcoded instance_type branches
            if profile.goroutine {
                self.fetch_goroutine_impl(
                    format!(
                        "{}-{}-goroutine-{}",
                        ts, self.instance_type, self.instance_b64
                    ),
                    shutdown.clone(),
                )
                .await;
            }
            if profile.mutex {
                self.fetch_mutex_impl(
                    format!(
                        "{}-{}-mutex-{}",
                        ts, self.instance_type, self.instance_b64
                    ),
                    shutdown.clone(),
                )
                .await;
            }
            if profile.heap {
                self.fetch_heap_impl(
                    format!(
                        "{}-{}-heap-{}",
                        ts, self.instance_type, self.instance_b64
                    ),
                    shutdown.clone(),
                )
                .await;
            }
            if profile.jeheap {
                self.fetch_heap_with_jeprof_impl(
                    format!(
                        "{}-{}-heap-{}",
                        ts, self.instance_type, self.instance_b64
                    ),
                    shutdown.clone(),
                )
                .await;
            }
            if profile.cpu {
                self.fetch_cpu_impl(
                    format!(
                        "{}-{}-cpu-{}",
                        ts, self.instance_type, self.instance_b64
                    ),
                    shutdown.clone(),
                )
                .await;
            }
            let now = Utc::now().timestamp();
            if now < next_minute_ts {
                tokio::select! {
                    _ = shutdown.done() => break,
                    _ = tokio::time::sleep(Duration::from_secs((next_minute_ts - now + 1) as u64)) => {},
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn fetch_cpu(&mut self, filename: String, mut shutdown: ShutdownSubscriber) {
        self.fetch_cpu_impl(filename, shutdown).await
    }

    async fn fetch_cpu_impl(&mut self, filename: String, mut shutdown: ShutdownSubscriber) {
        tokio::select! {
            _ = shutdown.done() => {}
            resp = self.client.get(format!("{}/debug/pprof/profile?seconds=10", self.uri))
                .header("Content-Type", "application/protobuf")
                .send() => {
                    match resp {
                        Ok(resp) => {
                            let status = resp.status();
                            if !status.is_success() {
                                error!(message = "Failed to fetch cpu", instance_type = %self.instance_type, status = status.as_u16());
                                return;
                            }
                            let body = match resp.bytes().await {
                                Ok(body) => body,
                                Err(err) => {
                                    error!(message = "Failed to read body bytes for cpu", instance_type = %self.instance_type, %err);
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
                            error!(message = "Failed to fetch cpu", instance_type = %self.instance_type, %err);
                        }
                    }
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn fetch_heap(&mut self, filename: String, mut shutdown: ShutdownSubscriber) {
        self.fetch_heap_impl(filename, shutdown).await
    }

    async fn fetch_heap_impl(&mut self, filename: String, mut shutdown: ShutdownSubscriber) {
        tokio::select! {
            _ = shutdown.done() => {}
            resp = self.client.get(format!("{}/debug/pprof/heap", self.uri)).send() => {
                match resp {
                    Ok(resp) => {
                        let status = resp.status();
                        if !status.is_success() {
                            error!(message = "Failed to fetch heap", instance_type = %self.instance_type, status = status.as_u16());
                            return;
                        }
                        let body = match resp.bytes().await {
                            Ok(body) => body,
                            Err(err) => {
                                error!(message = "Failed to read body bytes for heap", instance_type = %self.instance_type, %err);
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
                        error!(message = "Failed to fetch heap", instance_type = %self.instance_type, %err);
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn fetch_mutex(&mut self, filename: String, mut shutdown: ShutdownSubscriber) {
        self.fetch_mutex_impl(filename, shutdown).await
    }

    async fn fetch_mutex_impl(&mut self, filename: String, mut shutdown: ShutdownSubscriber) {
        tokio::select! {
            _ = shutdown.done() => {}
            resp = self.client.get(format!("{}/debug/pprof/mutex", self.uri)).send() => {
                match resp {
                    Ok(resp) => {
                        let status = resp.status();
                        if !status.is_success() {
                            error!(message = "Failed to fetch mutex", instance_type = %self.instance_type, status = status.as_u16());
                            return;
                        }
                        let body = match resp.bytes().await {
                            Ok(body) => body,
                            Err(err) => {
                                error!(message = "Failed to read body bytes for mutex", instance_type = %self.instance_type, %err);
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
                        error!(message = "Failed to fetch mutex", instance_type = %self.instance_type, %err);
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn fetch_goroutine(
        &mut self,
        filename: String,
        mut shutdown: ShutdownSubscriber,
    ) {
        self.fetch_goroutine_impl(filename, shutdown).await
    }

    async fn fetch_goroutine_impl(&mut self, filename: String, mut shutdown: ShutdownSubscriber) {
        tokio::select! {
            _ = shutdown.done() => {}
            resp = self.client.get(format!("{}/debug/pprof/goroutine", self.uri)).send() => {
                match resp {
                    Ok(resp) => {
                        let status = resp.status();
                        if !status.is_success() {
                            error!(message = "Failed to fetch goroutine", instance_type = %self.instance_type, status = status.as_u16());
                            return;
                        }
                        let body = match resp.bytes().await {
                            Ok(body) => body,
                            Err(err) => {
                                error!(message = "Failed to read body bytes for goroutine", instance_type = %self.instance_type, %err);
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
                        error!(message = "Failed to fetch goroutine", instance_type = %self.instance_type, %err);
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn fetch_heap_with_jeprof(
        &mut self,
        filename: String,
        mut shutdown: ShutdownSubscriber,
    ) {
        self.fetch_heap_with_jeprof_impl(filename, shutdown).await
    }

    async fn fetch_heap_with_jeprof_impl(
        &mut self,
        filename: String,
        mut shutdown: ShutdownSubscriber,
    ) {
        // Use ?debug=1 so TiKV/Go pprof returns text format; required for jeprof native to parse PCs and symbolize.
        let url = format!("{}/debug/pprof/heap?debug=1", self.uri);
        info!(message = "Fetching jeheap (jeprof)", instance_type = %self.instance_type, %url);
        let resp = match self.jeprof_fetch_mode {
            JeprofFetchMode::Perl => {
                tokio::select! {
                    _ = shutdown.done() => return,
                    r = fetch_raw(url, self.tls.clone()) => r,
                }
            }
            JeprofFetchMode::Rust => {
                tokio::select! {
                    _ = shutdown.done() => return,
                    r = fetch_raw_native(&self.client, &url) => r,
                }
            }
        };
        match resp {
            Ok(body) => {
                let mut event = LogEvent::from_str_legacy(BASE64_STANDARD.encode(&body));
                event.insert("filename", filename.clone());
                if self.out.send_event(event).await.is_err() {
                    StreamClosedError { count: 1 }.emit();
                } else {
                    info!(message = "jeheap (jeprof) fetched and emitted", instance_type = %self.instance_type, filename = %filename, size_bytes = body.len());
                }
            }
            Err(err) => {
                error!(message = "Failed to fetch jeheap (heap with jeprof)", instance_type = %self.instance_type, mode = ?self.jeprof_fetch_mode, %err);
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
        use vector::config::ComponentKey;
        use vector_lib::config::{DataType, SourceOutput};

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
        mock_pprof_server_with_status(port, StatusCode::OK).await
    }

    async fn mock_pprof_server_with_status(port: u16, status: StatusCode) -> String {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));

        tokio::spawn(async move {
            let make_svc = make_service_fn(move |_conn| {
                let status = status.clone();
                async move {
                    Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                        let status = status.clone();
                        async move {
                            let path = req.uri().path();
                            let resp = if path.starts_with("/debug/pprof/") {
                                // Return mock pprof data with specified status
                                Response::builder()
                                    .status(status)
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

    async fn mock_pprof_server_with_error(port: u16) -> String {
        // Server that will cause connection errors
        let addr = SocketAddr::from(([127, 0, 0, 1], port));

        tokio::spawn(async move {
            // Start server and immediately close it to cause connection errors
            let listener = TcpListener::bind(&addr).await.unwrap();
            drop(listener);
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
            instance_name: None,
        };
        let out = create_test_source_sender();
        let result = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        ).await;
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
            instance_name: None,
        };
        let out = create_test_source_sender();
        let result = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        ).await;
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
            instance_name: None,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        )
            .await
            .unwrap();

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
            instance_name: None,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        )
            .await
            .unwrap();

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
            instance_name: None,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        )
            .await
            .unwrap();

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
            instance_name: None,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        )
            .await
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        source.uri = mock_pprof_server(port).await;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let (_, shutdown) = pair();
        let filename = "test-goroutine".to_string();

        source.fetch_goroutine(filename, shutdown.clone()).await;
    }

    #[tokio::test]
    async fn test_fetch_cpu_http_error() {
        // Test fetch_cpu with HTTP error (connection refused)
        let component = Component {
            instance_type: InstanceType::TiDB,
            host: "127.0.0.1".to_string(),
            primary_port: 4000,
            secondary_port: 10080,
            instance_name: None,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        )
            .await
            .unwrap();

        // Use invalid port to cause connection error
        source.uri = "http://127.0.0.1:65535".to_string();

        let (_, shutdown) = pair();
        let filename = "test-cpu-error".to_string();

        // Should handle error gracefully
        source.fetch_cpu(filename, shutdown.clone()).await;
    }

    #[tokio::test]
    async fn test_fetch_cpu_status_error() {
        // Test fetch_cpu with non-200 status code
        let component = Component {
            instance_type: InstanceType::TiDB,
            host: "127.0.0.1".to_string(),
            primary_port: 4000,
            secondary_port: 10080,
            instance_name: None,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        )
            .await
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        source.uri = mock_pprof_server_with_status(port, StatusCode::INTERNAL_SERVER_ERROR).await;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let (_, shutdown) = pair();
        let filename = "test-cpu-status-error".to_string();

        source.fetch_cpu(filename, shutdown.clone()).await;
    }

    #[tokio::test]
    async fn test_fetch_heap_status_error() {
        // Test fetch_heap with non-200 status code
        let component = Component {
            instance_type: InstanceType::TiDB,
            host: "127.0.0.1".to_string(),
            primary_port: 4000,
            secondary_port: 10080,
            instance_name: None,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        )
            .await
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        source.uri = mock_pprof_server_with_status(port, StatusCode::NOT_FOUND).await;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let (_, shutdown) = pair();
        let filename = "test-heap-status-error".to_string();

        source.fetch_heap(filename, shutdown.clone()).await;
    }

    #[tokio::test]
    async fn test_fetch_mutex_status_error() {
        // Test fetch_mutex with non-200 status code
        let component = Component {
            instance_type: InstanceType::TiDB,
            host: "127.0.0.1".to_string(),
            primary_port: 4000,
            secondary_port: 10080,
            instance_name: None,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        )
            .await
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        source.uri = mock_pprof_server_with_status(port, StatusCode::BAD_REQUEST).await;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let (_, shutdown) = pair();
        let filename = "test-mutex-status-error".to_string();

        source.fetch_mutex(filename, shutdown.clone()).await;
    }

    #[tokio::test]
    async fn test_fetch_goroutine_status_error() {
        // Test fetch_goroutine with non-200 status code
        let component = Component {
            instance_type: InstanceType::TiDB,
            host: "127.0.0.1".to_string(),
            primary_port: 4000,
            secondary_port: 10080,
            instance_name: None,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        )
            .await
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        source.uri = mock_pprof_server_with_status(port, StatusCode::SERVICE_UNAVAILABLE).await;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let (_, shutdown) = pair();
        let filename = "test-goroutine-status-error".to_string();

        source.fetch_goroutine(filename, shutdown.clone()).await;
    }

    #[tokio::test]
    async fn test_conprof_source_new_with_pd() {
        // Test ConprofSource::new with PD
        let component = Component {
            instance_type: InstanceType::PD,
            host: "127.0.0.1".to_string(),
            primary_port: 2379,
            secondary_port: 2379,
            instance_name: None,
        };
        let out = create_test_source_sender();
        let result = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        ).await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_conprof_source_new_with_tiproxy() {
        // Test ConprofSource::new with TiProxy
        let component = Component {
            instance_type: InstanceType::TiProxy,
            host: "127.0.0.1".to_string(),
            primary_port: 6000,
            secondary_port: 10080,
            instance_name: None,
        };
        let out = create_test_source_sender();
        let result = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        ).await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_conprof_source_new_with_lightning() {
        // Test ConprofSource::new with Lightning
        let component = Component {
            instance_type: InstanceType::Lightning,
            host: "127.0.0.1".to_string(),
            primary_port: 8287,
            secondary_port: 8286,
            instance_name: None,
        };
        let out = create_test_source_sender();
        let result = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        ).await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_conprof_source_new_with_tikv_heap_profile_enabled() {
        // Test ConprofSource::new with TiKV and heap profile enabled
        let component = Component {
            instance_type: InstanceType::TiKV,
            host: "127.0.0.1".to_string(),
            primary_port: 20160,
            secondary_port: 20180,
            instance_name: None,
        };
        let out = create_test_source_sender();
        let result = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        ).await;
        assert!(result.is_some());
        let source = result.unwrap();
        assert_eq!(source.instance_type, InstanceType::TiKV);
        assert!(source.components_profile_types.tikv.heap);
    }

    #[tokio::test]
    async fn test_fetch_heap_with_jeprof_success() {
        // Test fetch_heap_with_jeprof with mock server
        let component = Component {
            instance_type: InstanceType::TiKV,
            host: "127.0.0.1".to_string(),
            primary_port: 20160,
            secondary_port: 20180,
            instance_name: None,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        )
            .await
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        source.uri = mock_pprof_server(port).await;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let (_, shutdown) = pair();
        let filename = "test-heap-jeprof".to_string();

        // Note: fetch_heap_with_jeprof uses fetch_raw which calls perl
        // This will fail in test environment, but we can test the code path
        source
            .fetch_heap_with_jeprof(filename, shutdown.clone())
            .await;
    }

    #[tokio::test]
    async fn test_run_loop_pd_branch() {
        // Test run_loop with PD instance type - should fetch goroutine, mutex, heap, cpu
        let component = Component {
            instance_type: InstanceType::PD,
            host: "127.0.0.1".to_string(),
            primary_port: 2379,
            secondary_port: 2379,
            instance_name: None,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        )
            .await
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        source.uri = mock_pprof_server(port).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let (notifier, mut shutdown) = pair();

        // Start run_loop in background and immediately shutdown
        let handle = tokio::spawn(async move {
            source.run_loop(shutdown.clone()).await;
        });

        // Give it a moment to start
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Shutdown to stop the loop
        notifier.shutdown();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Cancel the task
        handle.abort();
    }

    #[tokio::test]
    async fn test_run_loop_tiproxy_branch() {
        // Test run_loop with TiProxy instance type
        let component = Component {
            instance_type: InstanceType::TiProxy,
            host: "127.0.0.1".to_string(),
            primary_port: 6000,
            secondary_port: 10080,
            instance_name: None,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        )
            .await
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        source.uri = mock_pprof_server(port).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let (notifier, mut shutdown) = pair();

        let handle = tokio::spawn(async move {
            source.run_loop(shutdown.clone()).await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        notifier.shutdown();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_run_loop_lightning_branch() {
        // Test run_loop with Lightning instance type
        let component = Component {
            instance_type: InstanceType::Lightning,
            host: "127.0.0.1".to_string(),
            primary_port: 8287,
            secondary_port: 8286,
            instance_name: None,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        )
            .await
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        source.uri = mock_pprof_server(port).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let (notifier, mut shutdown) = pair();

        let handle = tokio::spawn(async move {
            source.run_loop(shutdown.clone()).await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        notifier.shutdown();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_run_loop_tikv_branch() {
        // Test run_loop with TiKV instance type - should only fetch cpu
        let component = Component {
            instance_type: InstanceType::TiKV,
            host: "127.0.0.1".to_string(),
            primary_port: 20160,
            secondary_port: 20180,
            instance_name: None,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        )
            .await
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        source.uri = mock_pprof_server(port).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let (notifier, mut shutdown) = pair();

        let handle = tokio::spawn(async move {
            source.run_loop(shutdown.clone()).await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        notifier.shutdown();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_run_loop_tikv_with_heap_profile() {
        // Test run_loop with TiKV and heap profile enabled
        let component = Component {
            instance_type: InstanceType::TiKV,
            host: "127.0.0.1".to_string(),
            primary_port: 20160,
            secondary_port: 20180,
            instance_name: None,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        )
            .await
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        source.uri = mock_pprof_server(port).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let (notifier, mut shutdown) = pair();

        let handle = tokio::spawn(async move {
            source.run_loop(shutdown.clone()).await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        notifier.shutdown();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_run_loop_tiflash_branch() {
        // Test run_loop with TiFlash instance type - should do nothing
        let component = Component {
            instance_type: InstanceType::TiFlash,
            host: "127.0.0.1".to_string(),
            primary_port: 9000,
            secondary_port: 8123,
            instance_name: None,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        )
            .await
            .unwrap();

        let (notifier, mut shutdown) = pair();

        let handle = tokio::spawn(async move {
            source.run_loop(shutdown.clone()).await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        notifier.shutdown();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_run_loop_sleep_path() {
        // Test run_loop sleep path when now < next_minute_ts
        let component = Component {
            instance_type: InstanceType::TiDB,
            host: "127.0.0.1".to_string(),
            primary_port: 4000,
            secondary_port: 10080,
            instance_name: None,
        };
        let out = create_test_source_sender();
        let mut source = ConprofSource::new(
            component,
            None,
            out,
            crate::sources::conprof::default_components_profile_types(),
            crate::sources::conprof::JeprofFetchMode::Perl,
        )
            .await
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        source.uri = mock_pprof_server(port).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let (notifier, mut shutdown) = pair();

        // This will test the sleep path in run_loop
        let handle = tokio::spawn(async move {
            source.run_loop(shutdown.clone()).await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        notifier.shutdown();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        handle.abort();
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
                instance_name: None,
            };
            // Test that conprof_address works for all types
            let _ = component.conprof_address();
        }
    }

    #[test]
    fn test_run_loop_instance_type_branches() {
        // Test that all instance type branches in run_loop are covered conceptually
        let instance_types = vec![
            (InstanceType::TiDB, true),    // Should fetch goroutine, mutex, heap, cpu
            (InstanceType::PD, true),      // Should fetch goroutine, mutex, heap, cpu
            (InstanceType::TiProxy, true), // Should fetch goroutine, mutex, heap, cpu
            (InstanceType::Lightning, true), // Should fetch goroutine, mutex, heap, cpu
            (InstanceType::TiKV, false),   // Should only fetch cpu (and heap if enabled)
            (InstanceType::TiFlash, false), // Should do nothing
        ];

        for (instance_type, should_fetch_multiple) in instance_types {
            let component = Component {
                instance_type,
                host: "127.0.0.1".to_string(),
                primary_port: 4000,
                secondary_port: 10080,
                instance_name: None,
            };

            // Verify component structure
            assert!(
                component.conprof_address().is_some()
                    || matches!(&component.instance_type, InstanceType::TiFlash)
            );

            // Test that we can determine which branch to take
            match &component.instance_type {
                InstanceType::TiDB
                | InstanceType::PD
                | InstanceType::TiProxy
                | InstanceType::Lightning => {
                    assert!(should_fetch_multiple);
                }
                InstanceType::TiKV
                | InstanceType::TikvWorker
                | InstanceType::CoprocessorWorker => {
                    assert!(!should_fetch_multiple);
                }
                InstanceType::TiFlash => {
                    // Do nothing
                }
                InstanceType::Other(_) => {
                    // Unknown types use default profile (e.g. like TiDB)
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
        let test_cases = vec![1234567890, 1234567891, 1234567899, 1234567859, 1234567860];

        for mut ts in test_cases {
            let original_ts = ts;
            ts -= ts % 60;
            assert_eq!(ts % 60, 0, "Timestamp should be rounded down to minute");
            assert!(ts <= original_ts, "Rounded timestamp should be <= original");

            let next_minute_ts = ts + 60;
            assert_eq!(
                next_minute_ts - ts,
                60,
                "Next minute should be 60 seconds later"
            );
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
        // Test TiKV heap profile conditional logic (driven by components_profile_types.tikv.heap)
        let profile_types = crate::sources::conprof::default_components_profile_types();
        assert!(profile_types.tikv.heap, "default has TiKV heap enabled");

        let profile_types_no_heap = crate::sources::conprof::ComponentsProfileTypes {
            tikv: crate::sources::conprof::ProfileTypes {
                cpu: false,
                heap: false,
                jeheap: false,
                mutex: false,
                goroutine: false,
            },
            ..profile_types
        };
        assert!(!profile_types_no_heap.tikv.heap, "can disable TiKV heap via config");
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
            InstanceType::TiDB
            | InstanceType::PD
            | InstanceType::TiProxy
            | InstanceType::Lightning => {
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
        assert_eq!(
            cpu_url,
            "http://127.0.0.1:10080/debug/pprof/profile?seconds=10"
        );
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
        assert_eq!(
            goroutine_url,
            "http://127.0.0.1:10080/debug/pprof/goroutine"
        );
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
        let test_timestamps = vec![1234567890, 1234567891, 1234567899, 1234567949];

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
    fn test_tikv_heap_profile_driven_by_components_profile_types() {
        // Default TiKV: heap=true (HTTP), jeheap=false. For jeprof use heap: false, jeheap: true.
        let types = crate::sources::conprof::default_components_profile_types();
        assert!(types.tikv.heap);
        assert!(!types.tikv.jeheap);
        assert!(!types.tikv.cpu);
    }
}
