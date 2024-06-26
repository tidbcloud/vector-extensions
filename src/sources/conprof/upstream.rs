use std::time::Duration;

use base64::{prelude::*, Engine};
use chrono::Utc;
use reqwest::{Certificate, Client, Identity};
use vector::{internal_events::StreamClosedError, SourceSender};
use vector_lib::{
    internal_event::InternalEvent,
    tls::TlsConfig,
    {event::LogEvent, tls::TlsSettings},
};

use crate::sources::conprof::{
    shutdown::ShutdownSubscriber,
    tools::fetch_raw,
    topology::{Component, InstanceType},
};

pub struct ConprofSource {
    client: Client,
    // instance: String,
    instance_b64: String,
    instance_type: InstanceType,
    uri: String,

    tls: Option<TlsConfig>,
    out: SourceSender,
    // init_retry_delay: Duration,
    // retry_delay: Duration,
}

impl ConprofSource {
    pub async fn new(
        component: Component,
        tls: Option<TlsConfig>,
        out: SourceSender,
        // init_retry_delay: Duration,
    ) -> Option<Self> {
        let mut builder = reqwest::Client::builder();
        if let Some(tls) = tls.clone() {
            let ca_file = tls.ca_file.clone().expect("tls ca file must be provided");
            let ca = match tokio::fs::read(ca_file).await {
                Ok(v) => v,
                Err(err) => {
                    error!(message = "Failed to read tls ca file", error = %err);
                    return None;
                }
            };
            let settings = TlsSettings::from_options(&Some(tls)).expect("invalid tls settings");
            let (crt, key) = settings.identity_pem().expect("invalid identity pem");
            builder = builder
                .add_root_certificate(Certificate::from_pem(&ca).expect("invalid ca"))
                .identity(Identity::from_pkcs8_pem(&crt, &key).expect("invalid crt & key"));
        }
        let client = match builder
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                error!(message = "Failed to build reqwest client", %err);
                return None;
            }
        };
        match component.conprof_address() {
            Some(address) => Some(ConprofSource {
                client,
                // instance: address.clone(),
                instance_b64: BASE64_URL_SAFE_NO_PAD.encode(&address),
                instance_type: component.instance_type,
                uri: if tls.is_some() {
                    format!("https://{}", address)
                } else {
                    format!("http://{}", address)
                },

                tls,
                out,
                // init_retry_delay,
                // retry_delay: init_retry_delay,
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
                InstanceType::TiDB | InstanceType::PD => {
                    self.fetch_goroutine(
                        format!(
                            "{}-{}-goroutine-{}",
                            ts, self.instance_type, self.instance_b64
                        ),
                        shutdown.clone(),
                    )
                    .await;
                    self.fetch_mutex(
                        format!("{}-{}-mutex-{}", ts, self.instance_type, self.instance_b64),
                        shutdown.clone(),
                    )
                    .await;
                    self.fetch_heap(
                        format!("{}-{}-heap-{}", ts, self.instance_type, self.instance_b64),
                        shutdown.clone(),
                    )
                    .await;
                    self.fetch_cpu(
                        format!("{}-{}-cpu-{}", ts, self.instance_type, self.instance_b64),
                        shutdown.clone(),
                    )
                    .await;
                }
                InstanceType::TiKV => {
                    self.fetch_cpu(
                        format!("{}-{}-cpu-{}", ts, self.instance_type, self.instance_b64),
                        shutdown.clone(),
                    )
                    .await;
                    self.fetch_heap_with_jeprof(
                        format!("{}-{}-heap-{}", ts, self.instance_type, self.instance_b64),
                        shutdown.clone(),
                    )
                    .await;
                }
                InstanceType::TiFlash => {
                    // do nothing.
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
