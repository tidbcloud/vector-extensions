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
