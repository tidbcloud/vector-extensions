pub mod parser;
pub mod tidb;
pub mod tikv;

mod consts;
mod tls_proxy;
mod utils;

use std::time::Duration;

use futures::StreamExt;
use tokio::time;
use tokio_stream::wrappers::IntervalStream;
use tonic::transport::{Channel, Endpoint};
use vector::internal_events::StreamClosedError;
use vector::{register, SourceSender};
use vector_common::byte_size_of::ByteSizeOf;
use vector_common::internal_event::{
    ByteSize, BytesReceived, CountByteSize, EventsReceived, InternalEvent, InternalEventHandle,
};
use vector_core::tls::TlsConfig;

use crate::shutdown::ShutdownSubscriber;
use crate::topology::{Component, InstanceType};
use crate::upstream::parser::UpstreamEventParser;
use crate::upstream::tidb::TiDBUpstream;
use crate::upstream::tikv::TiKVUpstream;
use crate::upstream::utils::instance_event;

#[async_trait::async_trait]
pub trait Upstream: Send {
    type Client: Send;
    type UpstreamEvent: ByteSizeOf + Send;
    type UpstreamEventParser: parser::UpstreamEventParser<UpstreamEvent = Self::UpstreamEvent>;

    async fn build_endpoint(
        address: String,
        tls_config: &Option<vector::tls::TlsConfig>,
        shutdown_subscriber: ShutdownSubscriber,
    ) -> vector::Result<Endpoint>;

    fn build_client(channel: Channel) -> Self::Client;

    async fn build_stream(
        client: Self::Client,
    ) -> Result<tonic::codec::Streaming<Self::UpstreamEvent>, tonic::Status>;
}

pub struct TopSQLSource {
    instance: String,
    instance_type: InstanceType,
    uri: String,

    tls: Option<TlsConfig>,
    protocal: String,
    out: SourceSender,

    init_retry_delay: Duration,
    retry_delay: Duration,
    top_n: usize,
}

enum State {
    RetryNow,
    RetryDelay,
}

const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);

impl TopSQLSource {
    pub fn new(
        component: Component,
        tls: Option<TlsConfig>,
        out: SourceSender,
        init_retry_delay: Duration,
        top_n: usize,
    ) -> Option<Self> {
        let protocal = if tls.is_none() {
            "http".into()
        } else {
            "https".into()
        };
        match component.topsql_address() {
            Some(address) => Some(TopSQLSource {
                instance: address.clone(),
                instance_type: component.instance_type,
                uri: if tls.is_some() {
                    format!("https://{}", address)
                } else {
                    format!("http://{}", address)
                },

                tls,
                protocal,
                out,
                init_retry_delay,
                retry_delay: init_retry_delay,
                top_n,
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

    async fn run_loop(&mut self, shutdown_subscriber: ShutdownSubscriber) {
        loop {
            let shutdown_subscriber = shutdown_subscriber.clone();
            let state = match self.instance_type {
                InstanceType::TiDB => self.run_once::<TiDBUpstream>(shutdown_subscriber).await,
                InstanceType::TiKV => self.run_once::<TiKVUpstream>(shutdown_subscriber).await,
                _ => unreachable!(),
            };

            match state {
                State::RetryNow => debug!("Retrying immediately."),
                State::RetryDelay => {
                    self.retry_delay *= 2;
                    if self.retry_delay > MAX_RETRY_DELAY {
                        self.retry_delay = MAX_RETRY_DELAY;
                    }
                    info!(
                        timeout_secs = self.retry_delay.as_secs_f64(),
                        "Retrying after timeout."
                    );
                    time::sleep(self.retry_delay).await;
                }
            }
        }
    }

    async fn run_once<U: Upstream>(&mut self, shutdown_subscriber: ShutdownSubscriber) -> State {
        let response_stream = self.build_stream::<U>(shutdown_subscriber).await;
        let mut response_stream = match response_stream {
            Ok(stream) => stream,
            Err(state) => return state,
        };
        self.on_connected();

        let mut tick_stream = IntervalStream::new(time::interval(Duration::from_secs(1)));
        let mut instance_stream = IntervalStream::new(time::interval(Duration::from_secs(30)));
        let mut responses = vec![];
        let mut last_event_recv_ts = chrono::Local::now().timestamp();
        loop {
            tokio::select! {
                response = response_stream.next() => {
                    match response {
                        Some(Ok(response)) => {
                            register!(BytesReceived {
                                protocol: self.protocal.clone().into(),
                            })
                            .emit(ByteSize(response.size_of()));
                            responses.push(response);
                            last_event_recv_ts = chrono::Local::now().timestamp();
                        },
                        Some(Err(error)) => {
                            error!(message = "Failed to fetch events.", error = %error);
                            break State::RetryDelay;
                        },
                        None => break State::RetryNow,
                    }
                }
                _ = tick_stream.next() => {
                    if chrono::Local::now().timestamp() > last_event_recv_ts + 10 {
                        if !responses.is_empty() {
                            self.handle_responses::<U>(responses).await;
                            responses = vec![];
                        }
                    }
                }
                _ = instance_stream.next() => self.handle_instance().await,
            }
        }
    }

    async fn build_stream<U: Upstream>(
        &self,
        shutdown_subscriber: ShutdownSubscriber,
    ) -> Result<tonic::codec::Streaming<U::UpstreamEvent>, State> {
        let endpoint = U::build_endpoint(self.uri.clone(), &self.tls, shutdown_subscriber).await;
        let endpoint = match endpoint {
            Ok(endpoint) => endpoint,
            Err(error) => {
                error!(message = "Failed to build endpoint.", error = %error);
                return Err(State::RetryDelay);
            }
        };

        let channel = endpoint.connect().await;
        let channel = match channel {
            Ok(channel) => channel,
            Err(error) => {
                error!(message = "Failed to connect to the server.", error = %error);
                return Err(State::RetryDelay);
            }
        };

        let client = U::build_client(channel);
        let response_stream = match U::build_stream(client).await {
            Ok(stream) => stream,
            Err(error) => {
                error!(message = "Failed to set up subscription.", error = %error);
                return Err(State::RetryDelay);
            }
        };

        Ok(response_stream)
    }

    async fn handle_responses<U: Upstream>(&mut self, responses: Vec<U::UpstreamEvent>) {
        let responses = if self.top_n > 0 {
            U::UpstreamEventParser::keep_top_n(responses, self.top_n)
        } else {
            responses
        };
        let mut batch = vec![];
        for response in responses {
            let mut events = U::UpstreamEventParser::parse(response, self.instance.clone());
            batch.append(&mut events);
        }
        let count = batch.len();
        register!(EventsReceived {}).emit(CountByteSize(count, batch.size_of().into()));
        if self.out.send_batch(batch).await.is_err() {
            StreamClosedError { count }.emit()
        }
    }

    async fn handle_instance(&mut self) {
        let event = instance_event(self.instance.clone(), self.instance_type.to_string());
        if self.out.send_event(event).await.is_err() {
            StreamClosedError { count: 1 }.emit();
        }
    }

    fn on_connected(&mut self) {
        self.retry_delay = self.init_retry_delay;
        info!("Connected to the upstream.");
    }
}
