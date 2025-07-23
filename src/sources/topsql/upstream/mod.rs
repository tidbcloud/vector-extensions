
pub mod parser;
pub mod tidb;
pub mod tikv;

mod consts;
mod tls_proxy;
mod utils;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tokio::time;
use tokio_stream::wrappers::IntervalStream;
use tonic::transport::{Channel, Endpoint};
use vector::{internal_events::StreamClosedError, SourceSender};
use vector_lib::{
    byte_size_of::ByteSizeOf,
    internal_event::{CountByteSize, EventsReceived, InternalEvent, InternalEventHandle},
    register,
    tls::TlsConfig,
};

use crate::sources::topsql::{
    schema_cache::SchemaCache,
    shutdown::ShutdownSubscriber,
    topology::{Component, InstanceType},
    upstream::{
        parser::UpstreamEventParser, tidb::TiDBUpstream, tikv::TiKVUpstream,
    },
};

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

// Legacy TopSQL source
pub struct LegacyTopSQLSource {
    instance: String,
    instance_type: InstanceType,
    _uri: String,
    tls: Option<TlsConfig>,
    _protocal: String,
    out: SourceSender,
    init_retry_delay: Duration,
    retry_delay: Duration,
    top_n: usize,
    downsampling_interval: u32,
    schema_cache: Arc<SchemaCache>,
}

impl LegacyTopSQLSource {
    pub fn new(
        component: Component,
        tls: Option<TlsConfig>,
        out: SourceSender,
        init_retry_delay: Duration,
        top_n: usize,
        downsampling_interval: u32,
        schema_cache: Arc<SchemaCache>,
    ) -> Option<Self> {
        let protocal = if tls.is_none() {
            "http".into()
        } else {
            "https".into()
        };
        match component.topsql_address() {
            Some(address) => Some(LegacyTopSQLSource {
                instance: address.clone(),
                instance_type: component.instance_type,
                _uri: if tls.is_some() {
                    format!("https://{}", address)
                } else {
                    format!("http://{}", address)
                },
                tls,
                _protocal: protocal,
                out,
                init_retry_delay,
                retry_delay: init_retry_delay,
                top_n,
                downsampling_interval,
                schema_cache,
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
                State::RetryDelay => {
                    self.retry_delay *= 2;
                    if self.retry_delay > MAX_RETRY_DELAY {
                        self.retry_delay = MAX_RETRY_DELAY;
                    }
                    debug!("Retrying after delay: {:?}", self.retry_delay);
                    tokio::time::sleep(self.retry_delay).await;
                }
            }
        }
    }

    async fn run_once<U: Upstream>(&mut self, shutdown_subscriber: ShutdownSubscriber) -> State {
        let stream = match self.build_stream::<U>(shutdown_subscriber).await {
            Ok(stream) => stream,
            Err(State::RetryDelay) => return State::RetryDelay,
        };

        self.on_connected();

        let mut stream = stream;
        let mut responses = Vec::new();
        let mut interval = IntervalStream::new(time::interval(Duration::from_secs(1)));

        loop {
            tokio::select! {
                response = stream.next() => {
                    match response {
                        Some(Ok(response)) => {
                            responses.push(response);
                        }
                        Some(Err(status)) => {
                            error!(message = "Stream error", %status);
                            return State::RetryDelay;
                        }
                        None => {
                            error!(message = "Stream ended");
                            return State::RetryDelay;
                        }
                    }
                }
                _ = interval.next() => {
                    if !responses.is_empty() {
                        self.handle_responses::<U>(responses).await;
                        responses = Vec::new();
                    }
                }
            }
        }
    }

    async fn build_stream<U: Upstream>(
        &self,
        shutdown_subscriber: ShutdownSubscriber,
    ) -> Result<tonic::codec::Streaming<U::UpstreamEvent>, State> {
        let endpoint = match U::build_endpoint(
            self.instance.clone(),
            &self.tls,
            shutdown_subscriber.clone(),
        )
        .await
        {
            Ok(endpoint) => endpoint,
            Err(error) => {
                error!(message = "Failed to build endpoint", %error);
                return Err(State::RetryDelay);
            }
        };

        let channel = match endpoint.connect().await {
            Ok(channel) => channel,
            Err(error) => {
                error!(message = "Failed to connect", %error);
                return Err(State::RetryDelay);
            }
        };

        let client = U::build_client(channel);
        match U::build_stream(client).await {
            Ok(stream) => Ok(stream),
            Err(status) => {
                error!(message = "Failed to build stream", %status);
                Err(State::RetryDelay)
            }
        }
    }

    async fn handle_responses<U: Upstream>(&mut self, responses: Vec<U::UpstreamEvent>) {
        // truncate top n
        let mut responses = if self.top_n > 0 {
            U::UpstreamEventParser::keep_top_n(responses, self.top_n)
        } else {
            responses
        };
        // downsample
        if self.downsampling_interval > 1 {
            U::UpstreamEventParser::downsampling(&mut responses, self.downsampling_interval);
        }
        // parse
        let mut batch = vec![];
        for response in responses {
            let mut events = U::UpstreamEventParser::parse(
                response,
                self.instance.clone(),
                self.schema_cache.clone(),
            );
            batch.append(&mut events);
        }
        // send
        let count = batch.len();
        register!(EventsReceived {}).emit(CountByteSize(count, batch.size_of().into()));
        if self.out.send_batch(batch).await.is_err() {
            StreamClosedError { count }.emit()
        }
    }



    fn on_connected(&mut self) {
        self.retry_delay = self.init_retry_delay;
        info!("Connected to the upstream.");
    }
}

// Nextgen TopSQL source
pub struct NextgenTopSQLSource {
    instance: String,
    instance_type: InstanceType,
    _uri: String,
    tls: Option<TlsConfig>,
    _protocal: String,
    out: SourceSender,
    init_retry_delay: Duration,
    retry_delay: Duration,
    top_n: usize,
    downsampling_interval: u32,
    schema_cache: Arc<SchemaCache>,
    _keyspace_to_vmtenants: HashMap<String, (String, String)>,
}

impl NextgenTopSQLSource {
    pub fn new(
        component: Component,
        tls: Option<TlsConfig>,
        out: SourceSender,
        init_retry_delay: Duration,
        top_n: usize,
        downsampling_interval: u32,
        schema_cache: Arc<SchemaCache>,
        _keyspace_to_vmtenants: HashMap<String, (String, String)>,
    ) -> Option<Self> {
        let protocal = if tls.is_none() {
            "http".into()
        } else {
            "https".into()
        };
        match component.topsql_address() {
            Some(address) => Some(NextgenTopSQLSource {
                instance: address.clone(),
                instance_type: component.instance_type,
                _uri: if tls.is_some() {
                    format!("https://{}", address)
                } else {
                    format!("http://{}", address)
                },
                tls,
                _protocal: protocal,
                out,
                init_retry_delay,
                retry_delay: init_retry_delay,
                top_n,
                downsampling_interval,
                schema_cache,
                _keyspace_to_vmtenants: _keyspace_to_vmtenants,
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
                State::RetryDelay => {
                    self.retry_delay *= 2;
                    if self.retry_delay > MAX_RETRY_DELAY {
                        self.retry_delay = MAX_RETRY_DELAY;
                    }
                    debug!("Retrying after delay: {:?}", self.retry_delay);
                    tokio::time::sleep(self.retry_delay).await;
                }
            }
        }
    }

    async fn run_once<U: Upstream>(&mut self, shutdown_subscriber: ShutdownSubscriber) -> State {
        let stream = match self.build_stream::<U>(shutdown_subscriber).await {
            Ok(stream) => stream,
            Err(State::RetryDelay) => return State::RetryDelay,
        };

        self.on_connected();

        let mut stream = stream;
        let mut responses = Vec::new();
        let mut interval = IntervalStream::new(time::interval(Duration::from_secs(1)));

        loop {
            tokio::select! {
                response = stream.next() => {
                    match response {
                        Some(Ok(response)) => {
                            responses.push(response);
                        }
                        Some(Err(status)) => {
                            error!(message = "Stream error", %status);
                            return State::RetryDelay;
                        }
                        None => {
                            error!(message = "Stream ended");
                            return State::RetryDelay;
                        }
                    }
                }
                _ = interval.next() => {
                    if !responses.is_empty() {
                        self.handle_responses::<U>(responses).await;
                        responses = Vec::new();
                    }
                }
            }
        }
    }

    async fn build_stream<U: Upstream>(
        &self,
        shutdown_subscriber: ShutdownSubscriber,
    ) -> Result<tonic::codec::Streaming<U::UpstreamEvent>, State> {
        let endpoint = match U::build_endpoint(
            self.instance.clone(),
            &self.tls,
            shutdown_subscriber.clone(),
        )
        .await
        {
            Ok(endpoint) => endpoint,
            Err(error) => {
                error!(message = "Failed to build endpoint", %error);
                return Err(State::RetryDelay);
            }
        };

        let channel = match endpoint.connect().await {
            Ok(channel) => channel,
            Err(error) => {
                error!(message = "Failed to connect", %error);
                return Err(State::RetryDelay);
            }
        };

        let client = U::build_client(channel);
        match U::build_stream(client).await {
            Ok(stream) => Ok(stream),
            Err(status) => {
                error!(message = "Failed to build stream", %status);
                Err(State::RetryDelay)
            }
        }
    }

    async fn handle_responses<U: Upstream>(&mut self, responses: Vec<U::UpstreamEvent>) {
        // truncate top n
        let mut responses = if self.top_n > 0 {
            U::UpstreamEventParser::keep_top_n(responses, self.top_n)
        } else {
            responses
        };
        // downsample
        if self.downsampling_interval > 1 {
            U::UpstreamEventParser::downsampling(&mut responses, self.downsampling_interval);
        }
        // parse
        let mut batch = vec![];
        for response in responses {
            let mut events = U::UpstreamEventParser::parse(
                response,
                self.instance.clone(),
                self.schema_cache.clone(),
            );
            batch.append(&mut events);
        }
        // send
        let count = batch.len();
        register!(EventsReceived {}).emit(CountByteSize(count, batch.size_of().into()));
        if self.out.send_batch(batch).await.is_err() {
            StreamClosedError { count }.emit()
        }
    }



    fn on_connected(&mut self) {
        self.retry_delay = self.init_retry_delay;
        info!("Connected to the upstream.");
    }
}

// Public interface that abstracts over both implementations
pub enum TopSQLSource {
    Legacy(LegacyTopSQLSource),
    Nextgen(NextgenTopSQLSource),
}

impl TopSQLSource {
    pub fn new(
        component: Component,
        tls: Option<TlsConfig>,
        out: SourceSender,
        init_retry_delay: Duration,
        top_n: usize,
        downsampling_interval: u32,
        schema_cache: Arc<SchemaCache>,
        _keyspace_to_vmtenants: HashMap<String, (String, String)>,
    ) -> Option<Self> {
        use crate::common::features::is_nextgen_mode;

        if is_nextgen_mode() {
            let source = NextgenTopSQLSource::new(
                component,
                tls,
                out,
                init_retry_delay,
                top_n,
                downsampling_interval,
                schema_cache,
                _keyspace_to_vmtenants,
            )?;
            Some(TopSQLSource::Nextgen(source))
        } else {
            let source = LegacyTopSQLSource::new(
                component,
                tls,
                out,
                init_retry_delay,
                top_n,
                downsampling_interval,
                schema_cache,
            )?;
            Some(TopSQLSource::Legacy(source))
        }
    }

    pub async fn run(self, shutdown: ShutdownSubscriber) {
        match self {
            TopSQLSource::Legacy(source) => source.run(shutdown).await,
            TopSQLSource::Nextgen(source) => source.run(shutdown).await,
        }
    }
}

enum State {
    RetryDelay,
}

const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);
