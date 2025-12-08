pub mod parser;
pub mod tidb;
pub mod tikv;

pub(crate) mod consts;
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
    internal_event::{
        ByteSize, BytesReceived, CountByteSize, EventsReceived, InternalEvent, InternalEventHandle,
    },
    register,
    tls::TlsConfig,
    event::{Event, LogEvent, Value as LogValue},
};
use chrono::Utc;
use crc32fast::Hasher as Crc32Hasher;

use crate::common::topology::{Component, InstanceType};
use crate::sources::topsql::{
    schema_cache::SchemaCache,
    shutdown::ShutdownSubscriber,
    upstream::{
        parser::UpstreamEventParser,
        tidb::TiDBUpstream,
        tikv::TiKVUpstream,
        utils::{instance_event, instance_event_metric, instance_event_with_tags},
    },
};

#[async_trait::async_trait]
pub trait Upstream: Send {
    type Client: Send;
    type UpstreamEvent: ByteSizeOf + Send;
    type UpstreamEventParser: parser::UpstreamEventParser<UpstreamEvent = Self::UpstreamEvent>;

    async fn build_endpoint(
        address: String,
        tls_config: Option<&vector::tls::TlsConfig>,
        shutdown_subscriber: ShutdownSubscriber,
    ) -> vector::Result<Endpoint>;

    fn build_client(channel: Channel) -> Self::Client;

    async fn build_stream(
        client: Self::Client,
    ) -> Result<tonic::codec::Streaming<Self::UpstreamEvent>, tonic::Status>;

    fn get_wait_seconds() -> u64;
}

// Common trait for TopSQL source behavior
#[async_trait::async_trait]
trait TopSQLSourceBehavior {
    async fn handle_instance_event(
        &self,
        instance: &str,
        instance_type: &str,
        enable_row_format: bool,
        out: &mut SourceSender,
    );
}

// Base TopSQL source with common functionality
struct BaseTopSQLSource {
    instance: String,
    instance_type: InstanceType,
    uri: String,
    tls: Option<TlsConfig>,
    protocal: String,
    out: SourceSender,
    init_retry_delay: Duration,
    retry_delay: Duration,
    top_n: usize,
    downsampling_interval: u32,
    schema_cache: Arc<SchemaCache>,
    enable_row_format: bool,
    partition_number: u32, // Only used when enable_row_format is true
    instance_partition_id: u32, // Only used when enable_row_format is true and partition_number > 0
}

impl BaseTopSQLSource {
    fn new(
        component: Component,
        tls: Option<TlsConfig>,
        out: SourceSender,
        init_retry_delay: Duration,
        top_n: usize,
        downsampling_interval: u32,
        schema_cache: Arc<SchemaCache>,
        enable_row_format: bool,
        partition_number: u32,
    ) -> Option<Self> {
        let protocal = if tls.is_none() {
            "http".into()
        } else {
            "https".into()
        };
        match component.topsql_address() {
            Some(address) => Some(BaseTopSQLSource {
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
                downsampling_interval,
                schema_cache,
                enable_row_format,
                partition_number,
                instance_partition_id: 0,
            }),
            None => None,
        }
    }

    async fn run<B: TopSQLSourceBehavior>(mut self, mut shutdown: ShutdownSubscriber, behavior: B) {
        let shutdown_subscriber = shutdown.clone();
        tokio::select! {
            _ = self.run_loop(shutdown_subscriber, &behavior) => {}
            _ = shutdown.done() => {}
        }
    }

    async fn run_loop<B: TopSQLSourceBehavior>(
        &mut self,
        shutdown_subscriber: ShutdownSubscriber,
        behavior: &B,
    ) {
        loop {
            let shutdown_subscriber = shutdown_subscriber.clone();
            let state = match self.instance_type {
                InstanceType::TiDB => {
                    self.run_once::<TiDBUpstream, B>(shutdown_subscriber, behavior)
                        .await
                }
                InstanceType::TiKV => {
                    self.run_once::<TiKVUpstream, B>(shutdown_subscriber, behavior)
                        .await
                }
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

    async fn run_once<U: Upstream, B: TopSQLSourceBehavior>(
        &mut self,
        shutdown_subscriber: ShutdownSubscriber,
        behavior: &B,
    ) -> State {
        let response_stream = self.build_stream::<U>(shutdown_subscriber).await;
        let mut response_stream = match response_stream {
            Ok(stream) => stream,
            Err(state) => return state,
        };
        self.on_connected().await;

        let mut tick_stream = IntervalStream::new(time::interval(Duration::from_secs(1)));
        let mut instance_stream = IntervalStream::new(time::interval(Duration::from_secs(30)));
        let mut responses = vec![];
        let mut responses_recv_ts_vec = vec![];
        info!(message = "Starting TopSQL source loop", instance = %self.instance, instance_type = %self.instance_type);
        let exit_state = loop {
            tokio::select! {
                response = response_stream.next() => {
                    match response {
                        Some(Ok(response)) => {
                            register!(BytesReceived {
                                protocol: self.protocal.clone().into(),
                            })
                            .emit(ByteSize(response.size_of()));
                            responses.push(response);
                            responses_recv_ts_vec.push(chrono::Local::now().timestamp());
                        },
                        Some(Err(error)) => {
                            error!(message = "Failed to fetch events.", error = %error);
                            break State::RetryDelay;
                        },
                        None => break State::RetryNow,
                    }
                }
                _ = tick_stream.next() => {
                    if !responses.is_empty() {
                        if chrono::Local::now().timestamp() > responses_recv_ts_vec[0] + U::get_wait_seconds() as i64 {
                            self.handle_responses::<U>(responses).await;
                            responses_recv_ts_vec.clear();
                            responses = vec![];
                        }
                    }
                }
                _ = instance_stream.next() => self.handle_instance(behavior).await,
            }
        };

        info!(message = "TopSQL source loop ended", instance = %self.instance, instance_type = %self.instance_type, exit_state = ?exit_state);
        exit_state
    }

    async fn build_stream<U: Upstream>(
        &self,
        shutdown_subscriber: ShutdownSubscriber,
    ) -> Result<tonic::codec::Streaming<U::UpstreamEvent>, State> {
        let endpoint =
            U::build_endpoint(self.uri.clone(), self.tls.as_ref(), shutdown_subscriber).await;
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
                self.enable_row_format,
                self.instance_partition_id,
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

    async fn handle_instance<B: TopSQLSourceBehavior>(&mut self, behavior: &B) {
        behavior
            .handle_instance_event(
                &self.instance,
                &self.instance_type.to_string(),
                self.enable_row_format,
                &mut self.out,
            )
            .await;
    }

    async fn on_connected(&mut self) {
        self.retry_delay = self.init_retry_delay;
        info!("Connected to the upstream.");

        if self.enable_row_format && self.partition_number > 0 {
            // Calculate CRC32 for (instance, instance_type)
            let instance_key = format!("{}_{}", self.instance, self.instance_type);
            let mut hasher = Crc32Hasher::new();
            hasher.update(instance_key.as_bytes());
            let crc_value = hasher.finalize();

            // Calculate partition by taking modulo
            // Use max(1, partition_number) to avoid division by zero
            let partition_number = if self.partition_number == 0 { 1 } else { self.partition_number };
            let instance_partition_id = (crc_value % partition_number as u32) as u32;
            self.instance_partition_id = instance_partition_id;

            // Create and send LogEvent with (instance, instance_type, partition_number)
            let mut event = Event::Log(LogEvent::default());
            let log = event.as_mut_log();
            log.insert("source_table", "instance_partition");
            log.insert("timestamps", LogValue::from(Utc::now().timestamp()));
            log.insert("instance", self.instance.clone());
            log.insert("instance_type", self.instance_type.to_string());
            log.insert("instance_partition_id", LogValue::from(instance_partition_id as i64));

            if self.out.send_event(event).await.is_err() {
                StreamClosedError { count: 1 }.emit();
            }
        }
    }
}

// Legacy TopSQL source behavior
struct LegacyTopSQLBehavior;

#[async_trait::async_trait]
impl TopSQLSourceBehavior for LegacyTopSQLBehavior {
    async fn handle_instance_event(
        &self,
        instance: &str,
        instance_type: &str,
        enable_row_format: bool,
        out: &mut SourceSender,
    ) {
        if !enable_row_format {
            let event = instance_event(instance.to_string(), instance_type.to_string());
            if out.send_event(event).await.is_err() {
                StreamClosedError { count: 1 }.emit();
            }            
        } else {
            let mut event = Event::Log(LogEvent::default());
            let log = event.as_mut_log();

            // Add metadata with Vector prefix (ensure all fields have values)
            log.insert("source_table", "instance");
            log.insert("timestamps", LogValue::from(Utc::now().timestamp()));
            log.insert("instance_type", instance_type.to_string());
            log.insert("instance", instance.to_string());

            if out.send_event(event).await.is_err() {
                StreamClosedError { count: 1 }.emit();
            }
        }
    }
}

// Legacy TopSQL source
pub struct LegacyTopSQLSource {
    base: BaseTopSQLSource,
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
        enable_row_format: bool,
        partition_number: u32,
    ) -> Option<Self> {
        let base = BaseTopSQLSource::new(
            component,
            tls,
            out,
            init_retry_delay,
            top_n,
            downsampling_interval,
            schema_cache,
            enable_row_format,
            partition_number,
        )?;
        Some(LegacyTopSQLSource { base })
    }

    pub async fn run(self, shutdown: ShutdownSubscriber) {
        let behavior = LegacyTopSQLBehavior;
        self.base.run(shutdown, behavior).await;
    }
}

// Nextgen TopSQL source behavior
struct NextgenTopSQLBehavior {
    keyspace_to_vmtenants: HashMap<String, (String, String)>,
}

#[async_trait::async_trait]
impl TopSQLSourceBehavior for NextgenTopSQLBehavior {
    async fn handle_instance_event(
        &self,
        instance: &str,
        instance_type: &str,
        enable_row_format: bool,
        out: &mut SourceSender,
    ) {
        let mut batch = vec![];
        if !enable_row_format {        
            let event = instance_event_metric(instance.to_string(), instance_type.to_string());
            batch.push(event);
            for (cluster_id, (vm_account_id, vm_project_id)) in &self.keyspace_to_vmtenants {
                let event = instance_event_with_tags(
                    instance.to_string(),
                    instance_type.to_string(),
                    cluster_id.clone(),
                    vm_account_id.clone(),
                    vm_project_id.clone(),
                );
                batch.push(event);
            }
            let count = batch.len();
            if out.send_batch(batch).await.is_err() {
                StreamClosedError { count }.emit()
            }
        } else {
            let mut event = Event::Log(LogEvent::default());
            let log = event.as_mut_log();

            // Add metadata with Vector prefix (ensure all fields have values)
            log.insert("source_table", "instance");
            log.insert("timestamps", LogValue::from(Utc::now().timestamp()));
            log.insert("instance_type", instance_type.to_string());
            log.insert("instance", instance.to_string());
            batch.push(event);

            for (cluster_id, (vm_account_id, vm_project_id)) in &self.keyspace_to_vmtenants {
                let mut event = Event::Log(LogEvent::default());
                let log = event.as_mut_log();
                log.insert("source_table", "instance");
                log.insert("timestamps", LogValue::from(Utc::now().timestamp()));
                log.insert("instance_type", instance_type.to_string());
                log.insert("instance", instance.to_string());
                log.insert("tidb_cluster_id", cluster_id.clone());
                log.insert("keyspace_name", cluster_id.clone());
                log.insert("vm_account_id", vm_account_id.clone());
                log.insert("vm_project_id", vm_project_id.clone());
                batch.push(event);
            }

            let count = batch.len();
            if out.send_batch(batch).await.is_err() {
                StreamClosedError { count }.emit()
            }
        }
    }
}

// Nextgen TopSQL source
pub struct NextgenTopSQLSource {
    base: BaseTopSQLSource,
    behavior: NextgenTopSQLBehavior,
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
        keyspace_to_vmtenants: HashMap<String, (String, String)>,
        enable_row_format: bool,
        partition_number: u32,
    ) -> Option<Self> {
        let base = BaseTopSQLSource::new(
            component,
            tls,
            out,
            init_retry_delay,
            top_n,
            downsampling_interval,
            schema_cache,
            enable_row_format,
            partition_number,
        )?;
        let behavior = NextgenTopSQLBehavior {
            keyspace_to_vmtenants,
        };
        Some(NextgenTopSQLSource { base, behavior })
    }

    pub async fn run(self, shutdown: ShutdownSubscriber) {
        self.base.run(shutdown, self.behavior).await;
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
        keyspace_to_vmtenants: HashMap<String, (String, String)>,
        enable_row_format: bool,
        partition_number: u32,
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
                keyspace_to_vmtenants,
                enable_row_format,
                partition_number,
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
                enable_row_format,
                partition_number,
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

#[derive(Debug)]
enum State {
    RetryNow,
    RetryDelay,
}

const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);
