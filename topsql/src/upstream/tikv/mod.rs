#![allow(warnings)]

mod parser;
mod proto;

#[cfg(test)]
pub mod mock_upstream;

use std::{future::Future, pin::Pin};

use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};
use tokio_openssl::SslStream;
use tonic::{
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity},
    Status, Streaming,
};
use tracing::Instrument;

use super::{tls_proxy, Upstream};
use vector::tls::{tls_connector_builder, MaybeTlsSettings, TlsConfig};

use crate::shutdown::ShutdownSubscriber;

pub struct TiKVUpstream;

#[async_trait::async_trait]
impl Upstream for TiKVUpstream {
    type Client = proto::resource_metering_pub_sub_client::ResourceMeteringPubSubClient<Channel>;
    type UpstreamEvent = proto::ResourceUsageRecord;
    type UpstreamEventParser = parser::ResourceUsageRecordParser;

    async fn build_endpoint(
        address: String,
        tls_config: &Option<vector::tls::TlsConfig>,
        shutdown_subscriber: ShutdownSubscriber,
    ) -> vector::Result<Endpoint> {
        let endpoint = if tls_config.is_none() {
            Channel::from_shared(address.clone())?
        } else {
            // do proxy
            let port = tls_proxy::tls_proxy(tls_config, &address, shutdown_subscriber).await?;
            Channel::from_shared(format!("http://127.0.0.1:{}", port))?
        };

        Ok(endpoint)
    }

    fn build_client(channel: Channel) -> Self::Client {
        Self::Client::new(channel)
    }

    async fn build_stream(
        mut client: Self::Client,
    ) -> Result<Streaming<Self::UpstreamEvent>, Status> {
        client
            .subscribe(proto::ResourceMeteringRequest {})
            .await
            .map(|r| r.into_inner())
    }
}
