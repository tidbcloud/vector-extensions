mod parser;
mod proto;

#[cfg(test)]
pub mod mock_upstream;

use std::time::Duration;

use tonic::codec::CompressionEncoding;
use tonic::transport::{Channel, Endpoint};
use tonic::{Status, Streaming};

use crate::sources::topsql_v2::shutdown::ShutdownSubscriber;
use crate::sources::topsql_v2::upstream::{tls_proxy, Upstream};
use crate::sources::topsql_v2::TopRUConfig;

pub struct TiKVUpstream;

#[async_trait::async_trait]
impl Upstream for TiKVUpstream {
    type Client = proto::resource_metering_pub_sub_client::ResourceMeteringPubSubClient<Channel>;
    type UpstreamEvent = proto::ResourceUsageRecord;
    type UpstreamEventParser = parser::ResourceUsageRecordParser;

    async fn build_endpoint(
        address: String,
        tls_config: Option<&vector::tls::TlsConfig>,
        shutdown_subscriber: ShutdownSubscriber,
    ) -> vector::Result<Endpoint> {
        // Initialize rustls CryptoProvider before using tonic
        crate::utils::rustls::init_rustls();
        
        let endpoint = if tls_config.is_none() {
            Channel::from_shared(address.clone())?
                .http2_keep_alive_interval(Duration::from_secs(300))
                .keep_alive_timeout(Duration::from_secs(10))
                .keep_alive_while_idle(true)
        } else {
            // do proxy
            let port = tls_proxy::tls_proxy(tls_config, &address, shutdown_subscriber).await?;
            Channel::from_shared(format!("http://127.0.0.1:{}", port))?
                .http2_keep_alive_interval(Duration::from_secs(300))
                .keep_alive_timeout(Duration::from_secs(10))
                .keep_alive_while_idle(true)
        };

        Ok(endpoint)
    }

    fn build_client(channel: Channel) -> Self::Client {
        Self::Client::new(channel).accept_compressed(CompressionEncoding::Gzip)
    }

    async fn build_stream(
        mut client: Self::Client,
        _topru_config: Option<&TopRUConfig>,
    ) -> Result<Streaming<Self::UpstreamEvent>, Status> {
        let _ = _topru_config; // TiKV does not use TopRU config
        client
            .subscribe(proto::ResourceMeteringRequest {})
            .await
            .map(|r| r.into_inner())
    }
}
