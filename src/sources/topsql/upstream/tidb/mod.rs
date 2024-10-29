mod parser;
pub mod proto;

#[cfg(test)]
pub mod mock_upstream;

use std::time::Duration;

use tonic::codec::CompressionEncoding;
use tonic::transport::{Channel, Endpoint};
use tonic::{Status, Streaming};

use crate::sources::topsql::shutdown::ShutdownSubscriber;
use crate::sources::topsql::upstream::{tls_proxy, Upstream};

pub struct TiDBUpstream;

#[async_trait::async_trait]
impl Upstream for TiDBUpstream {
    type Client = proto::top_sql_pub_sub_client::TopSqlPubSubClient<Channel>;
    type UpstreamEvent = proto::TopSqlSubResponse;
    type UpstreamEventParser = parser::TopSqlSubResponseParser;

    async fn build_endpoint(
        address: String,
        tls_config: &Option<vector::tls::TlsConfig>,
        shutdown_subscriber: ShutdownSubscriber,
    ) -> vector::Result<Endpoint> {
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
    ) -> Result<Streaming<Self::UpstreamEvent>, Status> {
        client
            .subscribe(proto::TopSqlSubRequest {})
            .await
            .map(|r| r.into_inner())
    }
}
