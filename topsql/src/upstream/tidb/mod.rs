mod parser;
pub mod proto;

#[cfg(test)]
pub mod mock_upstream;

use tonic::{
    transport::{Channel, Endpoint},
    Status, Streaming,
};

use super::{tls_proxy, Upstream};
use crate::shutdown::ShutdownSubscriber;

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
            .subscribe(proto::TopSqlSubRequest {})
            .await
            .map(|r| r.into_inner())
    }
}
