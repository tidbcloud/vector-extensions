use std::{net::SocketAddr, pin::Pin};

use futures::Stream;
use futures_util::stream;
use prost::Message;
use tonic::{transport::ServerTlsConfig, Request, Response, Status};

use super::proto::{
    resource_metering_pub_sub_server::{ResourceMeteringPubSub, ResourceMeteringPubSubServer},
    resource_usage_record::RecordOneof,
    GroupTagRecord, GroupTagRecordItem, ResourceMeteringRequest, ResourceUsageRecord,
};
use vector::sources::topsql::upstream::tidb::proto::ResourceGroupTag;

pub struct MockResourceMeteringPubSubServer;

impl MockResourceMeteringPubSubServer {
    pub async fn run(address: SocketAddr, tls_config: Option<ServerTlsConfig>) {
        let svc = ResourceMeteringPubSubServer::new(Self);
        let mut sb = tonic::transport::Server::builder();
        if tls_config.is_some() {
            sb = sb.tls_config(tls_config.unwrap()).unwrap();
        }
        sb.add_service(svc).serve(address).await.unwrap();
    }
}

#[tonic::async_trait]
impl ResourceMeteringPubSub for MockResourceMeteringPubSubServer {
    type SubscribeStream =
        Pin<Box<dyn Stream<Item = Result<ResourceUsageRecord, Status>> + Send + 'static>>;

    async fn subscribe(
        &self,
        _: Request<ResourceMeteringRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        Ok(Response::new(
            Box::pin(stream::iter(vec![Ok(ResourceUsageRecord {
                record_oneof: Some(RecordOneof::Record(GroupTagRecord {
                    resource_group_tag: ResourceGroupTag {
                        sql_digest: Some(b"sql_digest".to_vec()),
                        plan_digest: Some(b"plan_digest".to_vec()),
                        label: Some(1),
                    }
                    .encode_to_vec(),
                    items: vec![GroupTagRecordItem {
                        timestamp_sec: 1655363650,
                        cpu_time_ms: 10,
                        read_keys: 20,
                        write_keys: 30,
                    }],
                })),
            })])) as Self::SubscribeStream,
        ))
    }
}
