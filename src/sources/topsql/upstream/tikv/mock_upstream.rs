#![allow(dead_code)]

use std::net::SocketAddr;
use std::pin::Pin;

use futures::Stream;
use futures_util::stream;
use prost::Message;
use tonic::transport::ServerTlsConfig;
use tonic::{Request, Response, Status};

use crate::sources::topsql::upstream::tidb::proto::ResourceGroupTag;
use crate::sources::topsql::upstream::tikv::proto::resource_metering_pub_sub_server::{
    ResourceMeteringPubSub, ResourceMeteringPubSubServer,
};
use crate::sources::topsql::upstream::tikv::proto::resource_usage_record::RecordOneof;
use crate::sources::topsql::upstream::tikv::proto::{
    GroupTagRecord, GroupTagRecordItem, ResourceMeteringRequest, ResourceUsageRecord,
};

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
                        table_id: Some(1),
                        keyspace_name: None,
                    }
                    .encode_to_vec(),
                    items: vec![GroupTagRecordItem {
                        timestamp_sec: 1655363650,
                        cpu_time_ms: 10,
                        read_keys: 20,
                        write_keys: 30,
                        network_in_bytes: 0,
                        network_out_bytes: 0,
                        logical_read_bytes: 0,
                        logical_write_bytes: 0,
                        rocksdb_block_read_count: 0,
                    }],
                })),
            })])) as Self::SubscribeStream,
        ))
    }
}
