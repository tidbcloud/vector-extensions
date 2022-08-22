use std::{net::SocketAddr, pin::Pin};

use futures::Stream;
use futures_util::stream;
use tonic::{transport::ServerTlsConfig, Request, Response, Status};

use super::proto::{
    top_sql_pub_sub_server::{TopSqlPubSub, TopSqlPubSubServer},
    top_sql_sub_response::RespOneof,
    PlanMeta, SqlMeta, TopSqlRecord, TopSqlRecordItem, TopSqlSubRequest, TopSqlSubResponse,
};

pub struct MockTopSqlPubSubServer;

impl MockTopSqlPubSubServer {
    pub async fn run(address: SocketAddr, tls_config: Option<ServerTlsConfig>) {
        let svc = TopSqlPubSubServer::new(Self);
        let mut sb = tonic::transport::Server::builder();
        if tls_config.is_some() {
            sb = sb.tls_config(tls_config.unwrap()).unwrap();
        }
        sb.add_service(svc).serve(address).await.unwrap();
    }
}

#[tonic::async_trait]
impl TopSqlPubSub for MockTopSqlPubSubServer {
    type SubscribeStream =
        Pin<Box<dyn Stream<Item = Result<TopSqlSubResponse, Status>> + Send + 'static>>;

    async fn subscribe(
        &self,
        _: Request<TopSqlSubRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let dump_record = TopSqlRecord {
            sql_digest: b"sql_digest".to_vec(),
            plan_digest: b"plan_digest".to_vec(),
            items: vec![TopSqlRecordItem {
                timestamp_sec: 1655363650,
                cpu_time_ms: 10,
                stmt_exec_count: 20,
                stmt_kv_exec_count: (vec![("127.0.0.1:20180".to_owned(), 10)])
                    .into_iter()
                    .collect(),
                stmt_duration_sum_ns: 30,
                stmt_duration_count: 20,
            }],
        };

        let dump_sql_meta = SqlMeta {
            sql_digest: b"sql_digest".to_vec(),
            normalized_sql: "sql_text".to_owned(),
            is_internal_sql: false,
        };

        let dump_plan_meta = PlanMeta {
            plan_digest: b"plan_digest".to_vec(),
            normalized_plan: "plan_text".to_owned(),
            encoded_normalized_plan: "encoded_plan".to_owned(),
        };

        Ok(Response::new(Box::pin(stream::iter(vec![
            Ok(TopSqlSubResponse {
                resp_oneof: Some(RespOneof::Record(dump_record)),
            }),
            Ok(TopSqlSubResponse {
                resp_oneof: Some(RespOneof::SqlMeta(dump_sql_meta)),
            }),
            Ok(TopSqlSubResponse {
                resp_oneof: Some(RespOneof::PlanMeta(dump_plan_meta)),
            }),
        ])) as Self::SubscribeStream))
    }
}
