#[macro_use]
extern crate tracing;

mod config;
mod controller;
mod shutdown;
mod topology;
mod upstream;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}

// #[cfg(test)]
// mod tests {
//     use std::fs::read;
//     use std::net::SocketAddr;

//     use self::{
//         config::{default_retry_delay, TopSQLPubSubConfig},
//         consts::{INSTANCE_TYPE_TIDB, INSTANCE_TYPE_TIKV},
//         upstream::{
//             tidb::mock_upstream::MockTopSqlPubSubServer,
//             tikv::mock_upstream::MockResourceMeteringPubSubServer,
//         },
//     };
//     use super::*;
//     use vector::test_util::{
//         components::{run_and_assert_source_compliance, SOURCE_TAGS},
//         next_addr,
//     };
//     use vector::tls::{
//         TEST_PEM_CA_PATH, TEST_PEM_CLIENT_CRT_PATH, TEST_PEM_CLIENT_KEY_PATH, TEST_PEM_CRT_PATH,
//         TEST_PEM_KEY_PATH,
//     };

//     use tonic::transport::{Certificate, Identity, ServerTlsConfig};

//     #[tokio::test]
//     async fn test_topsql_scrape_tidb() {
//         let address = next_addr();
//         let config = TopSQLPubSubConfig {
//             instance: address.to_string(),
//             instance_type: INSTANCE_TYPE_TIDB.to_owned(),
//             tls: None,
//             retry_delay_seconds: default_retry_delay(),
//         };

//         check_topsql_scrape_tidb(address, config, None).await;
//     }

//     #[tokio::test]
//     async fn test_topsql_scrape_tidb_tls() {
//         let address = next_addr();
//         let config = TopSQLPubSubConfig {
//             instance: format!("localhost:{}", address.port()),
//             instance_type: INSTANCE_TYPE_TIDB.to_owned(),
//             tls: Some(TlsConfig {
//                 ca_file: Some(TEST_PEM_CA_PATH.into()),
//                 crt_file: Some(TEST_PEM_CLIENT_CRT_PATH.into()),
//                 key_file: Some(TEST_PEM_CLIENT_KEY_PATH.into()),
//                 ..Default::default()
//             }),
//             retry_delay_seconds: default_retry_delay(),
//         };

//         let ca = read(TEST_PEM_CA_PATH).unwrap();
//         let crt = read(TEST_PEM_CRT_PATH).unwrap();
//         let key = read(TEST_PEM_KEY_PATH).unwrap();

//         let tls_config = ServerTlsConfig::default()
//             .client_ca_root(Certificate::from_pem(ca))
//             .identity(Identity::from_pem(crt, key));

//         check_topsql_scrape_tidb(address, config, Some(tls_config)).await;
//     }

//     #[tokio::test]
//     async fn test_topsql_scrape_tikv() {
//         let address = next_addr();
//         let config = TopSQLPubSubConfig {
//             instance: address.to_string(),
//             instance_type: INSTANCE_TYPE_TIKV.to_owned(),
//             tls: None,
//             retry_delay_seconds: default_retry_delay(),
//         };

//         check_topsql_scrape_tikv(address, config, None).await;
//     }

//     #[tokio::test]
//     async fn test_topsql_scrape_tikv_tls() {
//         let address = next_addr();
//         let config = TopSQLPubSubConfig {
//             instance: format!("localhost:{}", address.port()),
//             instance_type: INSTANCE_TYPE_TIKV.to_owned(),
//             tls: Some(TlsConfig {
//                 ca_file: Some(TEST_PEM_CA_PATH.into()),
//                 crt_file: Some(TEST_PEM_CLIENT_CRT_PATH.into()),
//                 key_file: Some(TEST_PEM_CLIENT_KEY_PATH.into()),
//                 ..Default::default()
//             }),
//             retry_delay_seconds: default_retry_delay(),
//         };

//         let ca = read(TEST_PEM_CA_PATH).unwrap();
//         let crt = read(TEST_PEM_CRT_PATH).unwrap();
//         let key = read(TEST_PEM_KEY_PATH).unwrap();

//         let tls_config = ServerTlsConfig::default()
//             .client_ca_root(Certificate::from_pem(ca))
//             .identity(Identity::from_pem(crt, key));

//         check_topsql_scrape_tikv(address, config, Some(tls_config)).await;
//     }

//     async fn check_topsql_scrape_tidb(
//         address: SocketAddr,
//         config: TopSQLPubSubConfig,
//         tls_config: Option<ServerTlsConfig>,
//     ) {
//         tokio::spawn(MockTopSqlPubSubServer::run(address, tls_config));

//         // wait for server to set up
//         tokio::time::sleep(Duration::from_secs(1)).await;

//         let events =
//             run_and_assert_source_compliance(config, Duration::from_secs(1), &SOURCE_TAGS).await;
//         assert!(!events.is_empty());
//     }

//     async fn check_topsql_scrape_tikv(
//         address: SocketAddr,
//         config: TopSQLPubSubConfig,
//         tls_config: Option<ServerTlsConfig>,
//     ) {
//         tokio::spawn(MockResourceMeteringPubSubServer::run(address, tls_config));

//         // wait for server to set up
//         tokio::time::sleep(Duration::from_secs(1)).await;

//         let events =
//             run_and_assert_source_compliance(config, Duration::from_secs(1), &SOURCE_TAGS).await;
//         assert!(!events.is_empty());
//     }
// }
