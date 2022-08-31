#[macro_use]
extern crate tracing;

mod config;
mod encoder;
mod partition;
mod sink;

// #[cfg(test)]
// mod tests {
//     use std::{collections::BTreeMap, future::ready};

//     use chrono::Utc;
//     use futures_util::{stream, TryFutureExt};
//     use http::Response;
//     use hyper::{
//         service::{make_service_fn, service_fn},
//         Server,
//     };
//     use lookup::path;
//     use vector_core::{event::LogEvent, Error};

//     use super::*;
//     use crate::{
//         config::SinkContext,
//         test_util::{
//             components::{run_and_assert_sink_compliance, HTTP_SINK_TAGS},
//             next_addr, test_generate_config,
//         },
//     };

//     #[test]
//     fn generate_config() {
//         test_generate_config::<VMImportConfig>();
//     }

//     #[tokio::test]
//     async fn send_event() {
//         let service =
//             service_fn(
//                 |_| async move { Ok::<_, hyper::Error>(Response::new(hyper::Body::empty())) },
//             );

//         let addr = next_addr();
//         let server = Server::bind(&addr)
//             .serve(make_service_fn(move |_| ready(Ok::<_, Error>(service))))
//             .map_err(|error| panic!("Server error: {}", error));

//         tokio::spawn(server);

//         let config = VMImportConfig {
//             endpoint: format!("http://{}", addr),
//             batch: Default::default(),
//             request: Default::default(),
//             tls: None,
//         };
//         let cx = SinkContext::new_test();
//         let (sink, _) = config.build(cx).await.unwrap();

//         let mut labels = value::Value::from(BTreeMap::default());
//         labels.insert("__name__", "cpu");
//         let mut timestamps = value::Value::from(Vec::<value::Value>::default());
//         timestamps.insert(path!(0), Utc::now());
//         let mut values = value::Value::from(Vec::<value::Value>::default());
//         values.insert(path!(0), 1.0);
//         let mut log = LogEvent::from_map(Default::default(), Default::default());
//         log.insert("labels", labels);
//         log.insert("timestamps", timestamps);
//         log.insert("values", values);
//         run_and_assert_sink_compliance(sink, stream::once(ready(log)), &HTTP_SINK_TAGS).await;
//     }
// }
