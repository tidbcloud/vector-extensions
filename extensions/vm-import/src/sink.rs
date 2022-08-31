use std::io::Write;

use bytes::{BufMut, Bytes, BytesMut};
use flate2::write::GzEncoder;
use flate2::Compression;
use http::{Request, Uri};
use vector::sinks::util::http::HttpSink;
use vector::sinks::util::{BoxedRawValue, PartitionInnerBuffer};
use vector::template::Template;

use crate::encoder::VMImportSinkEventEncoder;
use crate::partition::PartitionKey;

#[derive(Clone)]
pub struct VMImportSink {
    endpoint_template: Template,
}

impl VMImportSink {
    pub const fn new(endpoint_template: Template) -> Self {
        Self { endpoint_template }
    }
}

#[async_trait::async_trait]
impl HttpSink for VMImportSink {
    type Input = PartitionInnerBuffer<serde_json::Value, PartitionKey>;
    type Output = PartitionInnerBuffer<Vec<BoxedRawValue>, PartitionKey>;
    type Encoder = VMImportSinkEventEncoder;

    fn build_encoder(&self) -> Self::Encoder {
        VMImportSinkEventEncoder::new(self.endpoint_template.clone())
    }

    async fn build_request(&self, output: Self::Output) -> vector::Result<Request<Bytes>> {
        let (events, key) = output.into_parts();

        let uri = key.endpoint.parse::<Uri>()?;

        let buffer = BytesMut::new();
        let mut w = GzEncoder::new(buffer.writer(), Compression::default());

        for event in events {
            w.write_all(event.get().as_bytes())?;
            w.write_all(b"\n")?;
        }
        let body = w.finish()?.into_inner().freeze();

        let builder = Request::post(uri).header("Content-Encoding", "gzip");
        let request = builder.body(body).unwrap();

        Ok(request)
    }
}
