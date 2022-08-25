use std::io::Write;

use bytes::{BufMut, Bytes, BytesMut};
use flate2::write::GzEncoder;
use flate2::Compression;
use http::{Request, Uri};
use serde_json::Value;
use vector::sinks::util::http::HttpSink;
use vector::sinks::util::BoxedRawValue;

use crate::encoder::VMImportSinkEventEncoder;

#[derive(Clone)]
pub struct VMImportSink {
    endpoint: Uri,
}

impl VMImportSink {
    pub const fn new(endpoint: Uri) -> Self {
        Self { endpoint }
    }
}

#[async_trait::async_trait]
impl HttpSink for VMImportSink {
    type Input = Value;
    type Output = Vec<BoxedRawValue>;
    type Encoder = VMImportSinkEventEncoder;

    fn build_encoder(&self) -> Self::Encoder {
        VMImportSinkEventEncoder
    }

    async fn build_request(&self, events: Self::Output) -> vector::Result<Request<Bytes>> {
        let buffer = BytesMut::new();
        let mut w = GzEncoder::new(buffer.writer(), Compression::default());

        for event in events {
            w.write_all(event.get().as_bytes())?;
            w.write_all(b"\n")?;
        }
        let body = w.finish()?.into_inner().freeze();

        let builder = Request::post(self.endpoint.clone()).header("Content-Encoding", "gzip");
        let request = builder.body(body).unwrap();

        Ok(request)
    }
}
