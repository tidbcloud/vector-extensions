use std::collections::HashMap;
use std::num::NonZeroUsize;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::stream::BoxStream;
use futures_util::StreamExt;
use vector_lib::{
    event::Event,
    finalization::{EventStatus, Finalizable},
    internal_event::{CountByteSize, EventsSent, InternalEventHandle},
    register,
    sink::StreamSink,
};

/// Key for partitioning: (component, hour_partition).
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct PartitionKey {
    component: String,
    hour_partition: String,
}

/// Per-partition buffer and next part index.
struct PartitionBuffer {
    buf: Vec<u8>,
    part_index: u64,
}

pub struct S3ContentPartitionedSink {
    client: S3Client,
    bucket: String,
    key_prefix: String,
    max_file_bytes: NonZeroUsize,
    compression_gzip: bool,
}

impl S3ContentPartitionedSink {
    pub fn new(
        client: S3Client,
        bucket: String,
        key_prefix: String,
        max_file_bytes: NonZeroUsize,
        compression_gzip: bool,
    ) -> Self {
        Self {
            client,
            bucket,
            key_prefix,
            max_file_bytes,
            compression_gzip,
        }
    }

    fn key_from_event(log: &vector_lib::event::LogEvent) -> Option<PartitionKey> {
        let component = log.get("component").and_then(|v| v.as_str())?.to_string();
        let hour_partition = log.get("hour_partition").and_then(|v| v.as_str())?.to_string();
        Some(PartitionKey {
            component,
            hour_partition,
        })
    }

    fn message_bytes(log: &vector_lib::event::LogEvent) -> Option<Vec<u8>> {
        let msg = log.get("message").and_then(|v| v.as_str())?;
        let mut bytes = msg.as_bytes().to_vec();
        if !bytes.is_empty() && *bytes.last().unwrap() != b'\n' {
            bytes.push(b'\n');
        }
        Some(bytes)
    }

    fn object_key(key_prefix: &str, component: &str, hour_partition: &str, part_index: u64, gzip: bool) -> String {
        let ext = if gzip { "log.gz" } else { "log" };
        let prefix = key_prefix.trim_end_matches('/');
        format!("{}/{}/{}/part-{:05}.{}", prefix, component, hour_partition, part_index, ext)
    }

    async fn flush_partition(
        client: &S3Client,
        bucket: &str,
        key_prefix: &str,
        key: &PartitionKey,
        data: &[u8],
        part_index: u64,
        compression_gzip: bool,
    ) -> std::io::Result<usize> {
        if data.is_empty() {
            return Ok(0);
        }
        let body = if compression_gzip {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            std::io::Write::write_all(&mut encoder, data)?;
            encoder.finish()?
        } else {
            data.to_vec()
        };
        let len = body.len();
        let object_key = Self::object_key(key_prefix, &key.component, &key.hour_partition, part_index, compression_gzip);
        client
            .put_object()
            .bucket(bucket)
            .key(&object_key)
            .body(ByteStream::from(body))
            .set_content_type(Some(if compression_gzip { "application/gzip" } else { "text/plain" }.to_string()))
            .set_content_encoding(if compression_gzip { Some("gzip".to_string()) } else { None })
            .send()
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        Ok(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vector_lib::event::{LogEvent, Value};
    use bytes::Bytes;

    // -- object_key --

    #[test]
    fn test_object_key_no_gzip() {
        let key = S3ContentPartitionedSink::object_key("loki", "tidb", "2026010804", 0, false);
        assert_eq!(key, "loki/tidb/2026010804/part-00000.log");
    }

    #[test]
    fn test_object_key_with_gzip() {
        let key = S3ContentPartitionedSink::object_key("loki", "tidb", "2026010804", 3, true);
        assert_eq!(key, "loki/tidb/2026010804/part-00003.log.gz");
    }

    #[test]
    fn test_object_key_trailing_slash_prefix() {
        let key = S3ContentPartitionedSink::object_key("loki/", "tidb", "2026010804", 0, false);
        assert_eq!(key, "loki/tidb/2026010804/part-00000.log");
    }

    #[test]
    fn test_object_key_large_part_index() {
        let key = S3ContentPartitionedSink::object_key("prefix", "comp", "hour", 99999, true);
        assert_eq!(key, "prefix/comp/hour/part-99999.log.gz");
    }

    // -- key_from_event --

    #[test]
    fn test_key_from_event_valid() {
        let mut log = LogEvent::default();
        log.insert("component", Value::Bytes(Bytes::from("tidb")));
        log.insert("hour_partition", Value::Bytes(Bytes::from("2026010804")));
        let key = S3ContentPartitionedSink::key_from_event(&log).unwrap();
        assert_eq!(key.component, "tidb");
        assert_eq!(key.hour_partition, "2026010804");
    }

    #[test]
    fn test_key_from_event_missing_component() {
        let mut log = LogEvent::default();
        log.insert("hour_partition", Value::Bytes(Bytes::from("2026010804")));
        assert!(S3ContentPartitionedSink::key_from_event(&log).is_none());
    }

    #[test]
    fn test_key_from_event_missing_hour_partition() {
        let mut log = LogEvent::default();
        log.insert("component", Value::Bytes(Bytes::from("tidb")));
        assert!(S3ContentPartitionedSink::key_from_event(&log).is_none());
    }

    #[test]
    fn test_key_from_event_missing_both() {
        let log = LogEvent::default();
        assert!(S3ContentPartitionedSink::key_from_event(&log).is_none());
    }

    // -- message_bytes --

    #[test]
    fn test_message_bytes_with_newline() {
        let mut log = LogEvent::default();
        log.insert("message", Value::Bytes(Bytes::from("hello world\n")));
        let bytes = S3ContentPartitionedSink::message_bytes(&log).unwrap();
        assert_eq!(bytes, b"hello world\n");
    }

    #[test]
    fn test_message_bytes_without_newline() {
        let mut log = LogEvent::default();
        log.insert("message", Value::Bytes(Bytes::from("hello world")));
        let bytes = S3ContentPartitionedSink::message_bytes(&log).unwrap();
        assert_eq!(bytes, b"hello world\n");
    }

    #[test]
    fn test_message_bytes_missing() {
        let log = LogEvent::default();
        assert!(S3ContentPartitionedSink::message_bytes(&log).is_none());
    }

    // -- PartitionKey equality --

    #[test]
    fn test_partition_key_equality() {
        let k1 = PartitionKey {
            component: "tidb".to_string(),
            hour_partition: "2026010804".to_string(),
        };
        let k2 = PartitionKey {
            component: "tidb".to_string(),
            hour_partition: "2026010804".to_string(),
        };
        let k3 = PartitionKey {
            component: "tikv".to_string(),
            hour_partition: "2026010804".to_string(),
        };
        assert_eq!(k1, k2);
        assert_ne!(k1, k3);
    }
}

#[async_trait::async_trait]
impl StreamSink<Event> for S3ContentPartitionedSink {
    async fn run(self: Box<Self>, mut input: BoxStream<'_, Event>) -> Result<(), ()> {
        let Self {
            client,
            bucket,
            key_prefix,
            max_file_bytes,
            compression_gzip,
        } = *self;

        let mut buffers: HashMap<PartitionKey, PartitionBuffer> = HashMap::new();

        while let Some(mut event) = input.next().await {
            let log = event.as_mut_log();

            let partition_key = match Self::key_from_event(log) {
                Some(k) => k,
                None => {
                    event.take_finalizers().update_status(EventStatus::Rejected);
                    continue;
                }
            };

            let message_bytes = match Self::message_bytes(log) {
                Some(b) => b,
                None => {
                    event.take_finalizers().update_status(EventStatus::Rejected);
                    continue;
                }
            };

            let entry = buffers
                .entry(partition_key.clone())
                .or_insert_with(|| PartitionBuffer {
                    buf: Vec::new(),
                    part_index: 0,
                });

            entry.buf.extend(&message_bytes);

            while entry.buf.len() >= max_file_bytes.get() {
                let part_index = entry.part_index;
                entry.part_index += 1;
                let rest = entry.buf.split_off(max_file_bytes.get());
                let to_upload = std::mem::replace(&mut entry.buf, rest);
                match Self::flush_partition(
                    &client,
                    &bucket,
                    &key_prefix,
                    &partition_key,
                    &to_upload,
                    part_index,
                    compression_gzip,
                )
                .await
                {
                    Ok(uploaded) => {
                        info!(
                            message = "Uploaded partitioned object.",
                            bucket = %bucket,
                            component = %partition_key.component,
                            hour_partition = %partition_key.hour_partition,
                            part = part_index,
                            bytes = uploaded,
                        );
                        register!(EventsSent { output: None }).emit(CountByteSize(1, uploaded.into()));
                    }
                    Err(e) => {
                        error!(
                            message = "Failed to upload partitioned object.",
                            bucket = %bucket,
                            component = %partition_key.component,
                            hour_partition = %partition_key.hour_partition,
                            part = part_index,
                            error = %e,
                        );
                        let mut full = to_upload;
                        full.extend(entry.buf.drain(..));
                        entry.buf = full;
                        event.take_finalizers().update_status(EventStatus::Rejected);
                        continue;
                    }
                }
            }

            event.take_finalizers().update_status(EventStatus::Delivered);
        }

        // Flush remaining buffers
        for (key, state) in buffers {
            if state.buf.is_empty() {
                continue;
            }
            let part_index = state.part_index;
            match Self::flush_partition(
                &client,
                &bucket,
                &key_prefix,
                &key,
                &state.buf,
                part_index,
                compression_gzip,
            )
            .await
            {
                Ok(uploaded) => {
                    info!(
                        message = "Uploaded final partitioned object.",
                        bucket = %bucket,
                        component = %key.component,
                        hour_partition = %key.hour_partition,
                        part = part_index,
                        bytes = uploaded,
                    );
                    register!(EventsSent { output: None }).emit(CountByteSize(1, uploaded.into()));
                }
                Err(e) => {
                    error!(
                        message = "Failed to upload final partitioned object.",
                        bucket = %bucket,
                        component = %key.component,
                        hour_partition = %key.hour_partition,
                        part = part_index,
                        error = %e,
                    );
                }
            }
        }

        Ok(())
    }
}
