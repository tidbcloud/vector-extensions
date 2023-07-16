use std::collections::HashSet;
use std::io;
use std::time::{Duration, SystemTime};

use common::checkpointer::{Checkpointer, UploadKey};
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use tokio_util::time::DelayQueue;
use vector::gcp::GcpAuthenticator;
use vector::http::HttpClient;
use vector::register;
use vector_common::finalization::{EventStatus, Finalizable};
use vector_common::internal_event::{CountByteSize, EventsSent, InternalEventHandle};
use vector_core::event::Event;
use vector_core::sink::StreamSink;

use crate::uploader::{GCSUploader, RequestSettings};

pub struct GcsUploadFileSink {
    client: HttpClient,
    bucket: String,
    auth: GcpAuthenticator,
    delay_upload: Duration,
    expire_after: Duration,
    checkpointer: Checkpointer,
    request_settings: RequestSettings,
}

impl GcsUploadFileSink {
    pub const fn new(
        client: HttpClient,
        bucket: String,
        auth: GcpAuthenticator,
        delay_upload: Duration,
        expire_after: Duration,
        checkpointer: Checkpointer,
        request_settings: RequestSettings,
    ) -> Self {
        Self {
            client,
            bucket,
            auth,
            delay_upload,
            expire_after,
            checkpointer,
            request_settings,
        }
    }

    async fn file_modified_time(filename: &str) -> io::Result<SystemTime> {
        tokio::fs::metadata(filename).await?.modified()
    }
}

#[async_trait::async_trait]
impl StreamSink<Event> for GcsUploadFileSink {
    async fn run(self: Box<Self>, mut input: BoxStream<'_, Event>) -> Result<(), ()> {
        let Self {
            client,
            bucket,
            auth,
            delay_upload,
            expire_after,
            mut checkpointer,
            request_settings,
        } = *self;

        let mut delay_queue = DelayQueue::new();
        let mut pending_uploads = HashSet::new();
        let mut uploader = GCSUploader::new(client, auth, request_settings);

        loop {
            tokio::select! {
                event = input.next() => {
                    let mut event = if let Some(event) = event {
                        event
                    } else {
                        break;
                    };

                    let finalizers = event.take_finalizers();
                    if let Some(upload_key) = UploadKey::from_event(&event, &bucket) {
                        let modified_time = match Self::file_modified_time(&upload_key.filename).await {
                            Ok(modified_time) => modified_time,
                            Err(err) => {
                                finalizers.update_status(EventStatus::Rejected);
                                error!(message = "Failed to get file modified time.", %err);
                                continue;
                            }
                        };

                        if !checkpointer.contains(&upload_key, modified_time) && !pending_uploads.contains(&upload_key) {
                            delay_queue.insert((upload_key.clone(), finalizers), delay_upload);
                            pending_uploads.insert(upload_key);
                        } else {
                            finalizers.update_status(EventStatus::Delivered);
                        }
                    } else {
                        finalizers.update_status(EventStatus::Rejected);
                    }
                }

                entry = delay_queue.next(), if !delay_queue.is_empty() => {
                    let (upload_key, finalizers) = if let Some(entry) = entry {
                        entry.into_inner()
                    } else {
                        // DelayQueue returns None if the queue is exhausted,
                        // however we disable the DelayQueue branch if there are
                        // no items in the queue.
                        unreachable!("an empty DelayQueue is never polled");
                    };
                    pending_uploads.remove(&upload_key);

                    let upload_time = SystemTime::now();
                    match uploader.upload(&upload_key).await {
                        Ok(response) => {
                            if response.count > 0 {
                                info!(
                                    message = "Uploaded file.",
                                    filename = %upload_key.filename,
                                    bucket = %upload_key.bucket,
                                    key = %upload_key.object_key,
                                    size = %response.events_byte_size,
                                );
                            }
                            finalizers.update_status(EventStatus::Delivered);
                            register!(EventsSent {
                                output: None,
                            }).emit(CountByteSize(response.count, response.events_byte_size.into()));
                            checkpointer.update(upload_key, upload_time, expire_after);
                        }
                        Err(error) => {
                            error!(
                                message = "Failed to upload file to GCS.",
                                %error,
                                filename = %upload_key.filename,
                                bucket = %upload_key.bucket,
                                key = %upload_key.object_key,
                            );
                            finalizers.update_status(EventStatus::Rejected);
                        }
                    }
                    match checkpointer.write_checkpoints() {
                        Ok(count) => trace!(message = "Checkpoints written", %count),
                        Err(error) => error!(message = "Failed to write checkpoints.", %error),
                    }
                }
            }
        }

        Ok(())
    }
}
