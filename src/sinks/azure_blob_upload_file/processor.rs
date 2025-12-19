use std::collections::HashSet;
use std::io;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use azure_storage_blobs::prelude::*;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use tokio_util::time::DelayQueue;
use vector_lib::{
    event::Event,
    finalization::{EventStatus, Finalizable},
    internal_event::{CountByteSize, EventsSent, InternalEventHandle},
    register,
    sink::StreamSink,
};

use crate::common::checkpointer::{Checkpointer, UploadKey};
use crate::sinks::azure_blob_upload_file::uploader::AzureBlobUploader;

pub struct AzureBlobUploadFileSink {
    pub client: Arc<ContainerClient>,
    pub container_name: String,
    pub delay_upload: Duration,
    pub expire_after: Duration,
    pub checkpointer: Checkpointer,
}

impl AzureBlobUploadFileSink {
    pub fn new(
        client: Arc<ContainerClient>,
        container_name: String,
        delay_upload: Duration,
        expire_after: Duration,
        checkpointer: Checkpointer,
    ) -> Self {
        Self {
            client,
            container_name,
            delay_upload,
            expire_after,
            checkpointer,
        }
    }

    async fn file_modified_time(filename: &str) -> io::Result<SystemTime> {
        tokio::fs::metadata(filename).await?.modified()
    }
}

#[async_trait::async_trait]
impl StreamSink<Event> for AzureBlobUploadFileSink {
    async fn run(self: Box<Self>, mut input: BoxStream<'_, Event>) -> Result<(), ()> {
        let Self {
            client,
            container_name,
            delay_upload,
            expire_after,
            mut checkpointer,
        } = *self;

        let mut delay_queue = DelayQueue::new();
        let mut pending_uploads = HashSet::new();
        let mut uploader = AzureBlobUploader::new(client);

        loop {
            tokio::select! {
                event = input.next() => {
                    let mut event = if let Some(event) = event {
                        event
                    } else {
                        break;
                    };

                    let finalizers = event.take_finalizers();
                    if let Some(upload_key) = UploadKey::from_event(&event, &container_name) {
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
                                message = "Failed to upload file to Azure Blob.",
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
