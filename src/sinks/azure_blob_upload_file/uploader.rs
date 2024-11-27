use std::io;
use std::sync::Arc;

use azure_storage_blobs::prelude::*;
use bytes::Bytes;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::common::checkpointer::UploadKey;

// limit the chunk size to 8MB to avoid OOM
const AZURE_BLOB_UPLOAD_CHUNK_SIZE: usize = 8 * 1024 * 1024;

pub struct AzureBlobUploader {
    client: Arc<ContainerClient>,
}

pub struct UploadResponse {
    pub count: usize,
    pub events_byte_size: usize,
}

impl AzureBlobUploader {
    pub fn new(client: Arc<ContainerClient>) -> Self {
        Self { client }
    }

    pub async fn upload(&mut self, upload_key: &UploadKey) -> io::Result<UploadResponse> {
        Ok(if self.need_upload(upload_key).await? {
            UploadResponse {
                count: 1,
                events_byte_size: self.do_upload(upload_key).await?,
            }
        } else {
            UploadResponse {
                count: 0,
                events_byte_size: 0,
            }
        })
    }

    async fn need_upload(&self, upload_key: &UploadKey) -> io::Result<bool> {
        match self
            .client
            .blob_client(&upload_key.object_key)
            .get_properties()
            .await
        {
            Err(_) => Ok(true),
            Ok(_) => Ok(false),
        }
    }

    async fn do_upload(&self, upload_key: &UploadKey) -> io::Result<usize> {
        let mut file = File::open(&upload_key.filename).await?;
        let file_size = file.metadata().await?.len();
        if file_size <= AZURE_BLOB_UPLOAD_CHUNK_SIZE as u64 {
            self.upload_directly(upload_key, &mut file, file_size).await
        } else {
            self.upload_in_blocks(upload_key, &mut file, file_size)
                .await
        }
    }

    async fn upload_directly(
        &self,
        upload_key: &UploadKey,
        file: &mut File,
        file_size: u64,
    ) -> io::Result<usize> {
        let mut buffer = Vec::with_capacity(file_size as usize);
        file.read_to_end(&mut buffer).await?;
        let client = self.client.blob_client(&upload_key.object_key);
        client
            .put_block_blob(buffer)
            .content_type("application/octet-stream")
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(file_size as usize)
    }

    async fn upload_in_blocks(
        &self,
        upload_key: &UploadKey,
        file: &mut File,
        file_size: u64,
    ) -> io::Result<usize> {
        let client = self.client.blob_client(&upload_key.object_key);
        let mut block_list = Vec::new();
        let mut uploaded_size = 0;
        let mut buffer = vec![0; AZURE_BLOB_UPLOAD_CHUNK_SIZE];
        while uploaded_size < file_size {
            let read_size = file.read(&mut buffer).await?;
            if read_size == 0 {
                break;
            }
            let block_id = format!("{:032}", block_list.len());
            client
                .put_block(
                    block_id.clone(),
                    Bytes::copy_from_slice(&buffer[..read_size]),
                )
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            block_list.push(BlobBlockType::new_committed(block_id));
            uploaded_size += read_size as u64;
        }
        client
            .put_block_list(BlockList { blocks: block_list })
            .content_type("application/octet-stream")
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(uploaded_size as usize)
    }
}
