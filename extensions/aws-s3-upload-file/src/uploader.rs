use std::io;

use aws_sdk_s3::model::{CompletedMultipartUpload, CompletedPart, MultipartUpload, Part};
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::Client as S3Client;
use common::checkpointer::UploadKey;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use vector::sinks::s3_common::config::S3Options;

use crate::etag_calculator::EtagCalculator;

// limit the chunk size to 8MB to avoid OOM
const S3_MULTIPART_UPLOAD_CHUNK_SIZE: usize = 8 * 1024 * 1024;
const S3_MULTIPART_UPLOAD_MAX_CHUNKS: usize = 10000;

pub struct S3Uploader {
    client: S3Client,
    options: S3Options,
    etag_calculator: EtagCalculator,
}

pub struct UploadResponse {
    pub count: usize,
    pub events_byte_size: usize,
}

impl S3Uploader {
    pub fn new(client: S3Client, options: S3Options) -> Self {
        Self {
            client,
            options,
            etag_calculator: EtagCalculator::new(
                S3_MULTIPART_UPLOAD_CHUNK_SIZE,
                S3_MULTIPART_UPLOAD_MAX_CHUNKS,
            ),
        }
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

    async fn need_upload(&mut self, upload_key: &UploadKey) -> io::Result<bool> {
        if let Some(object_etag) = self.fetch_object_etag(upload_key).await {
            let etag = self.etag_calculator.file(&upload_key.filename).await?;
            if etag == object_etag {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn fetch_object_etag(&self, upload_key: &UploadKey) -> Option<String> {
        self.client
            .head_object()
            .bucket(&upload_key.bucket)
            .key(&upload_key.object_key)
            .send()
            .await
            .map(|res| res.e_tag)
            .ok()
            .flatten()
    }

    async fn do_upload(&mut self, upload_key: &UploadKey) -> io::Result<usize> {
        let mut file = File::open(&upload_key.filename).await?;

        let mut chunk = Vec::new();
        let n = (&mut file)
            .take(S3_MULTIPART_UPLOAD_CHUNK_SIZE as u64)
            .read_to_end(&mut chunk)
            .await?;
        if n < S3_MULTIPART_UPLOAD_CHUNK_SIZE {
            let uploader = self.multipart_uploader(upload_key, vec![], file);
            uploader.abort_all_uploads().await?;
            self.put_object(upload_key, chunk).await
        } else {
            let uploader = self.multipart_uploader(upload_key, chunk, file);
            Ok(uploader.upload().await?)
        }
    }

    async fn put_object(&self, upload_key: &UploadKey, body: Vec<u8>) -> io::Result<usize> {
        let content_md5 = EtagCalculator::content_md5(&body);
        let size = body.len();
        let tagging = self.options.tags.as_ref().map(|tags| {
            let mut tagging = url::form_urlencoded::Serializer::new(String::new());
            for (p, v) in tags {
                tagging.append_pair(p, v);
            }
            tagging.finish()
        });

        let _ = self
            .client
            .put_object()
            .body(ByteStream::from(body))
            .bucket(&upload_key.bucket)
            .key(&upload_key.object_key)
            .set_content_encoding(self.options.content_encoding.clone())
            .set_content_type(self.options.content_type.clone())
            .set_acl(self.options.acl.map(Into::into))
            .set_grant_full_control(self.options.grant_full_control.clone())
            .set_grant_read(self.options.grant_read.clone())
            .set_grant_read_acp(self.options.grant_read_acp.clone())
            .set_grant_write_acp(self.options.grant_write_acp.clone())
            .set_server_side_encryption(self.options.server_side_encryption.map(Into::into))
            .set_ssekms_key_id(self.options.ssekms_key_id.clone())
            .set_storage_class(self.options.storage_class.map(Into::into))
            .set_tagging(tagging)
            .content_md5(content_md5)
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(size)
    }

    fn multipart_uploader<'a, 'b>(
        &'a mut self,
        upload_key: &'b UploadKey,
        chunk: Vec<u8>,
        file: File,
    ) -> MultipartUploader<'a, 'b> {
        MultipartUploader {
            client: &self.client,
            options: &self.options,
            upload_key,

            upload_id: "".to_owned(),
            file,
            chunk,
            part_number: 1,
            completed_parts: vec![],
        }
    }
}

struct MultipartUploader<'a, 'b> {
    client: &'a S3Client,
    options: &'a S3Options,
    upload_key: &'b UploadKey,

    upload_id: String,
    file: File,
    chunk: Vec<u8>,
    part_number: i32,
    completed_parts: Vec<CompletedPart>,
}

impl<'a, 'b> MultipartUploader<'a, 'b> {
    async fn upload(mut self) -> io::Result<usize> {
        self.initiate_upload().await?;

        let mut uploaded_size = 0;
        while !self.chunk.is_empty() {
            if self.part_number as usize > S3_MULTIPART_UPLOAD_MAX_CHUNKS {
                return Err(io::Error::new(io::ErrorKind::Other, "file is too large"));
            }

            let n = self.upload_part().await?;
            uploaded_size += n;

            self.chunk.clear();
            self.chunk.reserve(S3_MULTIPART_UPLOAD_CHUNK_SIZE);
            (&mut self.file)
                .take(S3_MULTIPART_UPLOAD_CHUNK_SIZE as u64)
                .read_to_end(&mut self.chunk)
                .await?;
            self.part_number += 1;
        }

        self.complete_upload().await?;
        Ok(uploaded_size)
    }

    async fn initiate_upload(&mut self) -> io::Result<()> {
        let uploads = self.list_existing_uploads().await?;
        if uploads.is_empty() {
            self.upload_id = self.create_upload().await?;
            return Ok(());
        }

        // only recover the latest multipart upload, abort others
        let upload_id = self.cleanup_uploads_except_latest(uploads).await?;
        let parts = self.list_parts(&upload_id).await?;
        if parts.is_empty() {
            self.upload_id = upload_id;
            return Ok(());
        }

        if self.verify_and_advance(parts, &upload_id).await? {
            self.upload_id = upload_id;
            return Ok(());
        }

        self.abort_upload(upload_id).await?;
        self.upload_id = self.create_upload().await?;
        // `verify_and_advance` modified these fields, reset them
        self.file = File::open(&self.upload_key.filename).await?;
        self.chunk.clear();
        (&mut self.file)
            .take(S3_MULTIPART_UPLOAD_CHUNK_SIZE as u64)
            .read_to_end(&mut self.chunk)
            .await?;
        self.part_number = 1;
        self.completed_parts.clear();
        Ok(())
    }

    async fn abort_all_uploads(&self) -> io::Result<()> {
        let uploads = self.list_existing_uploads().await?;
        for upload in uploads {
            let upload_id = upload.upload_id.unwrap_or_default();
            info!(
                message = "Cleaned up unused multipart upload",
                filename = %self.upload_key.filename,
                bucket = %self.upload_key.bucket,
                key = %self.upload_key.object_key,
                %upload_id,
            );
            self.abort_upload(upload_id).await?;
        }
        Ok(())
    }

    async fn list_existing_uploads(&self) -> io::Result<Vec<MultipartUpload>> {
        let uploads = self
            .client
            .list_multipart_uploads()
            .bucket(&self.upload_key.bucket)
            .prefix(&self.upload_key.object_key)
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(uploads.uploads.unwrap_or_default())
    }

    async fn create_upload(&mut self) -> io::Result<String> {
        let tagging = self.options.tags.as_ref().map(|tags| {
            let mut tagging = url::form_urlencoded::Serializer::new(String::new());
            for (p, v) in tags {
                tagging.append_pair(p, v);
            }
            tagging.finish()
        });

        let response = self
            .client
            .create_multipart_upload()
            .bucket(&self.upload_key.bucket)
            .key(&self.upload_key.object_key)
            .set_content_encoding(self.options.content_encoding.clone())
            .set_content_type(self.options.content_type.clone())
            .set_acl(self.options.acl.map(Into::into))
            .set_grant_full_control(self.options.grant_full_control.clone())
            .set_grant_read(self.options.grant_read.clone())
            .set_grant_read_acp(self.options.grant_read_acp.clone())
            .set_grant_write_acp(self.options.grant_write_acp.clone())
            .set_server_side_encryption(self.options.server_side_encryption.map(Into::into))
            .set_ssekms_key_id(self.options.ssekms_key_id.clone())
            .set_storage_class(self.options.storage_class.map(Into::into))
            .set_tagging(tagging)
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(response.upload_id.unwrap_or_default())
    }

    async fn cleanup_uploads_except_latest(
        &self,
        mut uploads: Vec<MultipartUpload>,
    ) -> io::Result<String> {
        uploads.sort_unstable_by_key(|a| {
            a.initiated
                .as_ref()
                .map(|a| a.as_nanos())
                .unwrap_or_default()
        });
        let upload = uploads.pop().unwrap();

        // abort older uploads
        for upload in uploads {
            let upload_id = upload.upload_id.unwrap_or_default();
            info!(
                message = "Cleaned up unused multipart upload",
                filename = %self.upload_key.filename,
                bucket = %self.upload_key.bucket,
                key = %self.upload_key.object_key,
                %upload_id,
            );
            self.abort_upload(upload_id).await?;
        }

        Ok(upload.upload_id.unwrap_or_default())
    }

    async fn abort_upload(&self, upload_id: String) -> io::Result<()> {
        self.client
            .abort_multipart_upload()
            .bucket(&self.upload_key.bucket)
            .key(&self.upload_key.object_key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(())
    }

    async fn verify_and_advance(
        &mut self,
        mut parts: Vec<Part>,
        upload_id: &str,
    ) -> io::Result<bool> {
        let mut recovered_part_size = 0;
        parts.sort_unstable_by_key(|a| a.part_number);
        for part in parts {
            // check part number
            let part_number = part.part_number;
            let expected_part_number = self.part_number;
            if part_number != expected_part_number {
                warn!(
                    message = "Unexpected part number, aborted multipart upload.",
                    filename = %self.upload_key.filename,
                    bucket = %self.upload_key.bucket,
                    key = %self.upload_key.object_key,
                    %part_number,
                    %expected_part_number,
                    %upload_id,
                );
                return Ok(false);
            }

            // check etag
            let expected_part_etag = EtagCalculator::part(&self.chunk);
            let part_etag = part.e_tag.unwrap_or_default();
            if part_etag != expected_part_etag {
                warn!(
                    message = "Unexpected part etag, aborted multipart upload.",
                    filename = %self.upload_key.filename,
                    bucket = %self.upload_key.bucket,
                    key = %self.upload_key.object_key,
                    %part_etag,
                    %expected_part_etag,
                    %upload_id,
                );
                return Ok(false);
            }

            let completed_part = CompletedPart::builder()
                .e_tag(part_etag)
                .part_number(part_number)
                .build();
            self.completed_parts.push(completed_part);
            recovered_part_size += part.size;

            self.chunk.clear();
            (&mut self.file)
                .take(S3_MULTIPART_UPLOAD_CHUNK_SIZE as u64)
                .read_to_end(&mut self.chunk)
                .await?;
            self.part_number += 1;
        }

        info!(
            message = "Resumed upload",
            filename = %self.upload_key.filename,
            bucket = %self.upload_key.bucket,
            key = %self.upload_key.object_key,
            %recovered_part_size,
            %upload_id,
        );
        Ok(true)
    }

    async fn list_parts(&self, upload_id: &str) -> io::Result<Vec<Part>> {
        let res = self
            .client
            .list_parts()
            .bucket(&self.upload_key.bucket)
            .key(&self.upload_key.object_key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(res.parts.unwrap_or_default())
    }

    async fn upload_part(&mut self) -> io::Result<usize> {
        let body = std::mem::take(&mut self.chunk);
        let size = body.len();
        let content_md5 = EtagCalculator::content_md5(&body);
        let response = self
            .client
            .upload_part()
            .body(ByteStream::from(body))
            .bucket(&self.upload_key.bucket)
            .key(&self.upload_key.object_key)
            .part_number(self.part_number)
            .upload_id(&self.upload_id)
            .content_md5(content_md5)
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let completed_part = CompletedPart::builder()
            .part_number(self.part_number)
            .e_tag(response.e_tag.unwrap_or_default())
            .build();
        self.completed_parts.push(completed_part);

        Ok(size)
    }

    async fn complete_upload(&mut self) -> io::Result<()> {
        let completed_parts = std::mem::take(&mut self.completed_parts);
        let completed_multipart_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();
        let _ = self
            .client
            .complete_multipart_upload()
            .bucket(&self.upload_key.bucket)
            .key(&self.upload_key.object_key)
            .upload_id(&self.upload_id)
            .multipart_upload(completed_multipart_upload)
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(())
    }
}
