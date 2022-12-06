use std::io;
use std::path::Path;

use md5::Digest;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

pub struct EtagCalculator {
    chunk: Vec<u8>,
    concat_md5: Vec<u8>,
    multipart_upload_chunk_size: usize,
    multipart_upload_max_chunks: usize,
}

impl EtagCalculator {
    pub fn new(multipart_upload_chunk_size: usize, multipart_upload_max_chunks: usize) -> Self {
        Self {
            chunk: vec![],
            concat_md5: vec![],
            multipart_upload_chunk_size,
            multipart_upload_max_chunks,
        }
    }

    pub fn content_md5(chunk: &[u8]) -> String {
        base64::encode(md5::Md5::digest(chunk))
    }

    pub async fn file(&mut self, filename: impl AsRef<Path>) -> io::Result<String> {
        let mut chunk_count = 0;
        let mut file = File::open(filename).await?;
        let mut total_size = 0;
        loop {
            self.chunk.clear();
            let read_size = (&mut file)
                .take(self.multipart_upload_chunk_size as u64)
                .read_to_end(&mut self.chunk)
                .await?;
            total_size += read_size;
            if read_size == 0 {
                break;
            }
            chunk_count += 1;
            let digest: [u8; 16] = md5::Md5::digest(&self.chunk).into();
            self.concat_md5.extend_from_slice(&digest);
            if read_size < self.multipart_upload_chunk_size {
                break;
            }
            if chunk_count > self.multipart_upload_max_chunks {
                return Err(io::Error::new(io::ErrorKind::Other, "file is too large"));
            }
        }

        if self.concat_md5.is_empty() {
            let digest: [u8; 16] = md5::Md5::digest(&[]).into();
            self.concat_md5.extend_from_slice(&digest);
        }

        let res = if total_size >= self.multipart_upload_chunk_size {
            format!(
                "\"{:x}-{}\"",
                md5::Md5::digest(&self.concat_md5),
                chunk_count
            )
        } else {
            format!("\"{}\"", hex::encode(&self.concat_md5))
        };

        // limit the capacity to avoid occupying too much memory
        const MAX_CAPACITY: usize = 10 * 1024; // 10KiB
        self.concat_md5.clear();
        self.chunk.clear();
        self.concat_md5.shrink_to(MAX_CAPACITY);
        self.chunk.shrink_to(MAX_CAPACITY);

        Ok(res)
    }
}
