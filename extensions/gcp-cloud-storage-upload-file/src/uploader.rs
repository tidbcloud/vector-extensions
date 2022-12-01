use std::io;

use common::checkpointer::UploadKey;
use http::header::HeaderName;
use http::{HeaderValue, Request, Uri};
use hyper::service::Service;
use hyper::Body;
use md5::{Digest, Md5};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use vector::gcp::GcpAuthenticator;
use vector::http::HttpClient;
use vector::serde::json;
use vector::sinks::gcs_common::config::BASE_URL;

use crate::config::GcsUploadFileSinkConfig;

// limit the chunk size to 8MB to avoid OOM
const GCS_UPLOAD_CHUNK_SIZE: usize = 8 * 1024 * 1024;

pub struct GCSUploader {
    client: HttpClient,
    auth: GcpAuthenticator,
    request_settings: RequestSettings,
}

pub struct UploadResponse {
    pub count: usize,
    pub events_byte_size: usize,
}

impl GCSUploader {
    pub const fn new(
        client: HttpClient,
        auth: GcpAuthenticator,
        request_settings: RequestSettings,
    ) -> Self {
        Self {
            client,
            auth,
            request_settings,
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
        if let Some(object_hash) = self.fetch_md5_hash(upload_key).await {
            let file_hash = self.calculate_file_md5_hash(&upload_key.filename).await?;
            Ok(object_hash != file_hash)
        } else {
            Ok(true)
        }
    }

    async fn do_upload(&mut self, upload_key: &UploadKey) -> io::Result<usize> {
        let session_uri = self.create_resumable_upload(upload_key).await?;
        self.resumable_upload(&session_uri, &upload_key.filename)
            .await
    }

    async fn fetch_md5_hash(&mut self, upload_key: &UploadKey) -> Option<String> {
        let uri = format!(
            "{}{}/{}",
            BASE_URL, upload_key.bucket, upload_key.object_key
        )
        .parse::<Uri>()
        .unwrap();

        let mut builder = Request::head(uri);
        let headers = builder.headers_mut().unwrap();
        self.request_settings.clone().apply(headers);

        let mut http_request = builder.body(Body::empty()).unwrap();
        self.auth.apply(&mut http_request);

        let resp = self.client.call(http_request).await.ok()?;
        for v in resp.headers().get_all("x-goog-hash") {
            let value_str = v.to_str().ok()?;
            if let Some((_, hash)) = value_str.split_once("md5=") {
                return Some(hash.to_string());
            }
        }
        None
    }

    async fn calculate_file_md5_hash(&self, filename: &str) -> io::Result<String> {
        let mut file = File::open(filename).await?;
        let mut hasher = Md5::new();
        let mut buffer = [0; 8096];
        loop {
            let n = file.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }
        let res = hasher.finalize();
        Ok(base64::encode(&res[..]))
    }

    async fn create_resumable_upload(&mut self, upload_key: &UploadKey) -> io::Result<Uri> {
        let uri = format!(
            "{}{}/{}",
            BASE_URL, upload_key.bucket, upload_key.object_key
        )
        .parse::<Uri>()
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

        let mut builder = Request::post(uri);
        let headers = builder.headers_mut().unwrap();
        self.request_settings.clone().apply(headers);

        headers.insert("content-length", HeaderValue::from_static("0"));
        headers.insert("x-goog-resumable", HeaderValue::from_static("start"));

        let mut http_request = builder.body(Body::empty()).unwrap();
        self.auth.apply(&mut http_request);

        let resp = self
            .client
            .call(http_request)
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

        if !resp.status().is_success() {
            let (parts, body) = resp.into_parts();
            let body = hyper::body::to_bytes(body).await.unwrap_or_default();
            let body = String::from_utf8_lossy(body.as_ref());
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "Failed to create resumable upload status: {} body: {}",
                    parts.status, body
                ),
            ));
        }

        let location = resp
            .headers()
            .get("location")
            .and_then(|l| l.to_str().ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Missing location header"))?;
        location
            .parse::<Uri>()
            .map_err(|error| io::Error::new(io::ErrorKind::Other, error))
    }

    async fn resumable_upload(&mut self, session_uri: &Uri, filename: &str) -> io::Result<usize> {
        let mut file = File::open(filename).await?;

        let mut uploaded_bytes = 0;
        let mut chunk = vec![];
        loop {
            chunk.clear();
            (&mut file)
                .take(GCS_UPLOAD_CHUNK_SIZE as u64)
                .read_to_end(&mut chunk)
                .await?;

            if chunk.len() < GCS_UPLOAD_CHUNK_SIZE {
                break;
            }

            let chunk_res = self
                .upload_chunk(session_uri, std::mem::take(&mut chunk), uploaded_bytes)
                .await;
            match chunk_res {
                Ok(bytes) => uploaded_bytes += bytes,
                Err(error) => {
                    self.cancel_upload(session_uri).await;
                    return Err(error);
                }
            }
        }

        let upload_res = self
            .complete_upload(session_uri, chunk, uploaded_bytes)
            .await;
        match upload_res {
            Ok(n) => Ok(uploaded_bytes + n),
            Err(error) => {
                self.cancel_upload(session_uri).await;
                Err(error)
            }
        }
    }

    async fn upload_chunk(
        &mut self,
        session_uri: &Uri,
        chunk: Vec<u8>,
        uploaded_bytes: usize,
    ) -> io::Result<usize> {
        let n = chunk.len();

        let mut builder = Request::put(session_uri);
        let headers = builder.headers_mut().unwrap();
        self.request_settings.clone().apply(headers);

        headers.insert(
            "content-length",
            HeaderValue::from_str(&n.to_string()).unwrap(),
        );
        headers.insert(
            "content-type",
            HeaderValue::from_static("application/octet-stream"),
        );
        headers.insert(
            "content-md5",
            HeaderValue::from_str(&base64::encode(Md5::digest(&chunk))).unwrap(),
        );
        let range_begin = uploaded_bytes;
        let range_end = uploaded_bytes + n - 1;
        headers.insert(
            "content-range",
            HeaderValue::from_str(&format!("bytes {}-{}/*", range_begin, range_end)).unwrap(),
        );

        let mut http_request = builder.body(Body::from(chunk)).unwrap();
        self.auth.apply(&mut http_request);

        let resp = self
            .client
            .call(http_request)
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

        if resp.status().as_u16() != 308 {
            let (parts, body) = resp.into_parts();
            let body = hyper::body::to_bytes(body).await.unwrap_or_default();
            let body = String::from_utf8_lossy(body.as_ref());
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "Failed to upload chunk status: {} body: {}",
                    parts.status, body
                ),
            ));
        }

        let range = resp
            .headers()
            .get("range")
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Failed to get range header"))?;
        let uploaded_range_end = range
            .to_str()
            .ok()
            .and_then(|r| r.split_once('-').map(|x| x.1))
            .and_then(|r| r.parse::<usize>().ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Failed to parse range header"))?;

        if uploaded_range_end != range_end {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "Failed to upload chunk received bytes: {} uploaded bytes: {}",
                    uploaded_range_end + 1,
                    range_end + 1
                ),
            ));
        }
        Ok(n)
    }

    async fn complete_upload(
        &mut self,
        session_uri: &Uri,
        chunk: Vec<u8>,
        uploaded_bytes: usize,
    ) -> io::Result<usize> {
        let n = chunk.len();
        let mut builder = Request::put(session_uri);
        let headers = builder.headers_mut().unwrap();
        self.request_settings.clone().apply(headers);

        headers.insert(
            "content-length",
            HeaderValue::from_str(&n.to_string()).unwrap(),
        );
        headers.insert(
            "content-type",
            HeaderValue::from_static("application/octet-stream"),
        );
        if n != 0 {
            let range_begin = uploaded_bytes;
            let range_end = uploaded_bytes + n - 1;
            headers.insert(
                "content-range",
                HeaderValue::from_str(&format!(
                    "bytes {}-{}/{}",
                    range_begin,
                    range_end,
                    uploaded_bytes + n
                ))
                .unwrap(),
            );
            headers.insert(
                "content-md5",
                HeaderValue::from_str(&base64::encode(Md5::digest(&chunk))).unwrap(),
            );
        } else {
            headers.insert(
                "content-range",
                HeaderValue::from_str(&format!("bytes */{}", uploaded_bytes)).unwrap(),
            );
        }

        let mut http_request = builder.body(Body::from(chunk)).unwrap();
        self.auth.apply(&mut http_request);

        let resp = self
            .client
            .call(http_request)
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

        if !resp.status().is_success() {
            let (parts, body) = resp.into_parts();
            let body = hyper::body::to_bytes(body).await.unwrap_or_default();
            let body = String::from_utf8_lossy(body.as_ref());
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "Failed to complete upload status: {} body: {}",
                    parts.status, body
                ),
            ));
        }
        Ok(n)
    }

    async fn cancel_upload(&mut self, session_uri: &Uri) {
        let mut builder = Request::delete(session_uri);
        let headers = builder.headers_mut().unwrap();
        self.request_settings.clone().apply(headers);
        headers.insert("content-length", HeaderValue::from_static("0"));

        let mut http_request = builder.body(Body::empty()).unwrap();
        self.auth.apply(&mut http_request);

        self.client.call(http_request).await.ok();
    }
}

// Settings required to produce a request that do not change per
// request. All possible values are pre-computed for direct use in
// producing a request.
#[derive(Clone, Debug)]
pub struct RequestSettings {
    acl: Option<HeaderValue>,
    storage_class: HeaderValue,
    headers: Vec<(HeaderName, HeaderValue)>,
}

impl RequestSettings {
    pub fn new(config: &GcsUploadFileSinkConfig) -> vector::Result<Self> {
        let acl = config
            .acl
            .map(|acl| HeaderValue::from_str(&json::to_string(acl)).unwrap());
        let storage_class = config.storage_class.unwrap_or_default();
        let storage_class = HeaderValue::from_str(&json::to_string(storage_class)).unwrap();
        let metadata = config
            .metadata
            .as_ref()
            .map(|metadata| {
                metadata
                    .iter()
                    .map(make_header)
                    .collect::<Result<Vec<_>, _>>()
            })
            .unwrap_or_else(|| Ok(vec![]))?;
        Ok(Self {
            acl,
            storage_class,
            headers: metadata,
        })
    }

    fn apply(self, headers: &mut http::HeaderMap) {
        self.acl.map(|acl| headers.insert("x-goog-acl", acl));
        headers.insert("x-goog-storage-class", self.storage_class);
        for (p, v) in self.headers {
            headers.insert(p, v);
        }
    }
}

// Make a header pair from a key-value string pair
fn make_header((name, value): (&String, &String)) -> vector::Result<(HeaderName, HeaderValue)> {
    Ok((
        HeaderName::from_bytes(name.as_bytes())?,
        HeaderValue::from_str(value)?,
    ))
}
