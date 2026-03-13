use std::sync::Arc;

use object_store::{
    aws::AmazonS3Builder,
    azure::MicrosoftAzureBuilder,
    gcp::GoogleCloudStorageBuilder,
    local::LocalFileSystem,
    ObjectStore,
};
use tracing::info;
use url::Url;

/// Build ObjectStore based on endpoint, cloud provider, and optional region.
pub fn build_object_store(
    endpoint: &str,
    cloud_provider: &str,
    region: Option<&str>,
) -> vector::Result<Arc<dyn ObjectStore>> {
    let url = Url::parse(endpoint)
        .map_err(|e| format!("Invalid endpoint URL: {}", e))?;

    match cloud_provider.to_lowercase().as_str() {
        "aws" | "s3" => build_s3_store(&url, region),
        "gcp" | "gs" => build_gcs_store(&url),
        "azure" | "az" => build_azure_store(&url),
        "aliyun" | "oss" => build_oss_store(&url),
        "file" | "local" => build_local_store(&url),
        _ => Err(format!("Unsupported cloud provider: {}", cloud_provider).into()),
    }
}

fn build_s3_store(url: &Url, config_region: Option<&str>) -> vector::Result<Arc<dyn ObjectStore>> {
    info!("Building AWS S3 ObjectStore");

    let bucket = url
        .host_str()
        .ok_or_else(|| "Missing bucket name in S3 URL".to_string())?;

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket);

    // Region: config first, then AWS_REGION, then AWS_DEFAULT_REGION
    let region = config_region
        .filter(|s| !s.is_empty())
        .map(String::from)
        .or_else(|| std::env::var("AWS_REGION").ok())
        .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok());
    if let Some(region) = region {
        builder = builder.with_region(region);
    }

    // Configure credentials from environment
    // object_store will use AWS SDK credential chain automatically
    if let Ok(access_key_id) = std::env::var("AWS_ACCESS_KEY_ID") {
        builder = builder.with_access_key_id(access_key_id);
    }
    if let Ok(secret_access_key) = std::env::var("AWS_SECRET_ACCESS_KEY") {
        builder = builder.with_secret_access_key(secret_access_key);
    }
    if let Ok(session_token) = std::env::var("AWS_SESSION_TOKEN") {
        builder = builder.with_token(session_token);
    }

    // Set endpoint for custom S3-compatible services (e.g., MinIO)
    if let Some(endpoint_url) = std::env::var("AWS_ENDPOINT_URL").ok() {
        builder = builder.with_endpoint(endpoint_url);
    }

    let store = builder
        .build()
        .map_err(|e| format!("Failed to build S3 ObjectStore: {}", e))?;

    Ok(Arc::new(store))
}

fn build_gcs_store(url: &Url) -> vector::Result<Arc<dyn ObjectStore>> {
    info!("Building GCP Cloud Storage ObjectStore");

    let bucket = url
        .host_str()
        .ok_or_else(|| "Missing bucket name in GCS URL".to_string())?;

    let builder = GoogleCloudStorageBuilder::new()
        .with_bucket_name(bucket);

    // GCP credentials are typically provided via:
    // 1. GOOGLE_APPLICATION_CREDENTIALS environment variable (service account key file)
    // 2. Application Default Credentials (ADC)
    // object_store will use these automatically

    let store = builder
        .build()
        .map_err(|e| format!("Failed to build GCS ObjectStore: {}", e))?;

    Ok(Arc::new(store))
}

fn build_azure_store(url: &Url) -> vector::Result<Arc<dyn ObjectStore>> {
    info!("Building Azure Blob Storage ObjectStore");

    // Azure URL format: az://account/container/path
    let path_segments: Vec<&str> = url.path().split('/').filter(|s| !s.is_empty()).collect();
    
    if path_segments.is_empty() {
        return Err("Missing account and container in Azure URL".to_string().into());
    }

    let account = path_segments[0];
    let container = path_segments.get(1).ok_or_else(|| {
        "Missing container name in Azure URL".to_string()
    })?;

    let mut builder = MicrosoftAzureBuilder::new()
        .with_account(account)
        .with_container_name(container.to_string());

    // Azure credentials from environment variables
    if let Ok(account) = std::env::var("AZURE_STORAGE_ACCOUNT") {
        builder = builder.with_account(&account);
    }
    if let Ok(key) = std::env::var("AZURE_STORAGE_KEY") {
        builder = builder.with_access_key(&key);
    }

    let store = builder
        .build()
        .map_err(|e| format!("Failed to build Azure ObjectStore: {}", e))?;

    Ok(Arc::new(store))
}

fn build_oss_store(url: &Url) -> vector::Result<Arc<dyn ObjectStore>> {
    info!("Building Aliyun OSS ObjectStore (using S3-compatible API)");

    let bucket = url
        .host_str()
        .ok_or_else(|| "Missing bucket name in OSS URL".to_string())?;

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket);

    // OSS uses S3-compatible API but with custom endpoint
    let endpoint = std::env::var("OSS_ENDPOINT")
        .map_err(|_| "OSS_ENDPOINT environment variable is required for Aliyun OSS".to_string())?;

    // OSS endpoint format: https://oss-cn-hangzhou.aliyuncs.com
    builder = builder.with_endpoint(&endpoint);

    // OSS credentials
    if let Ok(access_key_id) = std::env::var("OSS_ACCESS_KEY_ID")
        .or_else(|_| std::env::var("AWS_ACCESS_KEY_ID")) {
        builder = builder.with_access_key_id(access_key_id);
    }
    if let Ok(secret_access_key) = std::env::var("OSS_ACCESS_KEY_SECRET")
        .or_else(|_| std::env::var("AWS_SECRET_ACCESS_KEY")) {
        builder = builder.with_secret_access_key(secret_access_key);
    }

    // OSS uses virtual-hosted style (not path-style)
    builder = builder.with_virtual_hosted_style_request(true);

    let store = builder
        .build()
        .map_err(|e| format!("Failed to build OSS ObjectStore: {}", e))?;

    Ok(Arc::new(store))
}

fn build_local_store(url: &Url) -> vector::Result<Arc<dyn ObjectStore>> {
    info!("Building Local FileSystem ObjectStore");

    let path = url
        .to_file_path()
        .map_err(|_| "Invalid local file path".to_string())?;

    let store = LocalFileSystem::new_with_prefix(path)
        .map_err(|e| format!("Failed to build Local ObjectStore: {}", e))?;

    Ok(Arc::new(store))
}
