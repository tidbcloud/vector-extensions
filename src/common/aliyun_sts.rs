//! Aliyun STS helpers for ACK RRSA (OIDC) authentication.
//!
//! Used by Delta Lake storage options and the `alicloud_oss` sink to obtain
//! temporary credentials via `AssumeRoleWithOIDC`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use aws_credential_types::provider::{future, ProvideCredentials};
use aws_credential_types::Credentials;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde_json::Value;
use tokio::sync::Mutex;
use tracing::info;

/// Environment variables injected by ACK RRSA / pod identity webhook.
pub const ENV_ROLE_ARN: &str = "ALIBABA_CLOUD_ROLE_ARN";
pub const ENV_OIDC_TOKEN_FILE: &str = "ALIBABA_CLOUD_OIDC_TOKEN_FILE";
pub const ENV_OIDC_PROVIDER_ARN: &str = "ALIBABA_CLOUD_OIDC_PROVIDER_ARN";

/// Refresh credentials this long before the STS expiry.
const REFRESH_SKEW: Duration = Duration::from_secs(5 * 60);

/// Temporary credentials returned by Aliyun STS.
#[derive(Clone, Debug)]
pub struct AliyunStsCredentials {
    pub access_key_id: String,
    pub access_key_secret: String,
    pub security_token: String,
    pub expires_at: SystemTime,
}

/// Resolve RRSA role / token paths from the environment, with optional overrides.
pub fn resolve_rrsa_config(
    role_arn_override: Option<&str>,
) -> vector::Result<(String, String, String)> {
    let role_arn = role_arn_override
        .map(|s| s.to_string())
        .or_else(|| std::env::var(ENV_ROLE_ARN).ok())
        .ok_or_else(|| {
            vector::Error::from(format!(
                "{ENV_ROLE_ARN} is not set and auth.role_arn was not configured"
            ))
        })?;

    if !role_arn.starts_with("acs:ram::") {
        return Err(vector::Error::from(format!(
            "Invalid Aliyun role ARN (expected acs:ram::...): {role_arn}"
        )));
    }

    let token_file = std::env::var(ENV_OIDC_TOKEN_FILE).map_err(|_| {
        vector::Error::from(format!("{ENV_OIDC_TOKEN_FILE} is not set (ACK RRSA required)"))
    })?;

    let oidc_provider_arn = std::env::var(ENV_OIDC_PROVIDER_ARN).map_err(|_| {
        vector::Error::from(format!(
            "{ENV_OIDC_PROVIDER_ARN} is not set (ACK RRSA required)"
        ))
    })?;

    Ok((role_arn, token_file, oidc_provider_arn))
}

/// Get temporary credentials from Aliyun STS using an OIDC token (RRSA).
pub async fn get_aliyun_sts_credentials(
    token_file: &str,
    role_arn: &str,
    oidc_provider_arn: &str,
    region: Option<&str>,
    session_name: &str,
) -> vector::Result<AliyunStsCredentials> {
    use url::form_urlencoded;

    let oidc_token = tokio::fs::read_to_string(token_file)
        .await
        .map_err(|e| vector::Error::from(format!("Failed to read OIDC token file: {e}")))?;
    let oidc_token = oidc_token.trim();

    let parts: Vec<&str> = role_arn.split(':').collect();
    if parts.len() < 5 || parts[0] != "acs" || parts[1] != "ram" {
        return Err(vector::Error::from(format!(
            "Invalid Aliyun role ARN format: {role_arn}"
        )));
    }

    let sts_region = region.unwrap_or("cn-hangzhou");
    let sts_endpoint = format!("https://sts.{sts_region}.aliyuncs.com");
    let timestamp_utc = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let mut params = HashMap::new();
    params.insert("Action", "AssumeRoleWithOIDC");
    params.insert("RoleArn", role_arn);
    params.insert("OIDCProviderArn", oidc_provider_arn);
    params.insert("OIDCToken", oidc_token);
    params.insert("RoleSessionName", session_name);
    params.insert("Format", "JSON");
    params.insert("Version", "2015-04-01");
    params.insert("Timestamp", &timestamp_utc);

    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| vector::Error::from(format!("Failed to create HTTP client: {e}")))?;

    let query: String = form_urlencoded::Serializer::new(String::new())
        .extend_pairs(params.iter())
        .finish();
    let url = format!("{sts_endpoint}?{query}");

    info!(
        message = "Calling Aliyun STS AssumeRoleWithOIDC",
        endpoint = %sts_endpoint
    );

    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| vector::Error::from(format!("Failed to call Aliyun STS: {e}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(vector::Error::from(format!(
            "Aliyun STS returned error: {status} - {text}"
        )));
    }

    let json: Value = response
        .json()
        .await
        .map_err(|e| vector::Error::from(format!("Failed to parse STS response: {e}")))?;

    let credentials = json
        .get("Credentials")
        .ok_or_else(|| vector::Error::from("No Credentials in STS response"))?;

    let access_key_id = credentials
        .get("AccessKeyId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| vector::Error::from("No AccessKeyId in response"))?
        .to_string();

    let access_key_secret = credentials
        .get("AccessKeySecret")
        .and_then(|v| v.as_str())
        .ok_or_else(|| vector::Error::from("No AccessKeySecret in response"))?
        .to_string();

    let security_token = credentials
        .get("SecurityToken")
        .and_then(|v| v.as_str())
        .ok_or_else(|| vector::Error::from("No SecurityToken in response"))?
        .to_string();

    let expires_at = credentials
        .get("Expiration")
        .and_then(|v| v.as_str())
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| SystemTime::UNIX_EPOCH + Duration::from_secs(dt.timestamp() as u64))
        .unwrap_or_else(|| SystemTime::now() + Duration::from_secs(3600));

    info!(message = "Successfully obtained temporary credentials from Aliyun STS");

    Ok(AliyunStsCredentials {
        access_key_id,
        access_key_secret,
        security_token,
        expires_at,
    })
}

/// AWS SDK credentials provider that refreshes Aliyun RRSA/OIDC STS credentials.
#[derive(Debug, Clone)]
pub struct AliyunOidcCredentialsProvider {
    token_file: String,
    role_arn: String,
    oidc_provider_arn: String,
    region: Option<String>,
    session_name: String,
    cache: Arc<Mutex<Option<AliyunStsCredentials>>>,
}

impl AliyunOidcCredentialsProvider {
    pub fn new(
        token_file: impl Into<String>,
        role_arn: impl Into<String>,
        oidc_provider_arn: impl Into<String>,
        region: Option<String>,
        session_name: impl Into<String>,
    ) -> Self {
        Self {
            token_file: token_file.into(),
            role_arn: role_arn.into(),
            oidc_provider_arn: oidc_provider_arn.into(),
            region,
            session_name: session_name.into(),
            cache: Arc::new(Mutex::new(None)),
        }
    }

    /// Build a provider from RRSA environment variables (and optional role override).
    pub fn from_rrsa_env(
        role_arn_override: Option<&str>,
        region: Option<String>,
        session_name: impl Into<String>,
    ) -> vector::Result<Self> {
        let (role_arn, token_file, oidc_provider_arn) = resolve_rrsa_config(role_arn_override)?;
        Ok(Self::new(
            token_file,
            role_arn,
            oidc_provider_arn,
            region,
            session_name,
        ))
    }

    async fn load(&self) -> Result<Credentials, aws_credential_types::provider::error::CredentialsError>
    {
        let mut guard = self.cache.lock().await;
        let now = SystemTime::now();
        let needs_refresh = match guard.as_ref() {
            Some(cached) => cached
                .expires_at
                .checked_sub(REFRESH_SKEW)
                .map(|t| now >= t)
                .unwrap_or(true),
            None => true,
        };

        if needs_refresh {
            let creds = get_aliyun_sts_credentials(
                &self.token_file,
                &self.role_arn,
                &self.oidc_provider_arn,
                self.region.as_deref(),
                &self.session_name,
            )
            .await
            .map_err(|e| {
                aws_credential_types::provider::error::CredentialsError::provider_error(e)
            })?;
            *guard = Some(creds);
        }

        let cached = guard.as_ref().expect("credentials cache populated");
        Ok(Credentials::new(
            cached.access_key_id.clone(),
            cached.access_key_secret.clone(),
            Some(cached.security_token.clone()),
            Some(cached.expires_at),
            "aliyun-oidc-sts",
        ))
    }
}

impl ProvideCredentials for AliyunOidcCredentialsProvider {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        future::ProvideCredentials::new(self.load())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_aliyun_role_arn() {
        let err = resolve_rrsa_config(Some("arn:aws:iam::123:role/x")).unwrap_err();
        assert!(err.to_string().contains("Invalid Aliyun role ARN"));
    }
}
