//! Aliyun STS helpers for temporary credentials.
//!
//! Supports two credential sources used by Delta Lake OSS writes and the
//! `alicloud_oss` sink:
//!
//! 1. **ACK RRSA (OIDC)** → `AssumeRoleWithOIDC`
//! 2. **ECS / ACK node RAM role** (IMDS) → signed `AssumeRole`
//!
//! Node-role mode matches deployments where worker nodes already have a RAM
//! role that is trusted to assume a cross-account writer role (no static AK/SK,
//! no RRSA webhook injection).

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use aws_credential_types::provider::{future, ProvideCredentials};
use aws_credential_types::Credentials;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use chrono::{DateTime, Utc};
use openssl::hash::MessageDigest;
use openssl::pkey::PKey;
use openssl::sign::Signer;
use reqwest::Client;
use serde_json::Value;
use tokio::sync::Mutex;
use tracing::info;

/// Environment variables injected by ACK RRSA / pod identity webhook.
pub const ENV_ROLE_ARN: &str = "ALIBABA_CLOUD_ROLE_ARN";
pub const ENV_OIDC_TOKEN_FILE: &str = "ALIBABA_CLOUD_OIDC_TOKEN_FILE";
pub const ENV_OIDC_PROVIDER_ARN: &str = "ALIBABA_CLOUD_OIDC_PROVIDER_ARN";

/// ECS instance metadata endpoint for RAM role credentials.
const ECS_RAM_CREDENTIALS_BASE: &str =
    "http://100.100.100.200/latest/meta-data/ram/security-credentials/";

/// Refresh credentials this long before the STS expiry.
const REFRESH_SKEW: Duration = Duration::from_secs(5 * 60);

/// Temporary credentials returned by Aliyun STS / IMDS.
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

    validate_aliyun_role_arn(&role_arn)?;

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

fn validate_aliyun_role_arn(role_arn: &str) -> vector::Result<()> {
    if !role_arn.starts_with("acs:ram::") {
        return Err(vector::Error::from(format!(
            "Invalid Aliyun role ARN (expected acs:ram::...): {role_arn}"
        )));
    }
    let parts: Vec<&str> = role_arn.split(':').collect();
    if parts.len() < 5 || parts[0] != "acs" || parts[1] != "ram" {
        return Err(vector::Error::from(format!(
            "Invalid Aliyun role ARN format: {role_arn}"
        )));
    }
    Ok(())
}

fn sts_endpoint(region: Option<&str>) -> String {
    let sts_region = region.unwrap_or("cn-hangzhou");
    format!("https://sts.{sts_region}.aliyuncs.com")
}

fn parse_credentials_object(credentials: &Value) -> vector::Result<AliyunStsCredentials> {
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

    Ok(AliyunStsCredentials {
        access_key_id,
        access_key_secret,
        security_token,
        expires_at,
    })
}

/// Percent-encode for Aliyun RPC signatures.
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for b in s.as_bytes() {
        match *b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Build Aliyun OpenAPI RPC signature (HMAC-SHA1).
fn sign_rpc_request(
    method: &str,
    params: &BTreeMap<String, String>,
    access_key_secret: &str,
) -> vector::Result<String> {
    let canonical: String = params
        .iter()
        .map(|(k, v)| format!("{}={}", percent_encode(k), percent_encode(v)))
        .collect::<Vec<_>>()
        .join("&");

    let string_to_sign = format!(
        "{}&{}&{}",
        method,
        percent_encode("/"),
        percent_encode(&canonical)
    );

    let key = format!("{access_key_secret}&");
    let pkey = PKey::hmac(key.as_bytes())
        .map_err(|e| vector::Error::from(format!("Failed to create HMAC key: {e}")))?;
    let mut signer = Signer::new(MessageDigest::sha1(), &pkey)
        .map_err(|e| vector::Error::from(format!("Failed to create HMAC signer: {e}")))?;
    signer
        .update(string_to_sign.as_bytes())
        .map_err(|e| vector::Error::from(format!("Failed to update HMAC signer: {e}")))?;
    let sig = signer
        .sign_to_vec()
        .map_err(|e| vector::Error::from(format!("Failed to compute HMAC signature: {e}")))?;
    Ok(BASE64.encode(sig))
}

fn http_client() -> vector::Result<Client> {
    Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| vector::Error::from(format!("Failed to create HTTP client: {e}")))
}

/// Fetch temporary credentials for the ECS instance / node RAM role via IMDS.
pub async fn get_ecs_ram_role_credentials() -> vector::Result<AliyunStsCredentials> {
    let client = http_client()?;

    info!(
        message = "Fetching Aliyun ECS node RAM role name from IMDS",
        endpoint = ECS_RAM_CREDENTIALS_BASE
    );

    let role_name_resp = client
        .get(ECS_RAM_CREDENTIALS_BASE)
        .send()
        .await
        .map_err(|e| {
            vector::Error::from(format!(
                "Failed to list ECS RAM role from IMDS ({ECS_RAM_CREDENTIALS_BASE}): {e}"
            ))
        })?;

    if !role_name_resp.status().is_success() {
        let status = role_name_resp.status();
        let text = role_name_resp.text().await.unwrap_or_default();
        return Err(vector::Error::from(format!(
            "ECS IMDS role list failed: {status} - {text}"
        )));
    }

    let role_name = role_name_resp
        .text()
        .await
        .map_err(|e| vector::Error::from(format!("Failed to read ECS RAM role name: {e}")))?
        .trim()
        .to_string();

    if role_name.is_empty() {
        return Err(vector::Error::from(
            "ECS IMDS returned empty RAM role name (is a node RAM role attached?)",
        ));
    }

    let creds_url = format!("{ECS_RAM_CREDENTIALS_BASE}{role_name}");
    info!(
        message = "Fetching Aliyun ECS node RAM role credentials from IMDS",
        role_name = %role_name
    );

    let creds_resp = client.get(&creds_url).send().await.map_err(|e| {
        vector::Error::from(format!("Failed to fetch ECS RAM credentials from IMDS: {e}"))
    })?;

    if !creds_resp.status().is_success() {
        let status = creds_resp.status();
        let text = creds_resp.text().await.unwrap_or_default();
        return Err(vector::Error::from(format!(
            "ECS IMDS credentials failed: {status} - {text}"
        )));
    }

    let json: Value = creds_resp
        .json()
        .await
        .map_err(|e| vector::Error::from(format!("Failed to parse ECS IMDS credentials: {e}")))?;

    if let Some(code) = json.get("Code").and_then(|v| v.as_str()) {
        if code != "Success" {
            return Err(vector::Error::from(format!(
                "ECS IMDS credentials Code={code}: {json}"
            )));
        }
    }

    let creds = parse_credentials_object(&json)?;
    info!(
        message = "Successfully obtained Aliyun ECS node RAM role credentials",
        role_name = %role_name
    );
    Ok(creds)
}

/// Call Aliyun STS `AssumeRole` signed with the given base credentials.
pub async fn assume_role(
    base: &AliyunStsCredentials,
    role_arn: &str,
    region: Option<&str>,
    session_name: &str,
) -> vector::Result<AliyunStsCredentials> {
    validate_aliyun_role_arn(role_arn)?;

    let endpoint = sts_endpoint(region);
    let timestamp_utc = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let signature_nonce = uuid::Uuid::new_v4().to_string();

    let mut params = BTreeMap::new();
    params.insert("AccessKeyId".to_string(), base.access_key_id.clone());
    params.insert("Action".to_string(), "AssumeRole".to_string());
    params.insert("Format".to_string(), "JSON".to_string());
    params.insert("RoleArn".to_string(), role_arn.to_string());
    params.insert("RoleSessionName".to_string(), session_name.to_string());
    params.insert("SignatureMethod".to_string(), "HMAC-SHA1".to_string());
    params.insert("SignatureNonce".to_string(), signature_nonce);
    params.insert("SignatureVersion".to_string(), "1.0".to_string());
    params.insert("Timestamp".to_string(), timestamp_utc);
    params.insert("Version".to_string(), "2015-04-01".to_string());
    // Required when signing with temporary (STS/IMDS) credentials.
    params.insert(
        "SecurityToken".to_string(),
        base.security_token.clone(),
    );

    let signature = sign_rpc_request("GET", &params, &base.access_key_secret)?;
    params.insert("Signature".to_string(), signature);

    let query: String = params
        .iter()
        .map(|(k, v)| format!("{}={}", percent_encode(k), percent_encode(v)))
        .collect::<Vec<_>>()
        .join("&");
    let url = format!("{endpoint}?{query}");

    info!(
        message = "Calling Aliyun STS AssumeRole with node RAM credentials",
        endpoint = %endpoint,
        role_arn = %role_arn
    );

    let client = http_client()?;
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| vector::Error::from(format!("Failed to call Aliyun STS AssumeRole: {e}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(vector::Error::from(format!(
            "Aliyun STS AssumeRole returned error: {status} - {text}"
        )));
    }

    let json: Value = response.json().await.map_err(|e| {
        vector::Error::from(format!("Failed to parse STS AssumeRole response: {e}"))
    })?;

    let credentials = json
        .get("Credentials")
        .ok_or_else(|| vector::Error::from("No Credentials in STS AssumeRole response"))?;

    let creds = parse_credentials_object(credentials)?;
    info!(message = "Successfully obtained temporary credentials from Aliyun STS AssumeRole");
    Ok(creds)
}

/// Node RAM role (IMDS) → STS AssumeRole → temporary writer credentials.
pub async fn get_aliyun_sts_credentials_via_node_role(
    role_arn: &str,
    region: Option<&str>,
    session_name: &str,
) -> vector::Result<AliyunStsCredentials> {
    let base = get_ecs_ram_role_credentials().await?;
    assume_role(&base, role_arn, region, session_name).await
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

    validate_aliyun_role_arn(role_arn)?;

    let sts_endpoint = sts_endpoint(region);
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

    let client = http_client()?;

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

    let creds = parse_credentials_object(credentials)?;
    info!(message = "Successfully obtained temporary credentials from Aliyun STS");
    Ok(creds)
}

/// Obtain Aliyun temporary credentials for `role_arn`.
///
/// Preference order:
/// 1. ACK RRSA (`ALIBABA_CLOUD_OIDC_TOKEN_FILE` present) → `AssumeRoleWithOIDC`
/// 2. Otherwise ECS/ACK node RAM role via IMDS → signed `AssumeRole`
pub async fn get_aliyun_role_credentials(
    role_arn: &str,
    region: Option<&str>,
    session_name: &str,
) -> vector::Result<AliyunStsCredentials> {
    validate_aliyun_role_arn(role_arn)?;

    if std::env::var(ENV_OIDC_TOKEN_FILE).is_ok() {
        info!("Using Aliyun RRSA (OIDC) for role credentials");
        let (resolved_role, token_file, oidc_provider_arn) =
            resolve_rrsa_config(Some(role_arn))?;
        get_aliyun_sts_credentials(
            &token_file,
            &resolved_role,
            &oidc_provider_arn,
            region,
            session_name,
        )
        .await
    } else {
        info!("Using Aliyun ECS/ACK node RAM role (IMDS) for AssumeRole");
        get_aliyun_sts_credentials_via_node_role(role_arn, region, session_name).await
    }
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

/// Refreshable AWS SDK credentials provider for Aliyun RAM roles.
///
/// Uses RRSA OIDC when `ALIBABA_CLOUD_OIDC_TOKEN_FILE` is set; otherwise uses
/// the ECS/ACK node RAM role from IMDS and STS `AssumeRole`.
#[derive(Debug, Clone)]
pub struct AliyunRoleCredentialsProvider {
    role_arn: String,
    region: Option<String>,
    session_name: String,
    cache: Arc<Mutex<Option<AliyunStsCredentials>>>,
}

impl AliyunRoleCredentialsProvider {
    pub fn new(
        role_arn: impl Into<String>,
        region: Option<String>,
        session_name: impl Into<String>,
    ) -> vector::Result<Self> {
        let role_arn = role_arn.into();
        validate_aliyun_role_arn(&role_arn)?;
        Ok(Self {
            role_arn,
            region,
            session_name: session_name.into(),
            cache: Arc::new(Mutex::new(None)),
        })
    }

    /// Resolve role ARN from explicit override or `ALIBABA_CLOUD_ROLE_ARN`.
    pub fn from_env(
        role_arn_override: Option<&str>,
        region: Option<String>,
        session_name: impl Into<String>,
    ) -> vector::Result<Self> {
        let role_arn = role_arn_override
            .map(|s| s.to_string())
            .or_else(|| std::env::var(ENV_ROLE_ARN).ok())
            .ok_or_else(|| {
                vector::Error::from(format!(
                    "{ENV_ROLE_ARN} is not set and auth.assume_role / auth.role_arn was not configured"
                ))
            })?;
        Self::new(role_arn, region, session_name)
    }

    async fn load(
        &self,
    ) -> Result<Credentials, aws_credential_types::provider::error::CredentialsError> {
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
            let creds = get_aliyun_role_credentials(
                &self.role_arn,
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
            "aliyun-role-sts",
        ))
    }
}

impl ProvideCredentials for AliyunRoleCredentialsProvider {
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

    #[test]
    fn percent_encode_leaves_unreserved() {
        assert_eq!(percent_encode("abc-_.~XYZ012"), "abc-_.~XYZ012");
    }

    #[test]
    fn percent_encode_encodes_specials() {
        assert_eq!(percent_encode("a b*c"), "a%20b%2Ac");
        assert_eq!(percent_encode("acs:ram::1:role/x"), "acs%3Aram%3A%3A1%3Arole%2Fx");
    }

    #[test]
    fn sign_rpc_request_is_stable() {
        let mut params = BTreeMap::new();
        params.insert("AccessKeyId".to_string(), "testid".to_string());
        params.insert("Action".to_string(), "AssumeRole".to_string());
        params.insert("Format".to_string(), "JSON".to_string());
        params.insert("SignatureMethod".to_string(), "HMAC-SHA1".to_string());
        params.insert("SignatureNonce".to_string(), "nonce".to_string());
        params.insert("SignatureVersion".to_string(), "1.0".to_string());
        params.insert("Timestamp".to_string(), "2020-01-01T00:00:00Z".to_string());
        params.insert("Version".to_string(), "2015-04-01".to_string());

        let sig1 = sign_rpc_request("GET", &params, "testsecret").unwrap();
        let sig2 = sign_rpc_request("GET", &params, "testsecret").unwrap();
        assert_eq!(sig1, sig2);
        assert!(!sig1.is_empty());
    }
}
