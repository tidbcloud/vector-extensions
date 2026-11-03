use std::time::Duration;

use reqwest::{Certificate, Client, Identity};
use tracing::error;
use vector::tls::{TlsConfig, TlsSettings};

/// Builds a standardized reqwest::Client with consistent configuration
/// for use throughout the project.
///
/// TLS verification defaults to **on** (`verify_certificate` /
/// `verify_hostname` default to `true` when unset). Operators must
/// explicitly set those fields to `false` to disable verification —
/// matching `keyspace_cluster` and Vector's own TLS defaults.
pub async fn build_reqwest_client(
    tls: Option<TlsConfig>,
    timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
) -> Result<Client, Box<dyn std::error::Error + Send + Sync>> {
    let mut builder = Client::builder();

    // Configure TLS if provided
    if let Some(tls) = tls {
        // Secure-by-default: verify unless the operator opts out.
        let accept_invalid_certs = !tls.verify_certificate.unwrap_or(true);
        let accept_invalid_hostnames = !tls.verify_hostname.unwrap_or(true);
        builder = builder
            .danger_accept_invalid_certs(accept_invalid_certs)
            .danger_accept_invalid_hostnames(accept_invalid_hostnames);

        if let Some(ca_file) = tls.ca_file.clone() {
            let ca = match tokio::fs::read(ca_file).await {
                Ok(v) => v,
                Err(err) => {
                    error!(message = "Failed to read TLS CA file", error = %err);
                    return Err(Box::new(err));
                }
            };

            builder = builder.add_root_certificate(
                Certificate::from_pem(&ca)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
            );
        }

        if tls.crt_file.is_some() || tls.key_file.is_some() {
            let settings = TlsSettings::from_options(Some(&tls.clone()))
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            let (crt, key) = settings
                .identity_pem()
                .ok_or_else(|| "Invalid identity PEM")?;

            builder = builder.identity(
                Identity::from_pkcs8_pem(&crt, &key)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
            );
        }
    }

    // Set timeouts with reasonable defaults
    builder = builder
        .timeout(timeout.unwrap_or(Duration::from_secs(60)))
        .connect_timeout(connect_timeout.unwrap_or(Duration::from_secs(10)));

    // Build the client
    let client = builder
        .build()
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

    Ok(client)
}
