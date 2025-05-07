use std::time::Duration;

use reqwest::{Certificate, Client, Identity};
use vector::tls::{TlsConfig, TlsSettings};
use tracing::error;

/// Builds a standardized reqwest::Client with consistent configuration
/// for use throughout the project.
pub async fn build_reqwest_client(
    tls: Option<TlsConfig>,
    timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
) -> Result<Client, Box<dyn std::error::Error + Send + Sync>> {
    let mut builder = Client::builder();
    
    // Configure TLS if provided
    if let Some(tls) = tls {
        if let Some(ca_file) = tls.ca_file.clone() {
            let ca = match tokio::fs::read(ca_file).await {
                Ok(v) => v,
                Err(err) => {
                    error!(message = "Failed to read TLS CA file", error = %err);
                    return Err(Box::new(err));
                }
            };
            
            let settings = TlsSettings::from_options(&Some(tls.clone())).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            let (crt, key) = settings.identity_pem().ok_or_else(|| "Invalid identity PEM")?;
            
            builder = builder
                .add_root_certificate(Certificate::from_pem(&ca).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?)
                .identity(Identity::from_pkcs8_pem(&crt, &key).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?);
        }
    }
    
    // Set timeouts with reasonable defaults
    builder = builder
        .timeout(timeout.unwrap_or(Duration::from_secs(60)))
        .connect_timeout(connect_timeout.unwrap_or(Duration::from_secs(10)));
        
    // Build the client
    let client = builder.build().map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    
    Ok(client)
} 