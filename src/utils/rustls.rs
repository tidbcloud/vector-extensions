use std::sync::Once;

static INIT_RUSTLS: Once = Once::new();

/// Ensure rustls has a process-level `CryptoProvider` installed.
///
/// rustls 0.23 requires the application to select a provider when crate features
/// are ambiguous (e.g. both `ring` and `aws-lc-rs` are enabled via dependency
/// feature unification). Calling this early avoids runtime panics.
pub fn init_rustls() {
    INIT_RUSTLS.call_once(|| {
        // Ignore the error when some other code path raced to install a provider.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}


