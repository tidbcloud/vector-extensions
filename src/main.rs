#[macro_use]
extern crate tracing;

use std::process::ExitCode;

use vector::{app::Application, extra_context::ExtraContext};

mod common;
mod sinks;
mod sources;
mod utils;

#[cfg(unix)]
fn main() -> ExitCode {
    // Install the default crypto provider for Rustls
    // This is required for Rustls 0.23+ to avoid the panic about crypto provider selection
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install default crypto provider");

    let exit_code = Application::run(ExtraContext::default())
        .code()
        .unwrap_or(exitcode::UNAVAILABLE) as u8;
    ExitCode::from(exit_code)
}
