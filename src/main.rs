#[macro_use]
extern crate tracing;

use std::process::ExitCode;

use vector::{app::Application, extra_context::ExtraContext};

mod common;
mod sinks;
mod sources;

#[cfg(unix)]
fn main() -> ExitCode {
    let exit_code = Application::run(ExtraContext::default())
        .code()
        .unwrap_or(exitcode::UNAVAILABLE) as u8;
    ExitCode::from(exit_code)
}
