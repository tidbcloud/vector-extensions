#[macro_use]
extern crate tracing;

mod config;
mod processor;
mod uploader;

pub use config::GcsUploadFileSinkConfig;
