//! Vector Extensions Library
//!
//! This library provides extensions for Vector, including system table collectors
//! and various data processing components.

#[macro_use]
extern crate tracing;

pub mod common;
pub mod sinks;
pub mod sources;
pub mod utils;

// Re-export commonly used types for testing
// Note: These modules need to be made public in their respective mod.rs files
pub use sources::system_tables::collectors::CoprocessorCollector;
pub use sources::system_tables::data_collector::{
    CollectionError, CollectionMetadata, CollectionMethod, CollectionResult, CollectorConfig,
};
pub use sources::system_tables::{CollectionConfig, DatabaseConfig, TableConfig};
