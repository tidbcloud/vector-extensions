use crate::sources::system_tables::data_collector::{
    CollectionError, CollectionMethod, CollectorConfig, DataCollector,
};

use crate::sources::system_tables::collectors::{CoprocessorCollector, SqlCollector};

/// Simplified collector factory - direct creation without complex abstractions
pub struct CollectorFactory;

impl CollectorFactory {
    /// Create a collector instance based on method and config
    pub fn create_collector(
        method: CollectionMethod,
        config: CollectorConfig,
    ) -> Result<Box<dyn DataCollector>, CollectionError> {
        match method {
            CollectionMethod::Sql => {
                let collector = SqlCollector::new(config)?;
                Ok(Box::new(collector))
            }
            CollectionMethod::Coprocessor => {
                let collector = CoprocessorCollector::new(config)?;
                Ok(Box::new(collector))
            }
            CollectionMethod::HttpApi => Err(CollectionError::ConfigurationError(
                "HTTP API collection method not implemented yet".to_string(),
            )),
            CollectionMethod::CustomGrpc => Err(CollectionError::ConfigurationError(
                "Custom gRPC collection method not implemented yet".to_string(),
            )),
        }
    }
}
