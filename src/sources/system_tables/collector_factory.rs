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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::system_tables::DatabaseConfig;

    #[tokio::test]
    async fn test_create_sql_collector() {
        let database_config = DatabaseConfig {
            username: "root".to_string(),
            password: "".to_string(),
            host: "127.0.0.1".to_string(),
            port: 4000,
            database: "test".to_string(),
            max_connections: Some(10),
            connect_timeout: Some(30),
            tls: None,
        };

        let config = CollectorConfig::for_sql("test_sql".to_string(), database_config);
        let result = CollectorFactory::create_collector(CollectionMethod::Sql, config);

        assert!(result.is_ok());
        assert_eq!(result.unwrap().collection_method(), CollectionMethod::Sql);
    }

    #[tokio::test]
    async fn test_create_coprocessor_collector() {
        let config = CollectorConfig::for_coprocessor(
            "test_instance".to_string(),
            "127.0.0.1".to_string(),
            4000,
            Some(30),
            Some(3),
            None,
        );

        let result = CollectorFactory::create_collector(CollectionMethod::Coprocessor, config);

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().collection_method(),
            CollectionMethod::Coprocessor
        );
    }

    #[tokio::test]
    async fn test_unimplemented_collectors() {
        let http_config = CollectorConfig::for_http_api(
            "test".to_string(),
            "127.0.0.1".to_string(),
            8080,
            Some(30),
            Some(3),
        );

        assert!(
            CollectorFactory::create_collector(CollectionMethod::HttpApi, http_config).is_err()
        );

        let grpc_config = CollectorConfig::for_http_api(
            "test".to_string(),
            "127.0.0.1".to_string(),
            9000,
            Some(30),
            Some(3),
        );

        assert!(
            CollectorFactory::create_collector(CollectionMethod::CustomGrpc, grpc_config).is_err()
        );
    }
}
