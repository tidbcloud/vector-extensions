use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tonic::transport::Channel;
use tracing::{debug, error, info};

use crate::sources::system_tables::data_collector::{
    CollectionError, CollectionMetadata, CollectionMethod, CollectionResult, CollectorConfig,
    CollectorConfigType, DataCollector,
};
use crate::sources::system_tables::TableConfig;

pub mod proto {
    tonic::include_proto!("systemtable.v1");
}

use proto::{
    system_table_pull_service_client::SystemTablePullServiceClient, DescribeTableRequest, ListTablesRequest,
    TableQuery, TableQueryResponse, TableRow,
};

/// Pull table type for gRPC pull collectors
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PullTableType {
    /// Slow query table
    SlowQuery,
    /// Processlist table
    Processlist,
    /// Statements summary table
    StatementsSummary,
    /// Other tables (future extension)
    Other(String),
}

impl PullTableType {
    /// Parse from table name
    pub fn from_table_name(table_name: &str) -> Self {
        let name_upper = table_name.to_uppercase();
        if name_upper.contains("SLOW_QUERY") || name_upper.contains("SLOW_QUERIES") {
            PullTableType::SlowQuery
        } else if name_upper.contains("PROCESSLIST") {
            PullTableType::Processlist
        } else if name_upper.contains("STATEMENTS_SUMMARY") {
            PullTableType::StatementsSummary
        } else {
            PullTableType::Other(table_name.to_string())
        }
    }

    /// Get the table name
    pub fn table_name(&self) -> &str {
        match self {
            PullTableType::SlowQuery => "SLOW_QUERY",
            PullTableType::Processlist => "PROCESSLIST",
            PullTableType::StatementsSummary => "STATEMENTS_SUMMARY",
            PullTableType::Other(name) => name.as_str(),
        }
    }
}

/// gRPC pull collector — pulls data from TiDB via SystemTablePullService::QueryTable.
/// This is a generic collector that can query any system table.
pub struct GrpcPullCollector {
    instance: String,
    host: String,
    status_port: u16,
    grpc_timeout_secs: u64,
    max_retries: u32,
    /// The type of table this collector handles
    table_type: PullTableType,
}

impl GrpcPullCollector {
    pub fn new(config: CollectorConfig, table_config: TableConfig) -> Result<Self, CollectionError> {
        match config.config_type {
            CollectorConfigType::GrpcPull {
                host,
                status_port,
                grpc_timeout_secs,
                max_retries,
            } => {
                // Determine table type based on table_config
                let table_type = PullTableType::from_table_name(&table_config.source_table);

                Ok(Self {
                    instance: config.instance,
                    host,
                    status_port,
                    grpc_timeout_secs,
                    max_retries,
                    table_type,
                })
            }
            _ => Err(CollectionError::ConfigurationError(
                "Expected GrpcPull config".to_string(),
            )),
        }
    }

    /// Get the table type this collector handles
    pub fn table_type(&self) -> &PullTableType {
        &self.table_type
    }

    /// Get or create the gRPC client
    async fn get_client(&self) -> Result<SystemTablePullServiceClient<Channel>, CollectionError> {
        // For now, create a new client each time to avoid borrowing issues
        // In production, you might want to use a proper connection pool
        let endpoint = format!("http://{}:{}", self.host, self.status_port);
        info!("Connecting to TiDB pull service at {}", endpoint);

        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| CollectionError::ConfigurationError(format!("Invalid endpoint: {}", e)))?
            .connect()
            .await
            .map_err(|e| {
                CollectionError::ConnectionError(format!(
                    "Failed to connect to TiDB at {}: {}",
                    endpoint, e
                ))
            })?;

        let client = SystemTablePullServiceClient::new(channel);
        Ok(client)
    }

    /// Query a table using the pull service
    async fn query_table(
        &self,
        table_name: &str,
        where_clause: Option<&str>,
    ) -> Result<Vec<HashMap<String, Value>>, CollectionError> {
        let mut client = self.get_client().await?;

        // Build the query request
        let mut request = TableQuery {
            table: table_name.to_string(),
            time_range: None,
            r#where: where_clause.map(|s| s.to_string()),
            order_by: vec![],
            limit: None,
            offset: None,
            cursor: None,
            columns: vec![],
        };

        info!("Querying table {} with filter: {:?}", table_name, request.r#where);

        // Make the gRPC call
        let response = client
            .query_table(request)
            .await
            .map_err(|e| CollectionError::QueryError(format!("QueryTable failed: {}", e)))?;

        // Process the streaming response
        let mut all_rows = Vec::new();
        let mut schema: Option<proto::TableSchema> = None;

        // Note: In a real implementation, we'd need to handle the stream properly
        // For now, this is a simplified version
        let mut stream = response.into_inner();

        // Get first message to extract schema
        if let Ok(Some(first_response)) = stream.message().await {
            schema = first_response.schema;

            for row in first_response.rows {
                if let Some(parsed_row) = self.parse_row(&schema, &row) {
                    all_rows.push(parsed_row);
                }
            }
        }

        // Continue reading remaining messages
        while let Ok(Some(response)) = stream.message().await {
            for row in response.rows {
                if let Some(parsed_row) = self.parse_row(&schema, &row) {
                    all_rows.push(parsed_row);
                }
            }
        }

        info!("Received {} rows from {}", all_rows.len(), table_name);
        Ok(all_rows)
    }

    /// Parse a single row based on schema
    fn parse_row(
        &self,
        schema: &Option<proto::TableSchema>,
        row: &TableRow,
    ) -> Option<HashMap<String, Value>> {
        let schema = schema.as_ref()?;
        let columns = &schema.columns;

        if columns.len() != row.values.len() {
            error!(
                "Schema/row mismatch: {} columns vs {} values",
                columns.len(),
                row.values.len()
            );
            return None;
        }

        let mut result = HashMap::new();
        for (col, value) in columns.iter().zip(row.values.iter()) {
            let val = match &value.kind {
                Some(proto::value::Kind::StringVal(s)) => Value::String(s.clone()),
                Some(proto::value::Kind::Int64Val(i)) => Value::Number((*i).into()),
                Some(proto::value::Kind::Uint64Val(u)) => Value::Number((*u).into()),
                Some(proto::value::Kind::Float64Val(f)) => {
                    Value::Number(serde_json::Number::from_f64(*f).unwrap_or(serde_json::Number::from(0)))
                }
                Some(proto::value::Kind::BoolVal(b)) => Value::Bool(*b),
                Some(proto::value::Kind::TimestampMs(ts)) => Value::Number((*ts).into()),
                Some(proto::value::Kind::DurationUs(d)) => Value::Number((*d).into()),
                Some(proto::value::Kind::NullVal(_)) => Value::Null,
                _ => continue,
            };
            result.insert(col.name.clone(), val);
        }

        Some(result)
    }
}

#[async_trait]
impl DataCollector for GrpcPullCollector {
    fn collection_method(&self) -> CollectionMethod {
        CollectionMethod::GrpcPull
    }

    fn can_collect_table(&self, table: &TableConfig) -> bool {
        // gRPC pull can collect from many tables
        let table_type = PullTableType::from_table_name(&table.source_table);
        match table_type {
            PullTableType::Other(_) => false,
            _ => true,
        }
    }

    async fn initialize(&mut self) -> Result<(), CollectionError> {
        // Test the connection by listing tables
        let mut client = self.get_client().await?;

        let request = ListTablesRequest {
            pattern: Some("%".to_string()),
        };

        let response = client
            .list_tables(request)
            .await
            .map_err(|e| CollectionError::ConnectionError(format!("ListTables failed: {}", e)))?;

        let tables = response.into_inner().tables;
        info!(
            "GrpcPull collector initialized for instance {}, available tables: {:?}",
            self.instance,
            tables.iter().map(|t| t.name.clone()).collect::<Vec<_>>()
        );

        Ok(())
    }

    fn set_output_sender(&mut self, _sender: vector::SourceSender) {
        // GrpcPullCollector is pull-based, doesn't use output sender
    }

    async fn collect_table_data(
        &self,
        table: &TableConfig,
    ) -> Result<CollectionResult, CollectionError> {
        let start_time = std::time::Instant::now();

        // Query the table
        let rows = self
            .query_table(
                table.source_table.as_str(),
                table.where_clause.as_deref(),
            )
            .await?;

        let row_count = rows.len();
        let duration_ms = start_time.elapsed().as_millis() as u64;

        info!(
            "gRPC pull collected {} rows from {} in {}ms",
            row_count,
            table.source_table,
            duration_ms
        );

        Ok(CollectionResult {
            data: rows,
            metadata: CollectionMetadata {
                instance: self.instance.clone(),
                table_config: table.clone(),
                collection_method: CollectionMethod::GrpcPull,
                timestamp: chrono::Utc::now(),
                row_count,
                duration_ms,
                extra: HashMap::new(),
            },
        })
    }

    async fn health_check(&self) -> Result<(), CollectionError> {
        // Try to get the client to check connection
        let _ = self.get_client().await?;
        Ok(())
    }
}
