// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use super::grpc_server::proto::Statement;

/// Field data types supported in the contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    String,
    Int64,
    Uint64,
    Float64,
    Bool,
    Bytes,
    Timestamp,
    Duration,
}

/// A field requirement in the contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldRequirement {
    /// Field name
    pub name: String,

    /// Expected data type
    #[serde(rename = "type")]
    pub field_type: FieldType,

    /// Human-readable description
    #[serde(default)]
    pub description: String,

    /// Default value if not provided (for optional fields)
    #[serde(default)]
    pub default_value: Option<String>,
}

/// The requirements contract that Vector publishes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequirementsContract {
    /// Contract version
    pub version: String,

    /// Publisher identifier
    #[serde(default = "default_publisher")]
    pub publisher: String,

    /// Required fields (TiDB MUST provide)
    #[serde(default)]
    pub required_fields: Vec<FieldRequirement>,

    /// Optional fields (TiDB CAN provide)
    #[serde(default)]
    pub optional_fields: Vec<FieldRequirement>,
}

fn default_publisher() -> String {
    "vector-extensions".to_string()
}

impl Default for RequirementsContract {
    fn default() -> Self {
        Self {
            version: "1.0.0".to_string(),
            publisher: default_publisher(),
            required_fields: default_required_fields(),
            optional_fields: Vec::new(),
        }
    }
}

/// Returns the default required fields.
fn default_required_fields() -> Vec<FieldRequirement> {
    vec![
        FieldRequirement {
            name: "digest".to_string(),
            field_type: FieldType::String,
            description: "SQL statement digest (fingerprint)".to_string(),
            default_value: None,
        },
        FieldRequirement {
            name: "exec_count".to_string(),
            field_type: FieldType::Int64,
            description: "Total execution count in the window".to_string(),
            default_value: None,
        },
        FieldRequirement {
            name: "sum_latency_us".to_string(),
            field_type: FieldType::Int64,
            description: "Total latency in microseconds".to_string(),
            default_value: None,
        },
    ]
}

/// Validates incoming statements against the requirements contract.
pub struct ContractValidator {
    contract: RequirementsContract,
    required_fields: HashMap<String, FieldRequirement>,
}

impl ContractValidator {
    /// Creates a new ContractValidator.
    pub fn new(contract_path: Option<&str>) -> Self {
        let contract = match contract_path {
            Some(path) => Self::load_contract(path).unwrap_or_else(|e| {
                warn!("Failed to load contract from {}: {}, using defaults", path, e);
                RequirementsContract::default()
            }),
            None => {
                info!("No contract path specified, using default contract");
                RequirementsContract::default()
            }
        };

        let required_fields: HashMap<String, FieldRequirement> = contract
            .required_fields
            .iter()
            .map(|f| (f.name.clone(), f.clone()))
            .collect();

        info!(
            "Contract loaded: version={}, required_fields={}, optional_fields={}",
            contract.version,
            contract.required_fields.len(),
            contract.optional_fields.len()
        );

        Self {
            contract,
            required_fields,
        }
    }

    /// Loads a contract from a YAML file.
    fn load_contract<P: AsRef<Path>>(path: P) -> Result<RequirementsContract, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let contract: RequirementsContract = serde_yaml::from_str(&content)?;
        Ok(contract)
    }

    /// Validates a statement against the contract.
    pub fn validate(&self, stmt: &Statement) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Check required fields
        for (name, req) in &self.required_fields {
            match name.as_str() {
                "digest" => {
                    if stmt.digest.is_empty() {
                        return Err(format!("required field '{}' is empty", name).into());
                    }
                }
                "exec_count" => {
                    if stmt.exec_count <= 0 {
                        return Err(format!("required field '{}' must be > 0", name).into());
                    }
                }
                "sum_latency_us" => {
                    if stmt.sum_latency_us < 0 {
                        return Err(format!("required field '{}' must be >= 0", name).into());
                    }
                }
                // Add more field validations as needed
                _ => {
                    debug!("Unknown required field: {}", name);
                }
            }
        }

        Ok(())
    }

    /// Returns the contract.
    pub fn contract(&self) -> &RequirementsContract {
        &self.contract
    }

    /// Returns the contract as YAML for serving to clients.
    pub fn contract_yaml(&self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(&self.contract)
    }
}

/// Creates a sample contract YAML file.
pub fn create_sample_contract() -> String {
    let contract = RequirementsContract {
        version: "1.0.0".to_string(),
        publisher: "vector-extensions".to_string(),
        required_fields: vec![
            FieldRequirement {
                name: "digest".to_string(),
                field_type: FieldType::String,
                description: "SQL statement digest (fingerprint)".to_string(),
                default_value: None,
            },
            FieldRequirement {
                name: "exec_count".to_string(),
                field_type: FieldType::Int64,
                description: "Total execution count".to_string(),
                default_value: None,
            },
            FieldRequirement {
                name: "sum_latency_us".to_string(),
                field_type: FieldType::Int64,
                description: "Total latency in microseconds".to_string(),
                default_value: None,
            },
            FieldRequirement {
                name: "schema_name".to_string(),
                field_type: FieldType::String,
                description: "Database schema name".to_string(),
                default_value: None,
            },
        ],
        optional_fields: vec![
            FieldRequirement {
                name: "sum_disk_bytes".to_string(),
                field_type: FieldType::Int64,
                description: "Total disk I/O bytes".to_string(),
                default_value: Some("0".to_string()),
            },
            FieldRequirement {
                name: "p99_latency_us".to_string(),
                field_type: FieldType::Int64,
                description: "P99 latency in microseconds".to_string(),
                default_value: Some("0".to_string()),
            },
        ],
    };

    serde_yaml::to_string(&contract).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_contract() {
        let validator = ContractValidator::new(None);
        assert_eq!(validator.contract.required_fields.len(), 3);
    }

    #[test]
    fn test_sample_contract() {
        let yaml = create_sample_contract();
        assert!(yaml.contains("digest"));
        assert!(yaml.contains("exec_count"));
    }
}
