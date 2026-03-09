# Vector Extensions - AI Agent Control Guide

This document provides guidance for AI agents on how to understand, develop, and maintain this Vector extension project.

## Project Overview

This is a **Vector extension project** built with **Rust** that provides custom sources and sinks specifically designed for TiDB cluster observability and data synchronization. The project extends the official Vector data pipeline tool with domain-specific components.

## Project Structure

```
vector-extensions/
├── src/                          # Rust source code
│   ├── sources/                  # Custom Vector sources
│   │   ├── topsql/              # TopSQL data source
│   │   ├── topsql_v2/           # TopSQL v2 data source
│   │   ├── conprof/             # Continuous profiling data source
│   │   ├── system_tables/       # System tables data source
│   │   ├── mocked_topsql/       # Mocked TopSQL for testing
│   │   ├── keyviz/              # KeyViz data source
│   │   └── filename/            # Filename-based source
│   ├── sinks/                    # Custom Vector sinks
│   │   ├── deltalake/           # Delta Lake sink
│   │   ├── aws_s3_upload_file/  # AWS S3 file upload sink
│   │   ├── azure_blob_upload_file/ # Azure Blob file upload sink
│   │   ├── gcp_cloud_storage_upload_file/ # GCP Cloud Storage upload sink
│   │   ├── vm_import/            # VictoriaMetrics import sink
│   │   ├── topsql_data_deltalake/ # TopSQL data to Delta Lake
│   │   └── topsql_meta_deltalake/ # TopSQL metadata to Delta Lake
│   ├── common/                   # Shared components
│   │   ├── deltalake_writer/    # Delta Lake writer utilities
│   │   ├── topology/            # Topology fetching utilities
│   │   └── checkpointer.rs      # Checkpoint management
│   ├── utils/                    # Utility modules
│   ├── lib.rs                    # Library entry point
│   └── main.rs                   # Binary entry point
├── demo/                         # Demo cases for data synchronization
│   ├── app.py                   # Flask API server for demo
│   ├── scripts/                 # Setup and test scripts
│   ├── config/                  # Configuration files
│   └── tests/                   # Test scripts
├── spec/                         # Specifications
├── doc/v1/                       # Documentation
│   ├── readme.md                # User guide for demo
│   ├── arch.md                  # Architecture doc for demo
│   └── agent.md                 # Agent guide for demo
└── Cargo.toml                    # Rust project configuration
```

## Core Components

### Sources (Data Input)

Sources collect data from various TiDB cluster components:

1. **topsql** / **topsql_v2** - Collect TopSQL data from TiDB/TiKV clusters
2. **conprof** - Collect continuous profiling data from cluster components
3. **system_tables** - Collect data from system tables
4. **mocked_topsql** - Mock TopSQL source for testing
5. **keyviz** - Key visualization data source
6. **filename** - Filename-based source

### Sinks (Data Output)

Sinks write data to various destinations:

1. **deltalake** - Write data to Delta Lake format
2. **aws_s3_upload_file** - Upload files to AWS S3
3. **azure_blob_upload_file** - Upload files to Azure Blob Storage
4. **gcp_cloud_storage_upload_file** - Upload files to GCP Cloud Storage
5. **vm_import** - Import data to VictoriaMetrics
6. **topsql_data_deltalake** - Write TopSQL data to Delta Lake
7. **topsql_meta_deltalake** - Write TopSQL metadata to Delta Lake

### Common Components

Shared utilities used across sources and sinks:

1. **deltalake_writer** - Delta Lake writing utilities
2. **topology** - TiDB cluster topology fetching
3. **checkpointer** - Checkpoint management for data consistency

## Development Guidelines

### Adding a New Component

To add a new source or sink, follow these steps:

1. **Create the component module** in `src/sources/` or `src/sinks/`
2. **Implement the component** following Vector's component interface
3. **Register the component** in `src/main.rs` using `inventory::submit!`
4. **Add feature flag** in `Cargo.toml` if needed
5. **Create architecture documentation** in `src/{sources|sinks}/{component_name}/arch.md`

### Component Architecture Documentation

Each component has an `arch.md` file that describes:

- **Purpose**: What the component does
- **Architecture**: How it works internally
- **Configuration**: Available configuration options
- **Data Flow**: How data flows through the component
- **Dependencies**: External dependencies and requirements
- **Testing**: How to test the component

### Available Architecture Documents

All components have architecture documentation in their respective directories:

**Sources:**
- `src/sources/topsql/arch.md` - TopSQL source architecture
- `src/sources/topsql_v2/arch.md` - TopSQL v2 source architecture
- `src/sources/conprof/arch.md` - Continuous profiling source architecture
- `src/sources/system_tables/arch.md` - System tables source architecture
- `src/sources/mocked_topsql/arch.md` - Mocked TopSQL source architecture
- `src/sources/keyviz/arch.md` - KeyViz source architecture
- `src/sources/filename/arch.md` - Filename source architecture

**Sinks:**
- `src/sinks/deltalake/arch.md` - Delta Lake sink architecture
- `src/sinks/aws_s3_upload_file/arch.md` - AWS S3 upload sink architecture
- `src/sinks/azure_blob_upload_file/arch.md` - Azure Blob upload sink architecture
- `src/sinks/gcp_cloud_storage_upload_file/arch.md` - GCP Cloud Storage upload sink architecture
- `src/sinks/vm_import/arch.md` - VictoriaMetrics import sink architecture
- `src/sinks/topsql_data_deltalake/arch.md` - TopSQL data Delta Lake sink architecture
- `src/sinks/topsql_meta_deltalake/arch.md` - TopSQL metadata Delta Lake sink architecture

**Common:**
- `src/common/deltalake_writer/arch.md` - Delta Lake writer utilities architecture
- `src/common/topology/arch.md` - Topology fetching utilities architecture
- `src/common/checkpointer/arch.md` - Checkpoint management architecture

### Code Organization

- **Sources**: Located in `src/sources/`, each source is a self-contained module
- **Sinks**: Located in `src/sinks/`, each sink is a self-contained module
- **Common**: Shared code in `src/common/` for reuse across components
- **Utils**: General utilities in `src/utils/`

## Demo Directory

The `demo/` directory contains demonstration cases showing how to use Vector for data synchronization:

- **Purpose**: Showcase data synchronization use cases
- **Technology**: Python Flask API server
- **Use Case**: Slowlog backup from S3 to MySQL
- **Documentation**: See `doc/v1/` for detailed documentation

## Building and Testing

### Build Commands

```bash
# Development build
make build

# Release build
make build-release

# Cross-compilation for different architectures
make build-x86_64-unknown-linux-gnu
make build-aarch64-unknown-linux-gnu
make build-armv7-unknown-linux-gnueabihf
```

### Testing

```bash
# Run all tests
make test

# Check code
make check

# Lint code
make clippy

# Format code
make fmt
```

## Key Concepts

### Vector Extension Pattern

This project follows Vector's extension pattern:

1. **Component Registration**: Components are registered via `inventory::submit!`
2. **Configuration**: Components use `configurable_component` macro for config
3. **Type Safety**: Strong typing with Vector's type system
4. **Async Runtime**: Built on Tokio async runtime

### TiDB Cluster Integration

Components are designed to work with TiDB clusters:

- **Topology Discovery**: Automatic discovery of cluster components via PD
- **TLS Support**: Secure connections with TLS configuration
- **Multi-component**: Support for TiDB, TiKV, PD, TiFlash components

### Data Formats

- **Delta Lake**: Used for structured data storage
- **Parquet**: Columnar storage format
- **JSON**: Configuration and some data formats
- **Protobuf**: Communication with TiDB cluster components

## Documentation Structure

### Component Documentation

Each component should have:
- `arch.md` - Architecture documentation (in component directory)
- Code comments - Inline documentation in Rust code

### Project Documentation

- `README.md` - Project overview and build instructions
- `AGENTS.md` - This file, AI agent control guide
- `doc/v1/` - Demo documentation

## Common Tasks for AI Agents

### Understanding a Component

1. Read the component's `arch.md` file
2. Review the component's `mod.rs` file
3. Check configuration options in the config struct
4. Review the controller/processor implementation

### Modifying a Component

1. Understand the current implementation
2. Identify the change location
3. Follow Vector's component patterns
4. Update tests if needed
5. Update `arch.md` if architecture changes

### Adding a New Component

1. Create component directory structure
2. Implement Vector component traits
3. Register in `src/main.rs`
4. Create `arch.md` documentation
5. Add tests
6. Update this `AGENTS.md` if needed

### Debugging

1. Check Vector logs for errors
2. Review component-specific error handling
3. Verify configuration
4. Check topology connectivity (for cluster components)
5. Review checkpoint state (if applicable)

## Component-Specific Notes

### TopSQL Sources

- **topsql**: Original TopSQL implementation
- **topsql_v2**: Next-generation TopSQL with improved features
- Both connect to TiDB/TiKV to collect SQL execution data

### Delta Lake Sink

- Uses `deltalake` crate for Delta Lake operations
- Supports S3 as storage backend
- Handles schema evolution automatically

### Cloud Storage Sinks

- **aws_s3_upload_file**: AWS S3 file upload
- **azure_blob_upload_file**: Azure Blob Storage upload
- **gcp_cloud_storage_upload_file**: GCP Cloud Storage upload
- All support batch uploads and retry logic

### VictoriaMetrics Import

- Imports data to VictoriaMetrics via HTTP API
- Supports partitioning
- Handles batching and encoding

## Related Documentation

- **Component Architecture**: See `src/{sources|sinks}/{component}/arch.md`
- **Demo Documentation**: See `doc/v1/` directory
- **Vector Documentation**: https://vector.dev/docs/

## Maintenance Notes

- **Vector Version**: Based on Vector v0.49.0
- **Rust Edition**: 2021
- **Async Runtime**: Tokio
- **Testing**: Use Vector's testing utilities

## Getting Help

- Review component `arch.md` files
- Check Vector documentation
- Review existing component implementations as examples
- Check demo directory for usage examples
