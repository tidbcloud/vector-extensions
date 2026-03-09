# Mocked TopSQL Source - Architecture Documentation

## Overview

The Mocked TopSQL source is a testing component that generates mock TopSQL data for development and testing purposes without requiring a real TiDB cluster.

## Purpose

- Generate mock TopSQL data for testing
- Enable development without cluster access
- Support unit and integration testing
- Provide predictable test data

## Architecture

### Component Structure

```
Mocked TopSQL Source
├── Controller          # Mock data generation logic
└── Shutdown            # Graceful shutdown handling
```

### Data Flow

```
Mock Data Generator
    ↓ (Generate Events)
Controller
    ↓ (Vector Event)
Vector Pipeline
```

## Configuration

### MockedTopSQLConfig

```rust
pub struct MockedTopSQLConfig {
    // Configuration for mock data generation
    // Data patterns, generation rate, etc.
}
```

## Mock Data Generation

- Generates realistic TopSQL-like data
- Configurable data patterns
- Supports various SQL types
- Simulates cluster behavior

## Use Cases

- Unit testing
- Integration testing
- Development without cluster
- Performance testing

## Dependencies

- **vector**: Vector core library
