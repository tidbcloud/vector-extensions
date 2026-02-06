# VictoriaMetrics Import Sink - Architecture Documentation

## Overview

The VictoriaMetrics Import sink writes Vector events to VictoriaMetrics via its HTTP import API. It supports partitioning, batching, and efficient encoding for time-series data.

## Purpose

- Import Vector events to VictoriaMetrics
- Support time-series metrics and logs
- Enable high-performance data ingestion
- Support partitioning for scalability

## Architecture

### Component Structure

```
VM Import Sink
├── Sink              # Main sink implementation
├── Encoder           # Data encoding for VictoriaMetrics
└── Partition         # Partitioning logic
```

### Data Flow

```
Vector Events
    ↓
VM Import Sink
    ↓ (Encode & Partition)
HTTP Client
    ↓ (POST to /api/v1/import)
VictoriaMetrics
```

## Configuration

### VMImportConfig

```rust
pub struct VMImportConfig {
    pub endpoint: String,
    pub healthcheck_endpoint: Option<String>,
    pub tls: Option<TlsConfig>,
    pub request: TowerRequestConfig,
    pub batch: BatchConfig<VMImportDefaultBatchSettings>,
}
```

### Key Configuration Options

- **endpoint**: VictoriaMetrics import endpoint URL
- **healthcheck_endpoint**: Optional health check endpoint
- **tls**: TLS configuration for secure connections
- **request**: HTTP request configuration
- **batch**: Batching configuration

## Data Processing

1. **Event Reception**: Receive Vector events
2. **Encoding**: Encode events in VictoriaMetrics format
3. **Partitioning**: Partition events by labels/metrics
4. **Batching**: Accumulate events into batches
5. **HTTP Request**: Send batches via HTTP POST
6. **Response Handling**: Handle responses and errors

## Encoding

### VictoriaMetrics Format

- **Prometheus format**: For metrics
- **JSON Lines**: For logs
- **Native format**: Optimized binary format

### Partitioning

- Partition by metric name
- Partition by labels
- Distribute load across VictoriaMetrics instances

## Dependencies

- **vector**: Vector core library
- **reqwest**: HTTP client
- **hyper**: HTTP implementation
- **tower**: Request middleware

## Error Handling

- **HTTP Errors**: Retry with exponential backoff
- **Encoding Errors**: Skip invalid events, log errors
- **Network Errors**: Retry with backoff
- **Rate Limiting**: Handle 429 responses

## Performance Considerations

- **Batching**: Configurable batch sizes
- **Parallel Requests**: Multiple concurrent requests
- **Compression**: Gzip compression for HTTP requests
- **Connection Pooling**: Reuse HTTP connections

## Use Cases

- Metrics ingestion
- Log aggregation
- Time-series data storage
- Monitoring and alerting

## Health Checks

- Optional health check endpoint
- Validates VictoriaMetrics availability
- Ensures sink can write data
