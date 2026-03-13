# Azure Blob Upload File Sink - Architecture Documentation

## Overview

The Azure Blob Upload File sink uploads files to Azure Blob Storage, supporting batch uploads and retry logic for reliable file operations.

## Purpose

- Upload files to Azure Blob Storage
- Support batch file operations
- Handle large file uploads efficiently
- Provide reliable file transfer

## Architecture

### Component Structure

```
Azure Blob Upload File Sink
├── Processor          # Main processing logic
└── Uploader           # Azure Blob upload operations
```

### Data Flow

```
Vector Events
    ↓
Processor
    ↓ (Create Files)
Uploader
    ↓ (Upload to Azure Blob)
Azure Blob Storage
```

## Configuration

### AzureBlobUploadFileConfig

```rust
pub struct AzureBlobUploadFileConfig {
    pub container: String,
    pub blob_prefix: Option<String>,
    pub connection_string: Option<String>,
    // ... more fields
}
```

## File Processing

1. **Event Reception**: Receive Vector events
2. **File Creation**: Create files from events
3. **Azure Upload**: Upload files to Azure Blob Storage
4. **Verification**: Verify upload success
5. **Cleanup**: Clean up temporary files

## Features

### Batch Upload

- Upload multiple files in parallel
- Configurable batch size
- Efficient resource usage

### Retry Logic

- Automatic retry on failures
- Exponential backoff
- Configurable retry limits

## Dependencies

- **azure_storage_blobs**: Azure Blob Storage SDK
- **reqwest**: HTTP client

## Error Handling

- **Upload Failures**: Retry with backoff
- **Network Errors**: Retry with exponential backoff
- **Authentication Errors**: Handle credential issues

## Performance Considerations

- **Parallel Uploads**: Upload multiple files concurrently
- **Connection Reuse**: Reuse Azure connections
- **Chunked Upload**: Support for large files
