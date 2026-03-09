# AWS S3 Upload File Sink - Architecture Documentation

## Overview

The AWS S3 Upload File sink uploads files to AWS S3, supporting batch uploads, retry logic, and ETag verification for data integrity.

## Purpose

- Upload files to AWS S3
- Support batch file operations
- Ensure data integrity with ETag verification
- Handle large file uploads efficiently

## Architecture

### Component Structure

```
AWS S3 Upload File Sink
├── Processor          # Main processing logic
├── Uploader           # S3 upload operations
└── ETag Calculator    # ETag calculation for verification
```

### Data Flow

```
Vector Events
    ↓
Processor
    ↓ (Create Files)
Uploader
    ↓ (Upload to S3)
AWS S3
```

## Configuration

### AwsS3UploadFileConfig

```rust
pub struct AwsS3UploadFileConfig {
    pub bucket: String,
    pub key_prefix: Option<String>,
    pub region: Option<RegionOrEndpoint>,
    pub auth: Option<AwsAuthentication>,
    // ... more fields
}
```

## File Processing

1. **Event Reception**: Receive Vector events
2. **File Creation**: Create files from events
3. **ETag Calculation**: Calculate ETag for verification
4. **S3 Upload**: Upload files to S3
5. **Verification**: Verify upload with ETag
6. **Cleanup**: Clean up temporary files

## Features

### Batch Upload

- Upload multiple files in parallel
- Configurable batch size
- Efficient resource usage

### ETag Verification

- Calculate ETag before upload
- Verify after upload
- Ensure data integrity

### Retry Logic

- Automatic retry on failures
- Exponential backoff
- Configurable retry limits

## Dependencies

- **aws-sdk-s3**: AWS S3 SDK
- **aws-config**: AWS configuration
- **md-5**: MD5 for ETag calculation

## Error Handling

- **Upload Failures**: Retry with backoff
- **Network Errors**: Retry with exponential backoff
- **Verification Failures**: Re-upload on mismatch

## Performance Considerations

- **Parallel Uploads**: Upload multiple files concurrently
- **Multipart Upload**: Support for large files
- **Connection Reuse**: Reuse S3 connections
