# Delta Lake S3 Support with Complete AWS Authentication

## Summary

This enhancement adds complete S3 storage support to the Delta Lake sink, enabling it to write Delta Lake tables directly to S3 with full AWS authentication support, **completely mirroring the `aws_s3_upload_file` implementation**. The implementation now provides the same level of AWS integration and authentication options as the existing `aws_s3_upload_file` sink.

## Changes Made

### 1. Enhanced Configuration (`src/sinks/deltalake/mod.rs`)

**Completely mirrored** `aws_s3_upload_file` configuration by adding these fields to `DeltaLakeConfig`:

- `bucket`: S3 bucket name for remote storage
- `options`: Complete S3Options support (flattened, same as aws_s3_upload_file)
- `region`: AWS region or endpoint configuration (flattened, same as aws_s3_upload_file)
- `tls`: TLS configuration support (same as aws_s3_upload_file)
- `auth`: Full AwsAuthentication support with default (same as aws_s3_upload_file)
- `force_path_style`: S3 addressing style configuration (same as aws_s3_upload_file)

### 2. Complete S3Service Integration

**Mirrored `aws_s3_upload_file` architecture** by implementing:

- `create_service()`: Creates S3Service using s3_common::config::create_service (identical to aws_s3_upload_file)
- Full S3Service integration for authentication and configuration
- Real S3 healthcheck using s3_common::config::build_healthcheck (identical to aws_s3_upload_file)
- Automatic AWS region and endpoint configuration from S3Service

### 3. Enhanced Storage Options

The sink now automatically configures Delta Lake storage options based on AWS configuration:

- `AWS_STORAGE_ALLOW_HTTP`: Enables HTTP for local testing
- `AWS_REGION`: Set from Vector's AWS configuration
- `AWS_ENDPOINT_URL`: Set for custom endpoints
- `AWS_S3_ADDRESSING_STYLE`: Configures path-style or virtual-hosted-style addressing
- Full integration with Vector's AWS credential chain

### 4. Enhanced Writer (`src/sinks/deltalake/writer.rs`)

Updated `DeltaLakeWriter` to:

- Detect S3 URLs in table paths (`s3://` prefix)
- Pass storage options to Delta Lake table builder
- Handle both local filesystem and S3 storage paths
- Use Delta Lake's `with_storage_options()` API for configuration

### 5. Improved Processor (`src/sinks/deltalake/processor.rs`)

Enhanced `DeltaLakeSink` to:

- Properly construct S3 table paths by appending table names to S3 base paths
- Maintain backward compatibility with local filesystem paths

## Configuration Example

```yaml
sinks:
  deltalake_s3:
    type: "deltalake"
    inputs: ["your_source"]
    
    # S3 configuration
    base_path: "s3://your-bucket/deltalake-tables"
    bucket: "your-bucket"
    region: "us-west-2"
    
    # Assume role authentication
    auth:
      assume_role: "arn:aws:iam::123456789012:role/YourDeltaLakeRole"
    
    # Delta Lake settings
    batch_size: 1000
    timeout_secs: 30
    compression: "snappy"
    
    # Optional S3 storage options
    storage_options:
      AWS_STORAGE_ALLOW_HTTP: "true"
    
    acknowledgements:
      enabled: true
```

## Authentication Methods Supported

**Identical to `aws_s3_upload_file`**, supporting all AWS authentication methods:

1. **Assume Role**: Use `auth.assume_role` with optional `external_id` and `role_session_name`
2. **Static Credentials**: Use `auth.access_key_id`, `auth.secret_access_key`, and optional `auth.token`
3. **Default Credential Chain**: Uses AWS standard credential chain:
   - Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`)
   - IAM Instance Profile (when running on EC2)
   - AWS Config/Credentials Files (`~/.aws/credentials`, `~/.aws/config`)
   - ECS/Fargate task roles
   - Web Identity Token (for OIDC/SAML)
4. **Custom Endpoints**: Full support for custom S3-compatible endpoints
5. **TLS Configuration**: Complete TLS options for secure connections

## Backward Compatibility

- Existing local filesystem configurations remain unchanged
- All existing Delta Lake functionality is preserved
- S3 support is opt-in via the new configuration fields

## Benefits

1. **Centralized Storage**: Store Delta Lake tables in S3 for centralized access
2. **Scalability**: Leverage S3's scalability and durability
3. **Security**: Use AWS assume roles for secure, temporary access
4. **Cost Efficiency**: Benefit from S3's cost-effective storage tiers
5. **Integration**: Seamless integration with existing AWS infrastructure

## Testing

The implementation has been tested to compile successfully with the existing Vector codebase and Delta Lake dependencies.
