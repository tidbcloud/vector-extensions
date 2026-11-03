# Alicloud OSS Sink — Architecture

## Purpose

Upload batched, encoded observability events to Alibaba Cloud OSS using the
S3-compatible PutObject API, authenticated via ACK RRSA (OIDC) → STS temporary
credentials. Replaces static AK/SK previously required when using Vector's
built-in `aws_s3` sink against OSS endpoints.

## Architecture

```
Events → alicloud_oss sink
           ├─ batch / encode / compress (s3_common)
           ├─ AliyunOidcCredentialsProvider (RRSA → STS, refreshable)
           └─ AWS SDK S3 client → OSS endpoint (PutObject)
```

## Configuration

Sink type: `alicloud_oss`

Fields mirror Vector `aws_s3` for drop-in migration of existing templates:

| Field | Notes |
|-------|--------|
| `bucket` | OSS bucket name |
| `key_prefix` | Templateable object key prefix |
| `endpoint` / `region` | OSS endpoint (e.g. `http://oss-cn-hangzhou-internal.aliyuncs.com`) |
| `encoding` / `compression` / `batch` | Same as `aws_s3` |
| `filename_*` | Same as `aws_s3` |
| `force_path_style` | Default `false` (virtual-hosted style) |
| `auth.role_arn` | Optional; defaults to `ALIBABA_CLOUD_ROLE_ARN` |

### Auth (RRSA)

Required environment (injected by ACK RRSA / pod-identity webhook):

- `ALIBABA_CLOUD_ROLE_ARN`
- `ALIBABA_CLOUD_OIDC_TOKEN_FILE`
- `ALIBABA_CLOUD_OIDC_PROVIDER_ARN`

Credentials are refreshed before STS expiry (5-minute skew).

## Migration from `aws_s3`

```yaml
# before
type: aws_s3
endpoint: "http://oss-{{ .Region }}-internal.aliyuncs.com"
# + AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env

# after
type: alicloud_oss
endpoint: "http://oss-{{ .Region }}-internal.aliyuncs.com"
# + serviceAccount RRSA annotations; no static AK/SK
```

## Dependencies

- Vector `s3_common` (batching, partitioning, PutObject service)
- `aws-sdk-s3` against OSS S3-compatible API
- `common::aliyun_sts` for AssumeRoleWithOIDC
