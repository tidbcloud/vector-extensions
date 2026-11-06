# Alicloud OSS Sink — Architecture

## Purpose

Upload batched, encoded observability events to Alibaba Cloud OSS using the
S3-compatible PutObject API, authenticated via Aliyun STS temporary credentials
(no long-lived static AK/SK). Replaces Vector's built-in `aws_s3` sink against
OSS endpoints.

## Architecture

```
Events → alicloud_oss sink
           ├─ batch / encode / compress (s3_common)
           ├─ AliyunRoleCredentialsProvider (refreshable)
           │    ├─ RRSA OIDC → AssumeRoleWithOIDC (if ALIBABA_CLOUD_OIDC_* set)
           │    └─ else ECS/ACK node RAM role (IMDS) → STS AssumeRole
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
| `auth.assume_role` | RAM role ARN (`acs:ram::...`). Alias: `auth.role_arn`. Defaults to `ALIBABA_CLOUD_ROLE_ARN` |

### Auth

**Preferred (ACK/ECS node role):**

```yaml
type: alicloud_oss
auth:
  assume_role: "acs:ram::123456789012:role/o11y-agent-writer"
# Pod uses the node ECS RAM role from IMDS; that role must be trusted to AssumeRole
# into the writer role. No static AK/SK, no RRSA ServiceAccount annotation.
```

**Optional RRSA:** if `ALIBABA_CLOUD_OIDC_TOKEN_FILE` / `ALIBABA_CLOUD_OIDC_PROVIDER_ARN`
are injected, the same `assume_role` uses `AssumeRoleWithOIDC` instead.

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
auth:
  assume_role: "{{ .RoleARN }}"
# + node role trust to RoleARN; no static AK/SK
```

## Dependencies

- Vector `s3_common` (batching, partitioning, PutObject service)
- `aws-sdk-s3` against OSS S3-compatible API
- `common::aliyun_sts` for RRSA OIDC and node-role AssumeRole
