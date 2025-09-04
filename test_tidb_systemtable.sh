#!/usr/bin/env bash
set -euo pipefail

# Simple test script for running system_tables -> deltalake (S3) against an existing TiDB cluster.
#
# Usage:
#   ./test_tidb_systemtable.sh \
#     --host 127.0.0.1 --port 4000 --user root --password "" --database test \
#     [--pd 127.0.0.1:2379] [--bucket my-bucket] [--region us-east-1] [--duration 30] \
#     [--aws-access-key-id KEY] [--aws-secret-access-key SECRET] [--aws-session-token TOKEN] \
#     [--assume-role ROLE_ARN] [--external-id ID] [--role-session-name NAME]
#
# Requires the built binary `target/debug/vector` or `target/release/vector`.
# Requires AWS credentials to be configured via CLI args or environment variables.
#
# Examples:
#   # Using static credentials
#   ./test_tidb_systemtable.sh --bucket my-bucket --region us-east-1 \
#     --aws-access-key-id AKIAIOSFODNN7EXAMPLE \
#     --aws-secret-access-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
#
#   # Using assume role
#   ./test_tidb_systemtable.sh --bucket my-bucket --region us-east-1 \
#     --assume-role arn:aws:iam::123456789012:role/VectorRole
#
#   # Using default AWS credential chain (environment variables, ~/.aws/config, IAM role)
#   ./test_tidb_systemtable.sh --bucket my-bucket --region us-east-1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}"

HOST="127.0.0.1"
PORT="4000"
USER="root"
PASSWORD=""
DATABASE="test"
PD="127.0.0.1:2379"
BUCKET=""
REGION="us-east-1"
DURATION="30"
PROFILE="debug"
AWS_ACCESS_KEY_ID=""
AWS_SECRET_ACCESS_KEY=""
AWS_SESSION_TOKEN=""
ASSUME_ROLE=""
EXTERNAL_ID=""
ROLE_SESSION_NAME="vector-deltalake"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HOST="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --user) USER="$2"; shift 2 ;;
    --password) PASSWORD="$2"; shift 2 ;;
    --database) DATABASE="$2"; shift 2 ;;
    --pd) PD="$2"; shift 2 ;;
    --bucket) BUCKET="$2"; shift 2 ;;
    --region) REGION="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --aws-access-key-id) AWS_ACCESS_KEY_ID="$2"; shift 2 ;;
    --aws-secret-access-key) AWS_SECRET_ACCESS_KEY="$2"; shift 2 ;;
    --aws-session-token) AWS_SESSION_TOKEN="$2"; shift 2 ;;
    --assume-role) ASSUME_ROLE="$2"; shift 2 ;;
    --external-id) EXTERNAL_ID="$2"; shift 2 ;;
    --role-session-name) ROLE_SESSION_NAME="$2"; shift 2 ;;
    --release) PROFILE="release"; shift 1 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

CONFIG_FILE="${ROOT_DIR}/.tmp_test_tidb_config.yaml"
mkdir -p "${ROOT_DIR}/.tmp"

# Validate required S3 parameters
if [[ -z "${BUCKET}" ]]; then
  echo "ERROR: --bucket is required for S3 storage"
  exit 1
fi

# Check if AWS credentials are provided (either via CLI args or environment)
if [[ -z "${AWS_ACCESS_KEY_ID}" ]] && [[ -z "${AWS_ACCESS_KEY_ID:-}" ]] && [[ -z "${ASSUME_ROLE}" ]]; then
  echo "WARNING: No AWS credentials specified. Will use default AWS credential chain."
  echo "Make sure AWS credentials are configured via environment variables, ~/.aws/config, or IAM role."
fi

cat >"${CONFIG_FILE}" <<EOF
sources:
  system_tables:
    type: "system_tables"
    pd_address: "${PD}"
    database_username: "${USER}"
    database_password: "${PASSWORD}"
    database_host: "${HOST}"
    database_port: ${PORT}
    database_name: "${DATABASE}"
    database_max_connections: 10
    database_connect_timeout: 30
    short_interval: 5
    long_interval: 60
    retention_days: 1
    tables:
      - source_schema: "information_schema"
        source_table: "PROCESSLIST"
        dest_table: "hist_processlist"
        collection_interval: "short"
        where_clause: "command != 'Sleep'"
        enabled: true
      - source_schema: "information_schema"
        source_table: "CLUSTER_STATEMENTS_SUMMARY"
        dest_table: "hist_cluster_statements_summary"
        collection_interval: "long"
        enabled: true

sinks:
  deltalake_s3:
    type: "deltalake"
    inputs: ["system_tables"]
    # S3 base path where Delta Lake tables will be stored
    base_path: "s3://${BUCKET}/deltalake-tables"
    bucket: "${BUCKET}"
    region: "${REGION}"
EOF

# Add authentication section based on provided credentials
if [[ -n "${ASSUME_ROLE}" ]]; then
cat >>"${CONFIG_FILE}" <<EOF
    auth:
      assume_role: "${ASSUME_ROLE}"
EOF
  if [[ -n "${EXTERNAL_ID}" ]]; then
cat >>"${CONFIG_FILE}" <<EOF
      external_id: "${EXTERNAL_ID}"
EOF
  fi
elif [[ -n "${AWS_ACCESS_KEY_ID}" ]]; then
cat >>"${CONFIG_FILE}" <<EOF
    auth:
      access_key_id: "${AWS_ACCESS_KEY_ID}"
      secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
EOF
  if [[ -n "${AWS_SESSION_TOKEN}" ]]; then
cat >>"${CONFIG_FILE}" <<EOF
      session_token: "${AWS_SESSION_TOKEN}"
EOF
  fi
else
  # Use default credential chain when no explicit credentials are provided
  # For Default variant, we don't add auth section at all, let it use default
  echo "    # Using default AWS credential chain (no explicit auth config needed)"
fi
# Note: If no auth is specified, the default AWS credential chain will be used

# Continue with the rest of the configuration
cat >>"${CONFIG_FILE}" <<EOF
    
    # S3 options (commented out - you can uncomment as needed)
    #storage_class: "STANDARD"
    #server_side_encryption: "AES256"
    #force_path_style: false
    
    # Delta Lake configuration
    batch_size: 1000            # Minimum batch size to make batching easier to trigger
    timeout_secs: 60            # Reduce timeout for faster batch triggering
    compression: "snappy"
    
    # Storage options for Delta Lake
    storage_options:
      AWS_STORAGE_ALLOW_HTTP: "true"
    
    # Acknowledgments
    acknowledgements:
      enabled: false

data_dir: "/tmp/vector"
log_schema:
  source_type: "log"
  timestamp: "timestamp"
EOF

BIN_PATH="${ROOT_DIR}/target/${PROFILE}/vector"
if [[ ! -x "${BIN_PATH}" ]]; then
  echo "Binary not found at ${BIN_PATH}. Building..."
  if [[ "${PROFILE}" == "release" ]]; then
    (cd "${ROOT_DIR}" && cargo build --release)
  else
    (cd "${ROOT_DIR}" && cargo build)
  fi
fi



# Start vector in background
echo "Starting vector..."
"${BIN_PATH}" --config "${CONFIG_FILE}" &
VECTOR_PID=$!

# Function to cleanup on exit
cleanup() {
  echo "Cleaning up..."
  if kill -0 "${VECTOR_PID}" 2>/dev/null; then
    echo "Stopping vector (PID: ${VECTOR_PID})..."
    kill "${VECTOR_PID}"
    wait "${VECTOR_PID}" 2>/dev/null || true
  fi
  
  # Clean up temporary config file (commented out for debugging)
  # rm -f "${CONFIG_FILE}"
  
  echo "Cleanup complete."
}

# Set trap to cleanup on script exit
trap cleanup EXIT INT TERM

echo "Vector started with PID: ${VECTOR_PID}"
echo "Waiting ${DURATION} seconds for data collection..."

# Wait for the specified duration
sleep "${DURATION}"

echo "Test duration completed. Checking results..."

# Check if vector is still running
if ! kill -0 "${VECTOR_PID}" 2>/dev/null; then
  echo "ERROR: Vector process died unexpectedly!"
  exit 1
fi

# Check if data was collected (S3 storage)
echo "Checking S3 bucket: s3://${BUCKET}/deltalake-tables"

# Check if AWS CLI is available for verification
if command -v aws >/dev/null 2>&1; then
  echo "Using AWS CLI to check S3 bucket contents..."
  
  # Set AWS environment variables if provided
  export_cmd=""
  if [[ -n "${AWS_ACCESS_KEY_ID}" ]]; then
    export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}"
  fi
  if [[ -n "${AWS_SECRET_ACCESS_KEY}" ]]; then
    export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}"
  fi
  if [[ -n "${AWS_SESSION_TOKEN}" ]]; then
    export AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN}"
  fi
  
  # Check for Delta table directories in S3
  for table_name in "hist_processlist" "hist_cluster_statements_summary"; do
    table_path="s3://${BUCKET}/deltalake-tables/${table_name}/"
    echo "Checking table: ${table_path}"
    
    # List objects in the table directory
    if aws s3 ls "${table_path}" --region "${REGION}" >/dev/null 2>&1; then
      object_count=$(aws s3 ls "${table_path}" --recursive --region "${REGION}" | wc -l)
      if [[ ${object_count} -gt 0 ]]; then
        echo "✓ Found ${object_count} objects in ${table_path}"
        
        # Count parquet files specifically
        parquet_count=$(aws s3 ls "${table_path}" --recursive --region "${REGION}" | grep -c "\.parquet$" || echo "0")
        if [[ ${parquet_count} -gt 0 ]]; then
          echo "✓ Found ${parquet_count} parquet files in ${table_path}"
        else
          echo "⚠ No parquet files found in ${table_path}"
        fi
      else
        echo "⚠ No objects found in ${table_path}"
      fi
    else
      echo "⚠ Unable to access ${table_path} or directory doesn't exist"
    fi
  done
else
  echo "AWS CLI not found. Cannot verify S3 bucket contents."
  echo "Please install AWS CLI to verify data was written to S3."
  echo "Expected S3 locations:"
  echo "  - s3://${BUCKET}/deltalake-tables/hist_processlist/"
  echo "  - s3://${BUCKET}/deltalake-tables/hist_cluster_statements_summary/"
fi

echo "Test completed successfully!"
echo "Data should be written to S3 bucket: s3://${BUCKET}/deltalake-tables"
echo "Vector process is still running. Use Ctrl+C to stop it."
echo "Or manually stop with: kill ${VECTOR_PID}"

# Keep vector running until user interrupts
wait "${VECTOR_PID}" || true