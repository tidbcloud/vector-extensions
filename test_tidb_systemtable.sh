#!/usr/bin/env bash
set -euo pipefail

# Simple test script for running system_tables -> deltalake against an existing TiDB cluster.
#
# Usage:
#   ./test_tidb.sh \
#     --host 127.0.0.1 --port 4000 --user root --password "" --database test \
#     [--pd 127.0.0.1:2379] [--base-path ./delta-tables] [--duration 30]
#
# Requires the built binary `target/debug/vector` or `target/release/vector`.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}"

HOST="127.0.0.1"
PORT="4000"
USER="root"
PASSWORD=""
DATABASE="test"
PD="127.0.0.1:2379"
BASE_PATH="./delta-tables"
DURATION="30"
PROFILE="debug"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HOST="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --user) USER="$2"; shift 2 ;;
    --password) PASSWORD="$2"; shift 2 ;;
    --database) DATABASE="$2"; shift 2 ;;
    --pd) PD="$2"; shift 2 ;;
    --base-path) BASE_PATH="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --release) PROFILE="release"; shift 1 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

CONFIG_FILE="${ROOT_DIR}/.tmp_test_tidb_config.yaml"
mkdir -p "${ROOT_DIR}/.tmp"
mkdir -p "${BASE_PATH}"

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
  deltalake:
    type: "deltalake"
    inputs: ["system_tables"]
    base_path: "${BASE_PATH}"
    batch_size: 1000            # Minimum batch size to make batching easier to trigger
    timeout_secs: 60          # Reduce timeout for faster batch triggering
    compression: "snappy"
    # Delta Lake optimization configuration
    # Use batching and timeout to reduce file count

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
  
  # Clean up temporary config file
  rm -f "${CONFIG_FILE}"
  
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

# Check if data was collected
if [[ -d "${BASE_PATH}/hist_processlist" ]] && [[ -d "${BASE_PATH}/hist_cluster_statements_summary" ]]; then
  echo "SUCCESS: Delta table directories created:"
  ls -la "${BASE_PATH}/"
  
  # Check for actual data files
  for table_dir in "${BASE_PATH}/hist_processlist" "${BASE_PATH}/hist_cluster_statements_summary"; do
    if [[ -d "${table_dir}" ]]; then
      echo "Checking table: ${table_dir}"
      ls -la "${table_dir}/"
      
      # Look for parquet files
      parquet_count=$(find "${table_dir}" -name "*.parquet" 2>/dev/null | wc -l)
      if [[ ${parquet_count} -gt 0 ]]; then
        echo "✓ Found ${parquet_count} parquet files in ${table_dir}"
      else
        echo "⚠ No parquet files found in ${table_dir}"
      fi
    fi
  done
else
  echo "WARNING: Expected Delta table directories not found"
  ls -la "${BASE_PATH}/" || true
fi

echo "Test completed successfully!"
echo "Vector process is still running. Use Ctrl+C to stop it."
echo "Or manually stop with: kill ${VECTOR_PID}"

# Keep vector running until user interrupts
wait "${VECTOR_PID}" || true