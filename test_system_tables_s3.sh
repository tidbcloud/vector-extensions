#!/bin/bash

# System Tables Source S3 Testing Script
# For testing system_tables source with S3 Delta Lake output

set -e

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Log functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Default configuration values
HOST="127.0.0.1"
PORT="4000"
USER="root"
PASSWORD=""
DATABASE="test"
PD="127.0.0.1:2379"
BUCKET=""
REGION="us-west-2"
DURATION="30"
PROFILE="release"
COLLECTION_METHOD="coprocessor"  # Default to coprocessor, can be changed to sql
AWS_ACCESS_KEY_ID=""
AWS_SECRET_ACCESS_KEY=""
AWS_SESSION_TOKEN=""
ASSUME_ROLE=""
EXTERNAL_ID=""
ROLE_SESSION_NAME="vector-deltalake"

# Check dependencies
check_dependencies() {
    log_info "Checking dependencies..."

    # Check Rust
    if ! command -v cargo &> /dev/null; then
        log_error "cargo not found, please install Rust"
        exit 1
    fi

    # Check AWS CLI for verification
    if ! command -v aws &> /dev/null; then
        log_warning "aws CLI not found, S3 verification will be skipped"
    fi

    log_success "Dependencies check completed"
}

# Parse command line arguments
parse_arguments() {
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
            --collection-method) COLLECTION_METHOD="$2"; shift 2 ;;
            --aws-access-key-id) AWS_ACCESS_KEY_ID="$2"; shift 2 ;;
            --aws-secret-access-key) AWS_SECRET_ACCESS_KEY="$2"; shift 2 ;;
            --aws-session-token) AWS_SESSION_TOKEN="$2"; shift 2 ;;
            --assume-role) ASSUME_ROLE="$2"; shift 2 ;;
            --external-id) EXTERNAL_ID="$2"; shift 2 ;;
            --role-session-name) ROLE_SESSION_NAME="$2"; shift 2 ;;
            --release) PROFILE="release"; shift 1 ;;
            -h|--help) show_help; exit 0 ;;
            *) log_error "Unknown argument: $1"; show_help; exit 1 ;;
        esac
    done
}

# Validate configuration
validate_config() {
    log_info "Validating configuration..."

    # Validate required S3 parameters
    if [[ -z "${BUCKET}" ]]; then
        log_error "--bucket is required for S3 storage"
        exit 1
    fi

    # Validate collection method
    if [[ "${COLLECTION_METHOD}" != "sql" && "${COLLECTION_METHOD}" != "coprocessor" ]]; then
        log_error "collection-method must be 'sql' or 'coprocessor'"
        exit 1
    fi

    # Check if AWS credentials are provided (either via CLI args or environment)
    if [[ -z "${AWS_ACCESS_KEY_ID}" ]] && [[ -z "${AWS_ACCESS_KEY_ID:-}" ]] && [[ -z "${ASSUME_ROLE}" ]]; then
        log_warning "No AWS credentials specified. Will use default AWS credential chain."
        log_info "Make sure AWS credentials are configured via environment variables, ~/.aws/config, or IAM role."
    fi

    log_success "Configuration validation completed"
}

# Build project
build_project() {
    log_info "Building project..."
    if [[ "${PROFILE}" == "release" ]]; then
        cargo build --release
    else
        cargo build
    fi
    
    if [ $? -eq 0 ]; then
        log_success "Project build successful"
    else
        log_error "Project build failed"
        exit 1
    fi
}

# Generate Vector configuration file
create_vector_config() {
    local config_file="test_config_s3_${COLLECTION_METHOD}.toml"
    
    echo "Creating Vector configuration file: $config_file (collection: $COLLECTION_METHOD, S3: $BUCKET)" >&2

    cat > "$config_file" <<EOF
# Vector Configuration File - System Tables Source S3 Test
# Collection method: $COLLECTION_METHOD, Output: S3 Delta Lake

[sources.tidb_system_tables]
type = "system_tables"

# Database connection configuration
database_username = "$USER"
database_password = "$PASSWORD"
database_host = "$HOST"
database_port = $PORT
database_name = "$DATABASE"
database_max_connections = 10
database_connect_timeout = 30

# PD configuration (for topology discovery)
pd_address = "$PD"

# Collection configuration
short_interval = 5
long_interval = 60
retention_days = 1
topology_fetch_interval_seconds = 10.0

# Collection method configuration
collection_method = "$COLLECTION_METHOD"

# Tables to collect configuration
#[[sources.tidb_system_tables.tables]]
#source_schema = "information_schema"
#source_table = "PROCESSLIST"
#dest_table = "hist_processlist"
#collection_interval = "short"
#where_clause = "command != 'Sleep'"
#enabled = true

[[sources.tidb_system_tables.tables]]
source_schema = "information_schema"
source_table = "CLUSTER_STATEMENTS_SUMMARY"
dest_table = "cluster_statements_summary"
collection_interval = "short"
enabled = true

EOF

    # Add S3 Delta Lake sink configuration
    cat >> "$config_file" <<EOF
# Output to S3 Delta Lake
[sinks.deltalake_s3]
type = "deltalake"
inputs = ["tidb_system_tables"]

# S3 base path where Delta Lake tables will be stored
base_path = "s3://$BUCKET/deltalake-tables"
bucket = "$BUCKET"
region = "$REGION"

EOF

    # Add authentication section based on provided credentials
    if [[ -n "${ASSUME_ROLE}" ]]; then
        cat >> "$config_file" <<EOF
# AWS authentication using assume role
[sinks.deltalake_s3.auth]
assume_role = "$ASSUME_ROLE"
EOF
        if [[ -n "${EXTERNAL_ID}" ]]; then
            cat >> "$config_file" <<EOF
external_id = "$EXTERNAL_ID"
EOF
        fi
        if [[ -n "${ROLE_SESSION_NAME}" ]]; then
            cat >> "$config_file" <<EOF
role_session_name = "$ROLE_SESSION_NAME"
EOF
        fi
    elif [[ -n "${AWS_ACCESS_KEY_ID}" ]]; then
        cat >> "$config_file" <<EOF
# AWS authentication using static credentials
[sinks.deltalake_s3.auth]
access_key_id = "$AWS_ACCESS_KEY_ID"
secret_access_key = "$AWS_SECRET_ACCESS_KEY"
EOF
        if [[ -n "${AWS_SESSION_TOKEN}" ]]; then
            cat >> "$config_file" <<EOF
session_token = "$AWS_SESSION_TOKEN"
EOF
        fi
    else
        cat >> "$config_file" <<EOF
# Using default AWS credential chain (no explicit auth config needed)
EOF
    fi

    # Continue with the rest of the configuration
    cat >> "$config_file" <<EOF

# S3 options
storage_class = "STANDARD"
server_side_encryption = "AES256"
force_path_style = false

# Delta Lake configuration
batch_size = 1000
timeout_secs = 60
compression = "snappy"

# Storage options for Delta Lake
[sinks.deltalake_s3.storage_options]
AWS_STORAGE_ALLOW_HTTP = "true"

# Acknowledgments
[sinks.deltalake_s3.acknowledgements]
enabled = false
EOF

    echo "Configuration file created: $config_file" >&2
    echo "$config_file"
}

# Vector process management function
manage_vector_process() {
    local config_file=$1
    local duration=$2
    local vector_pid=""

    # Define cleanup function
    cleanup_vector() {
        if [ -n "$vector_pid" ] && kill -0 "$vector_pid" 2>/dev/null; then
            log_info "Stopping Vector process (PID: $vector_pid)..."

            # First try graceful shutdown (SIGTERM)
            kill -TERM "$vector_pid" 2>/dev/null
            local count=0
            while [ $count -lt 5 ] && kill -0 "$vector_pid" 2>/dev/null; do
                sleep 1
                count=$((count + 1))
            done

            # If still running, force stop (SIGKILL)
            if kill -0 "$vector_pid" 2>/dev/null; then
                log_warning "Graceful shutdown failed, force killing Vector process..."
                kill -KILL "$vector_pid" 2>/dev/null
                sleep 2
            fi

            log_success "Vector process stopped"
        fi
    }

    # Set signal trap
    trap cleanup_vector SIGINT SIGTERM

    # Start Vector (run in background)
    echo "Starting Vector..."
    ./target/${PROFILE}/vector --config "$config_file" &
    vector_pid=$!

    echo "Vector process started (PID: $vector_pid)"
    echo "Tip: Press Ctrl+C to stop the test anytime"

    # Wait for specified duration or user interruption
    local countdown=$duration
    while [ $countdown -gt 0 ] && kill -0 "$vector_pid" 2>/dev/null; do
        sleep 1
        countdown=$((countdown - 1))

        # Show progress every 10 seconds
        if [ $((duration - countdown)) -gt 0 ] && [ $(((duration - countdown) % 10)) -eq 0 ]; then
            log_info "Running... elapsed $((duration - countdown))s / ${duration}s"
        fi
    done

    # Check result
    local exit_code=0
    if kill -0 "$vector_pid" 2>/dev/null; then
        log_info "Test duration reached, stopping Vector..."
        cleanup_vector
    else
        wait "$vector_pid"
        exit_code=$?
        if [ $exit_code -ne 0 ]; then
            log_error "Vector exited abnormally with code: $exit_code"
        fi
    fi

    # Clean up signal trap
    trap - SIGINT SIGTERM

    return $exit_code
}

# Run Vector test
run_vector_test() {
    log_info "Running Vector S3 test (collection: $COLLECTION_METHOD, S3: $BUCKET, duration: ${DURATION}s)"

    # Create configuration file
    local config_file=$(create_vector_config)

    # Use improved process management to run Vector
    if manage_vector_process "$config_file" "$DURATION"; then
        log_success "Vector test completed (${DURATION}s)"
        
        # Show S3 verification results
        verify_s3_output
    else
        log_error "Vector test failed"
        return 1
    fi
}

# Verify S3 output results
verify_s3_output() {
    log_info "Verifying S3 bucket contents: s3://${BUCKET}/deltalake-tables"

    # Check if AWS CLI is available for verification
    if command -v aws >/dev/null 2>&1; then
        log_info "Using AWS CLI to check S3 bucket contents..."
        
        # Set AWS environment variables if provided
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
            log_info "Checking table: ${table_path}"
            
            # List objects in the table directory
            if aws s3 ls "${table_path}" --region "${REGION}" >/dev/null 2>&1; then
                object_count=$(aws s3 ls "${table_path}" --recursive --region "${REGION}" | wc -l)
                if [[ ${object_count} -gt 0 ]]; then
                    log_success "Found ${object_count} objects in ${table_path}"
                    
                    # Count parquet files specifically
                    parquet_count=$(aws s3 ls "${table_path}" --recursive --region "${REGION}" | grep -c "\.parquet$" || echo "0")
                    if [[ ${parquet_count} -gt 0 ]]; then
                        log_success "Found ${parquet_count} parquet files in ${table_path}"
                    else
                        log_warning "No parquet files found in ${table_path}"
                    fi
                else
                    log_warning "No objects found in ${table_path}"
                fi
            else
                log_warning "Unable to access ${table_path} or directory doesn't exist"
            fi
        done
    else
        log_warning "AWS CLI not found. Cannot verify S3 bucket contents."
        log_info "Please install AWS CLI to verify data was written to S3."
        log_info "Expected S3 locations:"
        log_info "  - s3://${BUCKET}/deltalake-tables/hist_processlist/"
        log_info "  - s3://${BUCKET}/deltalake-tables/hist_cluster_statements_summary/"
    fi
}

# Clean up test files
cleanup_test_files() {
    log_info "Cleaning up test files..."

    # Clean up configuration files
    rm -f test_config_s3_*.toml

    log_success "Test files cleanup completed"
}

# Show help
show_help() {
    cat <<EOF
System Tables Source S3 Testing Script

Usage: $0 [options]

Prerequisites:
    Please ensure local TiDB (port 4000) and PD (port 2379) are started
    AWS credentials must be configured for S3 access

Required Options:
    --bucket BUCKET_NAME        S3 bucket name for Delta Lake storage

Database Options:
    --host HOST                 TiDB host (default: 127.0.0.1)
    --port PORT                 TiDB port (default: 4000)
    --user USER                 Database username (default: root)
    --password PASSWORD         Database password (default: "")
    --database DATABASE         Database name (default: test)
    --pd PD_ADDRESS             PD address (default: 127.0.0.1:2379)

S3 Options:
    --region REGION             AWS region (default: us-east-1)
    --collection-method METHOD  Collection method: sql or coprocessor (default: coprocessor)

AWS Authentication Options:
    --aws-access-key-id KEY     AWS access key ID
    --aws-secret-access-key SECRET  AWS secret access key
    --aws-session-token TOKEN   AWS session token (for temporary credentials)
    --assume-role ROLE_ARN      AWS role ARN for assume role
    --external-id ID            External ID for assume role
    --role-session-name NAME    Role session name (default: vector-deltalake)

Other Options:
    --duration SECONDS          Test duration in seconds (default: 30)
    --release                   Use release build instead of debug
    -h, --help                 Show this help information

Examples:
    # Using static credentials
    $0 --bucket my-bucket --region us-east-1 \\
        --aws-access-key-id AKIAIOSFODNN7EXAMPLE \\
        --aws-secret-access-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

    # Using assume role
    $0 --bucket my-bucket --region us-east-1 \\
        --assume-role arn:aws:iam::123456789012:role/VectorRole

    # Using default AWS credential chain
    $0 --bucket my-bucket --region us-east-1

    # Test with coprocessor collection method
    $0 --bucket my-bucket --collection-method coprocessor --duration 60

    # Test with custom TiDB connection
    $0 --bucket my-bucket --host 192.168.1.100 --port 4000 --user admin --password secret

Notes:
    - This script assumes TiDB is listening on the specified host:port
    - This script assumes PD is listening on the specified PD address
    - AWS CLI is required for S3 verification (optional)
    - Vector process supports graceful Ctrl+C shutdown with automatic cleanup
    - Script will automatically clean up configuration files on exit

EOF
}

# Main function
main() {
    log_info "Starting System Tables S3 Test Script"
    
    # Parse arguments
    parse_arguments "$@"
    
    # Validate configuration
    validate_config
    
    # Set up cleanup trap
    trap cleanup_test_files EXIT
    
    # Check dependencies and build
    check_dependencies
    build_project
    
    # Run the test
    run_vector_test
    
    log_success "S3 test completed successfully!"
    log_info "Data should be written to S3 bucket: s3://${BUCKET}/deltalake-tables"
}

# Run main function
main "$@"