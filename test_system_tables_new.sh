#!/bin/bash

# System Tables Source Local Testing Script
# For testing system_tables source with various collection methods

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

# Check dependencies
check_dependencies() {
    log_info "Checking dependencies..."

    # Check Rust
    if ! command -v cargo &> /dev/null; then
        log_error "cargo not found, please install Rust"
        exit 1
    fi

    # Check mysql client (for connecting to local TiDB)
    if ! command -v mysql &> /dev/null; then
        log_warning "mysql client not found, some test data creation operations may not work"
    fi

    log_success "Dependencies check completed"
}

# Check if local TiDB is available
check_local_tidb() {
    log_info "Checking local TiDB connection..."

    # Try to connect to TiDB
    if command -v mysql &> /dev/null; then
        if mysql -h127.0.0.1 -P4000 -uroot -e "SELECT 1" >/dev/null 2>&1; then
            log_success "Local TiDB connection successful"
            return 0
        else
            log_error "Cannot connect to local TiDB (127.0.0.1:4000)"
            log_info "Please ensure TiDB is started and listening on port 4000"
            return 1
        fi
    else
        log_warning "No mysql client available, skipping TiDB connection check"
        return 0
    fi
}

# Check if local PD is available
check_local_pd() {
    log_info "Checking local PD connection..."

    # Try to connect to PD
    if command -v curl &> /dev/null; then
        if curl -s "http://127.0.0.1:2379/pd/api/v1/health" >/dev/null 2>&1; then
            log_success "Local PD connection successful"
            return 0
        else
            log_error "Cannot connect to local PD (127.0.0.1:2379)"
            log_info "Please ensure PD is started and listening on port 2379"
            return 1
        fi
    else
        log_warning "No curl command available, skipping PD connection check"
        return 0
    fi
}

# Build project
build_project() {
    log_info "Building project..."
    cargo build --release
    if [ $? -eq 0 ]; then
        log_success "Project build successful"
    else
        log_error "Project build failed"
        exit 1
    fi
}

# Check local test environment
check_local_env() {
    log_info "Checking local test environment..."

    local tidb_ok=0
    local pd_ok=0

    # Check TiDB
    if check_local_tidb; then
        tidb_ok=1
    fi

    # Check PD
    if check_local_pd; then
        pd_ok=1
    fi

    if [ $tidb_ok -eq 1 ] && [ $pd_ok -eq 1 ]; then
        log_success "Local test environment check passed"
        return 0
    else
        log_error "Local test environment check failed"
        log_info "Please ensure local TiDB (port 4000) and PD (port 2379) are properly started"
        return 1
    fi
}

# Create test data
create_test_data() {
    log_info "Creating test data..."

    if ! command -v mysql &> /dev/null; then
        log_warning "mysql client not available, skipping test data creation"
        return 0
    fi

    # Check TiDB connection
    if ! mysql -h127.0.0.1 -P4000 -uroot -e "SELECT 1" >/dev/null 2>&1; then
        log_error "Cannot connect to local TiDB, skipping test data creation"
        return 1
    fi

    # Create test database and tables
    mysql -h127.0.0.1 -P4000 -uroot <<EOF
CREATE DATABASE IF NOT EXISTS test_db;
USE test_db;

-- Create a test table
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert test data
INSERT INTO users (id, name, email) VALUES
(1, 'Alice', 'alice@example.com'),
(2, 'Bob', 'bob@example.com'),
(3, 'Charlie', 'charlie@example.com');

-- Generate some query activity
SELECT COUNT(*) FROM information_schema.processlist;
SELECT * FROM users LIMIT 1;
EOF

    log_success "Test data creation completed"
}

# Generate Vector configuration file
create_vector_config() {
    local collection_method=$1
    local sink_type=${2:-"console"}  # Default is console, optional deltalake
    local config_file="test_config_${collection_method}_${sink_type}.toml"

    log_info "Creating Vector configuration file: $config_file (sink: $sink_type)"

    cat > "$config_file" <<EOF
# Vector Configuration File - System Tables Source Test
# Collection method: $collection_method, Output: $sink_type

[sources.tidb_system_tables]
type = "system_tables"

# Database connection configuration
database_username = "root"
database_password = ""
database_host = "127.0.0.1"
database_port = 4000
database_name = "test_db"
database_max_connections = 5
database_connect_timeout = 30

# PD configuration (for topology discovery)
pd_address = "127.0.0.1:2379"

# Collection configuration
short_interval = 5
long_interval = 30
retention_days = 1
topology_fetch_interval_seconds = 10.0

# Collection method configuration
collection_method = "$collection_method"

# Tables to collect configuration - only CLUSTER_STATEMENTS_SUMMARY for testing
[[sources.tidb_system_tables.tables]]
source_schema = "information_schema"
source_table = "CLUSTER_STATEMENTS_SUMMARY"
dest_table = "statements_summary"
collection_interval = "short"
enabled = true

EOF

    if [ "$sink_type" = "console" ]; then
        cat >> "$config_file" <<EOF
# Output to console (for testing)
[sinks.console]
type = "console"
inputs = ["tidb_system_tables"]

[sinks.console.encoding]
codec = "json"
EOF
    elif [ "$sink_type" = "deltalake" ]; then
        # Create local data directory
        local data_dir="./test_data/deltalake"
        mkdir -p "$data_dir"

        cat >> "$config_file" <<EOF
# Output to Delta Lake (local storage, using default partitioning)
[sinks.deltalake]
type = "deltalake"
inputs = ["tidb_system_tables"]
base_path = "$data_dir"
batch_size = 100
timeout_secs = 30
compression = "snappy"

# Local storage options
[sinks.deltalake.storage_options]
# Use local file system
"file.enable_move" = "true"
EOF
    fi

    log_success "Configuration file created: $config_file"
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
    log_info "Starting Vector..."
    ./target/release/vector --config "$config_file" &
    vector_pid=$!

    log_info "Vector process started (PID: $vector_pid)"
    log_info "Tip: Press Ctrl+C to stop the test anytime"

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
    local collection_method=$1
    local sink_type=${2:-"console"}
    local duration=${3:-30}
    local config_file="test_config_${collection_method}_${sink_type}.toml"

    log_info "Running Vector test (collection: $collection_method, output: $sink_type, duration: ${duration}s)"

    # Create configuration file
    create_vector_config "$collection_method" "$sink_type"

    # Use improved process management to run Vector
    if manage_vector_process "$config_file" "$duration"; then
        log_success "Vector test completed (${duration}s)"

        # If deltalake, show generated files
        if [ "$sink_type" = "deltalake" ]; then
            show_deltalake_output
        fi
    else
        log_error "Vector test failed"
        return 1
    fi
}

# Show Delta Lake output results
show_deltalake_output() {
    local data_dir="./test_data/deltalake"

    if [ -d "$data_dir" ]; then
        log_info "Delta Lake output results:"

        # Show partition directory structure
        log_info "Partition directory structure:"
        if command -v tree &> /dev/null; then
            tree "$data_dir" -d | head -20
        else
            find "$data_dir" -type d | head -15 | while read dir; do
                log_info "  Directory: $dir"
            done
        fi

        # Show data files
        log_info "Data files (Parquet):"
        find "$data_dir" -name "*.parquet" | head -10 | while read file; do
            local size=$(ls -lh "$file" | awk '{print $5}')
            log_info "  File: $file (size: $size)"
        done

        # Show Delta Log files
        log_info "Delta Log files:"
        find "$data_dir" -path "*/_delta_log/*" -name "*.json" | head -5 | while read file; do
            log_info "  Log: $file"
        done

        # Show statistics
        local parquet_count=$(find "$data_dir" -name "*.parquet" | wc -l)
        local partition_count=$(find "$data_dir" -type d -name "_vector_table=*" | wc -l)
        log_info "Statistics: $parquet_count data files, $partition_count table partitions"
    else
        log_warning "Delta Lake data directory does not exist: $data_dir"
    fi
}

# Clean up all Vector processes
cleanup_vector_processes() {
    local vector_pids=$(pgrep -f "vector.*--config.*test_config_" 2>/dev/null || true)
    if [ -n "$vector_pids" ]; then
        log_warning "Found lingering Vector processes, cleaning up..."
        echo "$vector_pids" | while read pid; do
            if [ -n "$pid" ]; then
                log_info "Stopping Vector process (PID: $pid)..."
                kill -TERM "$pid" 2>/dev/null || true
                sleep 2
                if kill -0 "$pid" 2>/dev/null; then
                    kill -KILL "$pid" 2>/dev/null || true
                fi
            fi
        done
        sleep 1
    fi
}

# Clean up test files
cleanup_test_files() {
    log_info "Cleaning up test files..."

    # First clean up any existing Vector processes
    cleanup_vector_processes

    # Clean up configuration files
    rm -f test_config_*.toml

    # Clean up test data directory
    if [ -d "./test_data" ]; then
        log_info "Cleaning up test data directory..."
        rm -rf "./test_data"
    fi

    log_success "Test files cleanup completed"
}

# Show help
show_help() {
    cat <<EOF
System Tables Source Testing Script (Local Environment Version)

Usage: $0 [options] [command]

Prerequisites:
    Please ensure local TiDB (port 4000) and PD (port 2379) are started

Commands:
    build           Build project
    check-env       Check local test environment (TiDB + PD)
    create-data     Create test data
    test-sql        Test SQL collection method (console output)
    test-copr       Test Coprocessor collection method (console output)
    test-sql-delta  Test SQL collection method (Delta Lake output)
    test-copr-delta Test Coprocessor collection method (Delta Lake output)
    test-all        Test all collection methods (console output)
    test-all-delta  Test all collection methods (Delta Lake output)
    cleanup         Clean up test files
    full-test       Full test process (check env -> create data -> test -> cleanup)

Options:
    -d, --duration SECONDS    Test duration (default: 30s)
    --no-cleanup              Do not clean up test files on exit
    -h, --help               Show help information

Examples:
    $0 build                 # Build project
    $0 check-env             # Check local environment
    $0 full-test             # Run full test (console output)
    $0 test-sql -d 60        # Test SQL method for 60 seconds (console output)
    $0 test-sql-delta -d 60  # Test SQL method for 60 seconds (Delta Lake output)
    $0 test-copr-delta --no-cleanup -d 60  # Test coprocessor method without cleanup
    $0 test-all-delta -d 120 # Test all methods for 120 seconds (Delta Lake output)
    $0 cleanup               # Clean up test files

Notes:
    - This script assumes TiDB is listening on 127.0.0.1:4000
    - This script assumes PD is listening on 127.0.0.1:2379
    - mysql client is required for test data creation
    - curl is required for PD health check
    - Vector process supports graceful Ctrl+C shutdown with automatic cleanup
    - Script will automatically clean up all lingering Vector processes on exit

EOF
}

# Main function
main() {
    local duration=30
    local command=""

    # Parse arguments
    local no_cleanup=false
    while [[ $# -gt 0 ]]; do
        case $1 in
            -d|--duration)
                duration="$2"
                shift 2
                ;;
            --no-cleanup)
                no_cleanup=true
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            build|check-env|create-data|test-sql|test-copr|test-sql-delta|test-copr-delta|test-all|test-all-delta|cleanup|full-test)
                command="$1"
                shift
                ;;
            *)
                log_error "Unknown parameter: $1"
                show_help
                exit 1
                ;;
        esac
    done

    if [ -z "$command" ]; then
        log_error "Please specify a command"
        show_help
        exit 1
    fi

    # Set up cleanup trap unless --no-cleanup is specified
    if [ "$no_cleanup" = false ]; then
        trap cleanup_test_files EXIT
        log_info "Cleanup enabled (will cleanup on exit)"
    else
        log_info "Cleanup disabled (--no-cleanup specified)"
    fi

    case $command in
        build)
            check_dependencies
            build_project
            ;;
        check-env)
            check_dependencies
            check_local_env
            ;;
        create-data)
            create_test_data
            ;;
        test-sql)
            if ! check_local_env; then
                exit 1
            fi
            run_vector_test "sql" "console" "$duration"
            ;;
        test-copr)
            if ! check_local_env; then
                exit 1
            fi
            run_vector_test "coprocessor" "console" "$duration"
            ;;
        test-sql-delta)
            if ! check_local_env; then
                exit 1
            fi
            run_vector_test "sql" "deltalake" "$duration"
            ;;
        test-copr-delta)
            if ! check_local_env; then
                exit 1
            fi
            run_vector_test "coprocessor" "deltalake" "$duration"
            ;;
        test-all)
            if ! check_local_env; then
                exit 1
            fi
            log_info "Testing all collection methods (console output)..."
            run_vector_test "sql" "console" "$duration"
            sleep 5
            run_vector_test "coprocessor" "console" "$duration"
            ;;
        test-all-delta)
            if ! check_local_env; then
                exit 1
            fi
            log_info "Testing all collection methods (Delta Lake output)..."
            run_vector_test "sql" "deltalake" "$duration"
            sleep 5
            run_vector_test "coprocessor" "deltalake" "$duration"
            ;;
        cleanup)
            cleanup_test_files
            ;;
        full-test)
            log_info "Starting full test process..."
            check_dependencies
            build_project

            if ! check_local_env; then
                log_error "Local environment check failed, please start TiDB and PD first"
                exit 1
            fi

            cleanup_test_files  # Clean up any old files
            create_test_data

            log_info "=== Console Output Tests ==="
            log_info "Testing SQL collection method (console)..."
            run_vector_test "sql" "console" "$duration"
            sleep 5

            log_info "Testing Coprocessor collection method (console)..."
            run_vector_test "coprocessor" "console" "$duration"
            sleep 5

            log_info "=== Delta Lake Output Tests ==="
            log_info "Testing SQL collection method (Delta Lake)..."
            run_vector_test "sql" "deltalake" "$duration"
            sleep 5

            log_info "Testing Coprocessor collection method (Delta Lake)..."
            run_vector_test "coprocessor" "deltalake" "$duration"

            cleanup_test_files
            log_success "Full test process completed"
            ;;
        *)
            log_error "Unknown command: $command"
            show_help
            exit 1
            ;;
    esac
}

# Signal handling - conditionally cleanup (will be set in main function)
# trap cleanup_test_files EXIT

# Run main function
main "$@"
