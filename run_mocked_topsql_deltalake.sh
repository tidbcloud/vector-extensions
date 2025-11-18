#!/bin/bash

# Script to run mocked_topsql source with deltalake sink
# This script builds the project (if needed) and runs Vector with the configuration

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
CONFIG_FILE="config_mocked_topsql_deltalake.toml"
BUILD_PROFILE="${BUILD_PROFILE:-release}"
VECTOR_BINARY="./target/${BUILD_PROFILE}/vector"
DELTA_TABLES_DIR="./local-delta-tables"

# Functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    log_error "Configuration file not found: $CONFIG_FILE"
    exit 1
fi

# Create local delta tables directory if it doesn't exist
if [ ! -d "$DELTA_TABLES_DIR" ]; then
    log_info "Creating local delta tables directory: $DELTA_TABLES_DIR"
    mkdir -p "$DELTA_TABLES_DIR"
fi

# Check if vector binary exists, if not, build it
if [ ! -f "$VECTOR_BINARY" ]; then
    log_warn "Vector binary not found at $VECTOR_BINARY"
    log_info "Building project (profile: $BUILD_PROFILE)..."
    
    if [ "$BUILD_PROFILE" = "release" ]; then
        cargo build --release
    else
        cargo build
    fi
    
    if [ $? -ne 0 ]; then
        log_error "Build failed"
        exit 1
    fi
    
    log_info "Build completed successfully"
else
    log_info "Using existing Vector binary: $VECTOR_BINARY"
fi

# Validate configuration
log_info "Validating configuration file: $CONFIG_FILE"
if ! "$VECTOR_BINARY" validate --config "$CONFIG_FILE" 2>/dev/null; then
    log_warn "Configuration validation failed, but continuing anyway..."
fi

# Cleanup function
cleanup() {
    if [ -n "$VECTOR_PID" ] && kill -0 "$VECTOR_PID" 2>/dev/null; then
        log_info "Stopping Vector process (PID: $VECTOR_PID)..."
        
        # Try graceful shutdown first
        kill -TERM "$VECTOR_PID" 2>/dev/null
        local count=0
        while [ $count -lt 5 ] && kill -0 "$VECTOR_PID" 2>/dev/null; do
            sleep 1
            count=$((count + 1))
        done
        
        # Force kill if still running
        if kill -0 "$VECTOR_PID" 2>/dev/null; then
            log_warn "Graceful shutdown failed, force killing..."
            kill -KILL "$VECTOR_PID" 2>/dev/null
            sleep 1
        fi
        
        log_info "Vector process stopped"
    fi
}

# Set trap for cleanup
trap cleanup SIGINT SIGTERM EXIT

# Run Vector
log_info "Starting Vector with configuration: $CONFIG_FILE"
log_info "Delta Lake tables will be written to: $DELTA_TABLES_DIR"
log_info "Press Ctrl+C to stop"

"$VECTOR_BINARY" --config "$CONFIG_FILE" &
VECTOR_PID=$!

log_info "Vector process started (PID: $VECTOR_PID)"
log_info "Delta tables directory: $(pwd)/$DELTA_TABLES_DIR"

# Wait for the process
wait $VECTOR_PID
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    log_info "Vector exited successfully"
else
    log_error "Vector exited with code: $EXIT_CODE"
fi

# Show delta tables info if directory has content
if [ -d "$DELTA_TABLES_DIR" ] && [ "$(ls -A $DELTA_TABLES_DIR 2>/dev/null)" ]; then
    log_info "Delta Lake tables created in: $DELTA_TABLES_DIR"
    log_info "Contents:"
    ls -lh "$DELTA_TABLES_DIR" | head -20
fi

exit $EXIT_CODE

