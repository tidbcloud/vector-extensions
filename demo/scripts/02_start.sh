#!/bin/bash
# 02_start.sh - Start Backup Manager Demo API Server
#
# Usage:
#   1. Check and install Python dependencies
#   2. Check MySQL connection
#   3. Auto-detect Vector binary
#   4. Start Flask API server
#
# Examples:
#   ./scripts/02_start.sh
#   or
#   cd demo && ./scripts/02_start.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== Backup Manager Demo Startup Script ==="
echo ""

if [ -z "$AWS_ACCESS_KEY_ID" ]; then
    echo "⚠️  AWS credentials not set, please set environment variables"
    return 1
fi

# Find Vector binary
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Try to find vector binary
VECTOR_BINARY=""
if command -v vector &> /dev/null; then
    VECTOR_BINARY="vector"
    echo "✓ Found Vector: $(which vector)"
elif [ -f "$PROJECT_ROOT/target/release/vector" ]; then
    VECTOR_BINARY="$PROJECT_ROOT/target/release/vector"
    echo "✓ Found Vector: $VECTOR_BINARY"
elif [ -f "$PROJECT_ROOT/target/debug/vector" ]; then
    VECTOR_BINARY="$PROJECT_ROOT/target/debug/vector"
    echo "✓ Found Vector: $VECTOR_BINARY"
else
    echo "⚠️  Warning: Vector binary not found"
    echo "   Please ensure Vector is in PATH, or set VECTOR_BINARY environment variable"
    VECTOR_BINARY="${VECTOR_BINARY:-vector}"
fi

export VECTOR_BINARY

# Set other environment variables
export CONFIG_DIR="/tmp/vector-tasks"

# Check Python dependencies
echo ""
echo "Checking Python dependencies..."
if ! python3 -c "import flask" 2>/dev/null; then
    echo "⚠️  Flask not installed, installing dependencies..."
    pip3 install -r "$DEMO_DIR/requirements.txt" || {
        echo "❌ Dependency installation failed, please run manually: pip3 install -r requirements.txt"
        exit 1
    }
fi

# Check MySQL connection (optional)
echo ""
echo "Checking MySQL connection..."
if command -v mysql &> /dev/null; then
    if mysql -h localhost -u root -proot -e "SELECT 1" 2>/dev/null; then
        echo "✓ MySQL connection successful"
        
        # Check if table exists, create if not
        if ! mysql -h localhost -u root -proot -e "USE testdb; SELECT 1 FROM slowlogs LIMIT 1" 2>/dev/null; then
            echo "Creating MySQL tables..."
            mysql -h localhost -u root -proot < "$DEMO_DIR/config/create_mysql_table.sql" 2>/dev/null || {
                echo "⚠️  Table creation failed or already exists, continuing..."
            }
        fi
    else
        echo "⚠️  MySQL connection failed, please ensure MySQL is running"
    fi
else
    echo "⚠️  mysql command not found, skipping MySQL check"
fi

# Display configuration information
echo ""
echo "=== Configuration Information ==="
echo "AWS Region: $AWS_REGION"
echo "S3 Bucket: o11y-dev-shared-us-west-2"
echo "Vector Binary: $VECTOR_BINARY"
echo "Config Directory: $CONFIG_DIR"
echo "MySQL: localhost:3306 (user: root)"
echo ""

# Switch to demo directory
cd "$DEMO_DIR"

# Start server
echo "=== Starting Server ==="
echo "Server will start at http://0.0.0.0:8080"
echo "Press Ctrl+C to stop the server"
echo ""

python3 app.py