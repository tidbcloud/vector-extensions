#!/bin/bash
# 01_setup.sh - Initialize environment: Create MySQL database and tables, configure AWS credentials
#
# Usage:
#   1. Create MySQL database and tables
#   2. Configure AWS credentials (optional, via environment variables)
#
# Examples:
#   ./scripts/01_setup.sh
#   or
#   source scripts/01_setup.sh  # Export AWS environment variables to current shell

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== Environment Initialization ==="
echo ""

# 1. Create MySQL database and tables
echo "1. Creating MySQL database and tables..."

if docker ps | grep -q mysql; then
    CONTAINER=$(docker ps | grep mysql | awk '{print $1}' | head -1)
    echo "   Found MySQL container: $CONTAINER"
    
    docker exec -i $CONTAINER mysql -u root -proot < "$DEMO_DIR/config/create_mysql_table.sql" && {
        echo "   ✓ Database and tables created successfully"
    } || {
        echo "   ⚠️  Tables may already exist, continuing..."
    }
else
    echo "   ⚠️  MySQL Docker container not found"
    echo "   Please create database manually:"
    echo "     mysql -h localhost -u root -proot < $DEMO_DIR/config/create_mysql_table.sql"
fi

echo ""

# 2. AWS credentials configuration (optional)
echo "2. AWS Credentials Configuration"
echo "   Note: To configure AWS credentials, set the following environment variables:"
echo "     export AWS_ACCESS_KEY_ID=\"your-key\""
echo "     export AWS_SECRET_ACCESS_KEY=\"your-secret\""
echo "     export AWS_SESSION_TOKEN=\"your-token\"  # If using temporary credentials"
echo "     export AWS_REGION=\"us-west-2\""
echo ""

if [ -n "$AWS_ACCESS_KEY_ID" ]; then
    echo "   ✓ AWS credentials configured"
else
    echo "   ⚠️  AWS credentials not configured, please set environment variables"
fi

echo ""
echo "=== Initialization Complete ==="
