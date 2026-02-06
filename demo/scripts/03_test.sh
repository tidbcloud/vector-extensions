#!/bin/bash
# 03_test.sh - End-to-end test script
#
# Usage:
#   1. Health check
#   2. Create backup task
#   3. Query task status
#   4. Check MySQL data
#
# Examples:
#   ./scripts/03_test.sh
#   or
#   cd demo && ./scripts/03_test.sh
#
# Prerequisites:
#   - Server is running (run 02_start.sh)
#   - MySQL is configured (run 01_setup.sh)

set -e

API_URL="http://localhost:8080"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== End-to-End Test ==="
echo ""

# 1. Health check
echo "1. Health Check"
curl -s "$API_URL/health" | jq . || echo "Server not running"
echo ""

# 2. Create task (with time range)
echo "2. Creating backup task (time range: 2025-06-06 to 2025-06-10)"
TASK_RESPONSE=$(curl -s -X POST "$API_URL/api/v1/tasks" \
  -H "Content-Type: application/json" \
  -d @"$DEMO_DIR/config/test_request.json")

echo "$TASK_RESPONSE" | jq . || echo "$TASK_RESPONSE"
echo ""

TASK_ID=$(echo "$TASK_RESPONSE" | jq -r '.task_id // empty')
if [ -z "$TASK_ID" ]; then
    echo "❌ Task creation failed"
    exit 1
fi

echo "✓ Task created successfully, Task ID: $TASK_ID"
echo ""

# 3. Wait for processing
echo "3. Waiting for processing (10 seconds)..."
sleep 10

# 4. Query task status
echo "4. Querying task status"
curl -s "$API_URL/api/v1/tasks/$TASK_ID" | jq . || echo "Query failed"
echo ""

# 5. Check MySQL data
echo "5. Checking MySQL data"
MYSQL_CONTAINER=$(docker ps | grep mysql | awk '{print $1}' | head -1)
if [ -n "$MYSQL_CONTAINER" ]; then
    docker exec $MYSQL_CONTAINER mysql -u root -proot testdb -e "SELECT COUNT(*) as total FROM slowlogs;" 2>/dev/null | grep -v "Warning" || echo "MySQL query failed"
    echo ""
    docker exec $MYSQL_CONTAINER mysql -u root -proot testdb -e "SELECT id, LEFT(log_line, 100) as preview FROM slowlogs LIMIT 5;" 2>/dev/null | grep -v "Warning" || echo "MySQL query failed"
else
    echo "⚠️  MySQL container not found"
fi

echo ""
echo "=== Test Complete ==="
echo ""
echo "Continue monitoring task:"
echo "  curl $API_URL/api/v1/tasks/$TASK_ID"
echo ""
echo "View all tasks:"
echo "  curl $API_URL/api/v1/tasks"
