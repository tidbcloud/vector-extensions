#!/bin/bash
# Script to test Demo API

API_URL="http://localhost:8080"

echo "=== Testing Backup Manager Demo API ==="
echo ""

# 1. Health check
echo "1. Health check"
curl -s "$API_URL/health" | jq . || echo "Server not running or jq not installed"
echo ""

# 2. Create task
echo "2. Creating backup task"
TASK_RESPONSE=$(curl -s -X POST "$API_URL/api/v1/tasks" \
  -H "Content-Type: application/json" \
  -d @test_request.json)

echo "$TASK_RESPONSE" | jq . || echo "$TASK_RESPONSE"
echo ""

TASK_ID=$(echo "$TASK_RESPONSE" | jq -r '.task_id // empty')
if [ -z "$TASK_ID" ]; then
    echo "❌ Task creation failed"
    exit 1
fi

echo "✓ Task created successfully, Task ID: $TASK_ID"
echo ""

# 3. Wait a few seconds
echo "3. Waiting 5 seconds..."
sleep 5

# 4. Query task status
echo "4. Querying task status"
curl -s "$API_URL/api/v1/tasks/$TASK_ID" | jq . || echo "Query failed"
echo ""

# 5. List all tasks
echo "5. Listing all tasks"
curl -s "$API_URL/api/v1/tasks" | jq . || echo "Query failed"
echo ""

# 6. Check MySQL data
echo "6. Checking MySQL data"
mysql -h localhost -u root -proot testdb -e "SELECT COUNT(*) as total_rows FROM slowlogs;" 2>/dev/null || echo "MySQL query failed"
echo ""

echo "=== Test Complete ==="
echo "Continue monitoring task status:"
echo "  curl $API_URL/api/v1/tasks/$TASK_ID"
echo ""
echo "View MySQL data:"
echo "  mysql -h localhost -u root -proot testdb -e 'SELECT * FROM slowlogs LIMIT 10;'"
