#!/bin/bash
# 04_test_api.sh - Example script for testing API

API_URL="http://localhost:8080"

echo "=== 1. Health Check ==="
curl -s "$API_URL/health" | jq .

echo -e "\n=== 2. Create Task ==="
TASK_RESPONSE=$(curl -s -X POST "$API_URL/api/v1/tasks" \
  -H "Content-Type: application/json" \
  -d '{
    "s3_bucket": "my-logs-bucket",
    "s3_prefix": "slowlogs/2024/01/01/",
    "s3_region": "us-west-2",
    "file_pattern": "*.log.gz",
    "mysql_connection": "mysql://user:password@localhost:3306/mydb",
    "mysql_table": "slowlogs",
    "filter_keywords": ["ERROR", "WARN"]
  }')

echo "$TASK_RESPONSE" | jq .

TASK_ID=$(echo "$TASK_RESPONSE" | jq -r '.task_id')
echo -e "\nTask ID: $TASK_ID"

echo -e "\n=== 3. Get Task Status ==="
sleep 2
curl -s "$API_URL/api/v1/tasks/$TASK_ID" | jq .

echo -e "\n=== 4. List All Tasks ==="
curl -s "$API_URL/api/v1/tasks" | jq .

echo -e "\n=== 5. Wait and check status again ==="
sleep 5
curl -s "$API_URL/api/v1/tasks/$TASK_ID" | jq .

# Uncomment to delete task
# echo -e "\n=== 6. Delete Task ==="
# curl -s -X DELETE "$API_URL/api/v1/tasks/$TASK_ID" | jq .
