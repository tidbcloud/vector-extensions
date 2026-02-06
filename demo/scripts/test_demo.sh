#!/bin/bash
# 测试 Demo API 的脚本

API_URL="http://localhost:8080"

echo "=== 测试 Backup Manager Demo API ==="
echo ""

# 1. 健康检查
echo "1. 健康检查"
curl -s "$API_URL/health" | jq . || echo "服务器未运行或 jq 未安装"
echo ""

# 2. 创建任务
echo "2. 创建备份任务"
TASK_RESPONSE=$(curl -s -X POST "$API_URL/api/v1/tasks" \
  -H "Content-Type: application/json" \
  -d @test_request.json)

echo "$TASK_RESPONSE" | jq . || echo "$TASK_RESPONSE"
echo ""

TASK_ID=$(echo "$TASK_RESPONSE" | jq -r '.task_id // empty')
if [ -z "$TASK_ID" ]; then
    echo "❌ 任务创建失败"
    exit 1
fi

echo "✓ 任务创建成功，Task ID: $TASK_ID"
echo ""

# 3. 等待几秒
echo "3. 等待 5 秒..."
sleep 5

# 4. 查询任务状态
echo "4. 查询任务状态"
curl -s "$API_URL/api/v1/tasks/$TASK_ID" | jq . || echo "查询失败"
echo ""

# 5. 列出所有任务
echo "5. 列出所有任务"
curl -s "$API_URL/api/v1/tasks" | jq . || echo "查询失败"
echo ""

# 6. 检查 MySQL 数据
echo "6. 检查 MySQL 数据"
mysql -h localhost -u root -proot testdb -e "SELECT COUNT(*) as total_rows FROM slowlogs;" 2>/dev/null || echo "MySQL 查询失败"
echo ""

echo "=== 测试完成 ==="
echo "继续监控任务状态:"
echo "  curl $API_URL/api/v1/tasks/$TASK_ID"
echo ""
echo "查看 MySQL 数据:"
echo "  mysql -h localhost -u root -proot testdb -e 'SELECT * FROM slowlogs LIMIT 10;'"
