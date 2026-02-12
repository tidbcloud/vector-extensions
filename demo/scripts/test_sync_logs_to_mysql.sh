#!/usr/bin/env bash
# 测试 POST /api/v1/sync-logs-to-mysql
# 使用前：1) 启动 demo: cd demo && python3 app.py
#        2) 确保 MySQL 已建表: mysql -u root -p testdb < config/create_parsed_logs_table.sql
#        3) 如需读 S3，请 export AWS 凭证
#
# 使用自定义解析 line_parse_regexes 匹配 Loki/Go logfmt 格式：
#   level=info ts=2026-02-04T10:57:20.549Z caller=foo.go:123 msg="..."
# 命名捕获与表列一致：level, log_timestamp, logger, message_body

curl -s -m 120 -X POST http://127.0.0.1:8080/api/v1/sync-logs-to-mysql \
  -H "Content-Type: application/json" \
  -d '{
    "source_bucket": "o11y-prod-shared-us-west-2-staging",
    "cluster_id": "o11y",
    "types": ["raw_logs"],
    "time_range": { "start": "2026-02-04T11:00:00Z", "end": "2026-02-04T11:15:00Z" },
    "raw_log_components": ["loki"],
    "parse_lines": true,
    "line_parse_regexes": [
      "level=(?P<level>\\S+)\\s+ts=(?P<log_timestamp>[^\\s]+)\\s+caller=(?P<logger>[^\\s]+)\\s+msg=\"(?P<message_body>[^\"]*)\""
    ],
    "mysql_connection": "mysql://root:root@localhost:3306/testdb",
    "mysql_table": "parsed_logs",
    "max_keys": 500,
    "region": "us-west-2",
    "timeout_secs": 120
  }'
