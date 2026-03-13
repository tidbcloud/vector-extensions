#!/usr/bin/env bash
# Test POST /api/v1/sync-logs-to-mysql
# Before use: 1) Start demo: cd demo && python3 app.py
#             2) Ensure MySQL table exists: mysql -u root -p testdb < config/create_parsed_logs_table.sql
#             3) Export AWS creds if reading from S3
#
# Custom line_parse_regexes for Loki/Go logfmt: level=info ts=... caller=... [key=value] msg="..."
# Use .*? between caller and msg for optional fields; capture names match table columns

curl -s -X POST http://127.0.0.1:8080/api/v1/sync-logs-to-mysql \
  -H "Content-Type: application/json" \
  -d '{
    "source_bucket": "o11y-prod-shared-us-west-2-staging",
    "cluster_id": "o11y",
    "types": ["raw_logs"],
    "time_range": { "start": "2026-02-04T11:00:00Z", "end": "2026-02-04T11:15:00Z" },
    "raw_log_components": ["loki"],
    "parse_lines": true,
    "line_parse_regexes": [
      "level=(?P<level>\\S+)\\s+ts=(?P<log_timestamp>[^\\s]+)\\s+caller=(?P<logger>[^\\s]+).*?msg=\"(?P<message_body>[^\"]*)\""
    ],
    "mysql_connection": "mysql://root:root@localhost:3306/testdb",
    "mysql_table": "parsed_logs",
    "max_keys": 500,
    "region": "us-west-2"
  }'
