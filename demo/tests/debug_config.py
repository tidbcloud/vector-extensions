#!/usr/bin/env python3
"""
调试脚本：生成并验证 Vector 配置
"""
import toml
import json

# 生成测试配置
config = {
    "data_dir": "/tmp/vector-data/test",
    
    "api": {
        "enabled": True,
        "address": "127.0.0.1:8686",
        "graphql_enabled": False,
    },
    
    "sources": {
        "s3_slowlogs": {
            "type": "aws_s3",
            "region": "us-west-2",
            "bucket": "o11y-dev-shared-us-west-2",
            "key_prefix": "slowlogs/",
            "compression": "gzip",
            "poll_interval_ms": 1000,
        }
    },
    
    "transforms": {
        "decompress": {
            "type": "decompress",
            "inputs": ["s3_slowlogs"],
            "method": "gzip",
        },
        "split_lines": {
            "type": "split",
            "inputs": ["decompress"],
            "field": "message",
            "separator": "\n",
        }
    },
    
    "sinks": {
        "file_sink": {
            "type": "file",
            "inputs": ["split_lines"],
            "path": "/tmp/vector-output/test/slowlogs-%Y-%m-%d-%H%M%S.jsonl",
            "encoding": {
                "codec": "json"
            },
            "compression": "none",
        }
    }
}

# 输出配置
config_toml = toml.dumps(config)
print("=== Vector 配置 ===")
print(config_toml)

# 保存到文件
with open("/tmp/vector-debug-config.toml", "w") as f:
    f.write(config_toml)

print("\n✓ 配置已保存到 /tmp/vector-debug-config.toml")
print("\n测试命令:")
print("  vector --config /tmp/vector-debug-config.toml --dry-run")
print("  vector --config /tmp/vector-debug-config.toml")
