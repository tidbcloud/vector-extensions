#!/usr/bin/env python3
"""
Debug script: Generate and validate Vector configuration
"""
import toml
import json

# Generate test configuration
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

# Output configuration
config_toml = toml.dumps(config)
print("=== Vector Configuration ===")
print(config_toml)

# Save to file
with open("/tmp/vector-debug-config.toml", "w") as f:
    f.write(config_toml)

print("\n✓ Configuration saved to /tmp/vector-debug-config.toml")
print("\nTest commands:")
print("  vector --config /tmp/vector-debug-config.toml --dry-run")
print("  vector --config /tmp/vector-debug-config.toml")
