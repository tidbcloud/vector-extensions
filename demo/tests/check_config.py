#!/usr/bin/env python3
"""
Quick check of configuration generation logic
"""
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(__file__))

# Mock toml module (if not available)
try:
    import toml
except ImportError:
    print("Warning: toml module not installed, will use simple output")
    class toml:
        @staticmethod
        def dumps(d):
            import json
            return json.dumps(d, indent=2)

# Import configuration generation function
try:
    from app import generate_vector_config
    
    print("=== Testing Configuration Generation ===\n")
    
    config = generate_vector_config(
        task_id="test-001",
        s3_bucket="o11y-dev-shared-us-west-2",
        s3_prefix="deltalake/slowlogs/",
        s3_region="us-west-2",
        file_pattern="*.log.gz",
        mysql_connection="mysql://root:root@localhost:3306/testdb",
        mysql_table="slowlogs",
        filter_keywords=[],
    )
    
    print("✓ Configuration generation successful\n")
    print("=== Generated Configuration ===")
    print(config)
    
    # Check key parts
    print("\n=== Configuration Check ===")
    if "deltalake/slowlogs/" in config:
        print("✓ S3 prefix correct: deltalake/slowlogs/")
    else:
        print("❌ S3 prefix may have issues")
    
    if "split_lines" in config:
        print("✓ split_lines transform exists")
    else:
        print("❌ split_lines transform missing")
    
    if "decompress" in config:
        print("✓ decompress transform exists")
    else:
        print("❌ decompress transform missing")
    
    print("\nConfiguration generated, can be saved to file for Vector testing")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
