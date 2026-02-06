#!/usr/bin/env python3
"""
Test Vector configuration generation and validation
"""
import os
import sys
import toml
import json
from pathlib import Path

# Import functions from app.py
sys.path.insert(0, os.path.dirname(__file__))
from app import generate_vector_config

def test_config_generation():
    """Test configuration generation"""
    print("=== Testing Vector Configuration Generation ===\n")
    
    task_id = "test-001"
    s3_bucket = "o11y-dev-shared-us-west-2"
    s3_prefix = "deltalake/slowlogs/"
    s3_region = "us-west-2"
    file_pattern = "*.log.gz"
    mysql_connection = "mysql://root:root@localhost:3306/testdb"
    mysql_table = "slowlogs"
    filter_keywords = []
    
    try:
        config_toml = generate_vector_config(
            task_id=task_id,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            s3_region=s3_region,
            file_pattern=file_pattern,
            mysql_connection=mysql_connection,
            mysql_table=mysql_table,
            filter_keywords=filter_keywords,
        )
        
        print("✓ Configuration generation successful\n")
        print("=== Vector Configuration ===")
        print(config_toml)
        
        # Save to file
        config_file = Path("/tmp/vector-test-config.toml")
        config_file.write_text(config_toml)
        print(f"\n✓ Configuration saved to: {config_file}")
        
        # Validate TOML format
        try:
            config_dict = toml.loads(config_toml)
            print("✓ TOML format validation passed")
            
            # Check key configurations
            print("\n=== Configuration Check ===")
            print(f"S3 Bucket: {config_dict['sources']['s3_slowlogs']['bucket']}")
            print(f"S3 Prefix: {config_dict['sources']['s3_slowlogs']['key_prefix']}")
            print(f"Transforms: {list(config_dict['transforms'].keys())}")
            print(f"Sinks: {list(config_dict['sinks'].keys())}")
            
            # Check split_lines transform
            if 'split_lines' in config_dict['transforms']:
                print(f"✓ split_lines transform exists")
                split_config = config_dict['transforms']['split_lines']
                print(f"  - Type: {split_config['type']}")
                print(f"  - Field: {split_config.get('field', 'N/A')}")
                print(f"  - Separator: {repr(split_config.get('separator', 'N/A'))}")
            else:
                print("⚠️  split_lines transform does not exist")
            
        except Exception as e:
            print(f"❌ TOML parsing failed: {e}")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration generation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_vector_dry_run():
    """Test Vector dry-run"""
    print("\n=== Testing Vector Dry-Run ===\n")
    
    config_file = "/tmp/vector-test-config.toml"
    if not Path(config_file).exists():
        print("❌ Configuration file does not exist, please run configuration generation test first")
        return False
    
    # Find vector binary
    import shutil
    vector_binary = shutil.which("vector")
    if not vector_binary:
        # Try to find vector in project directory
        project_root = Path(__file__).parent.parent
        for path in [project_root / "target" / "release" / "vector",
                     project_root / "target" / "debug" / "vector"]:
            if path.exists():
                vector_binary = str(path)
                break
    
    if not vector_binary:
        print("⚠️  Vector binary not found, skipping dry-run test")
        print("   Please ensure Vector is in PATH, or set VECTOR_BINARY environment variable")
        return None
    
    print(f"Using Vector: {vector_binary}")
    
    import subprocess
    try:
        result = subprocess.run(
            [vector_binary, "--config", config_file, "--dry-run"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        
        if result.returncode == 0:
            print("✓ Vector dry-run successful")
            if result.stdout:
                print("\nOutput:")
                print(result.stdout)
            return True
        else:
            print("❌ Vector dry-run failed")
            print(f"Return code: {result.returncode}")
            if result.stderr:
                print("\nError message:")
                print(result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Vector dry-run timeout")
        return False
    except Exception as e:
        print(f"❌ Vector dry-run exception: {e}")
        return False

if __name__ == "__main__":
    print("Starting test...\n")
    
    # Test configuration generation
    if not test_config_generation():
        sys.exit(1)
    
    # Test Vector dry-run
    result = test_vector_dry_run()
    if result is False:
        sys.exit(1)
    
    print("\n=== Test Complete ===")
    print("\nNext steps:")
    print("1. Ensure MySQL is running")
    print("2. Run: python3 app.py")
    print("3. Create a task in another terminal:")
    print("   curl -X POST http://localhost:8080/api/v1/tasks \\")
    print("     -H 'Content-Type: application/json' \\")
    print("     -d @test_request.json")
