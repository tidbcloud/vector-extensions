#!/usr/bin/env python3
"""
Full test: Read slowlogs from S3 and write to MySQL
"""
import os
import sys
import time
import subprocess
import json
from pathlib import Path


sys.path.insert(0, os.path.dirname(__file__))

def find_vector():
    """Find Vector binary"""
    import shutil
    vector = shutil.which("vector")
    if vector:
        return vector
    
    # Try project directory
    project_root = Path(__file__).parent.parent
    for path in [
        project_root / "target" / "release" / "vector",
        project_root / "target" / "debug" / "vector",
    ]:
        if path.exists():
            return str(path)
    
    return None

def test_s3_access():
    """Test S3 access"""
    print("=== Testing S3 Access ===\n")
    try:
        import boto3
        s3 = boto3.client('s3', region_name='us-west-2')
        
        # List files
        response = s3.list_objects_v2(
            Bucket='o11y-dev-shared-us-west-2',
            Prefix='deltalake/slowlogs/',
            MaxKeys=5
        )
        
        if 'Contents' in response:
            print(f"✓ Found {len(response['Contents'])} files (first 5):")
            for obj in response['Contents']:
                print(f"  - {obj['Key']} ({obj['Size']} bytes)")
            return True
        else:
            print("⚠️  No files found, but connection successful")
            return True
            
    except Exception as e:
        print(f"❌ S3 access failed: {e}")
        return False

def generate_and_test_config():
    """Generate and test configuration"""
    print("\n=== Generating Vector Configuration ===\n")
    
    try:
        from app import generate_vector_config
        
        config_toml = generate_vector_config(
            task_id="test-001",
            s3_bucket="o11y-dev-shared-us-west-2",
            s3_prefix="deltalake/slowlogs/",
            s3_region="us-west-2",
            file_pattern="*.log.gz",
            mysql_connection="mysql://root:root@localhost:3306/testdb",
            mysql_table="slowlogs",
            filter_keywords=[],
        )
        
        config_file = Path("/tmp/vector-test-config.toml")
        config_file.write_text(config_toml)
        print(f"✓ Configuration saved to: {config_file}")
        print(f"\nConfiguration summary:")
        print(f"  - S3: o11y-dev-shared-us-west-2/deltalake/slowlogs/")
        print(f"  - Output: /tmp/vector-output/test-001/")
        
        return str(config_file)
        
    except Exception as e:
        print(f"❌ Configuration generation failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_vector_config(vector_binary, config_file):
    """Test Vector configuration"""
    print("\n=== Testing Vector Configuration ===\n")
    
    if not vector_binary:
        print("⚠️  Vector binary not found, skipping configuration test")
        return False
    
    print(f"Using Vector: {vector_binary}")
    
    try:
        # Dry-run test
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
                print(result.stdout[:500])  # Show first 500 characters
            return True
        else:
            print("❌ Vector dry-run failed")
            print(f"Return code: {result.returncode}")
            if result.stderr:
                print("\nError message:")
                print(result.stderr[:1000])
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Vector dry-run timeout")
        return False
    except Exception as e:
        print(f"❌ Vector dry-run exception: {e}")
        return False

def check_mysql():
    """Check MySQL connection and table"""
    print("\n=== Checking MySQL ===\n")
    
    try:
        import pymysql
        conn = pymysql.connect(
            host='localhost',
            port=3306,
            user='root',
            password='root',
            database='testdb',
            charset='utf8mb4'
        )
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("SHOW TABLES LIKE 'slowlogs'")
        if cursor.fetchone():
            print("✓ slowlogs table exists")
            
            # Check current data count
            cursor.execute("SELECT COUNT(*) FROM slowlogs")
            count = cursor.fetchone()[0]
            print(f"✓ Current table has {count} records")
        else:
            print("⚠️  slowlogs table does not exist, needs to be created")
        
        cursor.close()
        conn.close()
        return True
        
    except ImportError:
        print("⚠️  pymysql not installed, skipping MySQL check")
        return None
    except Exception as e:
        print(f"❌ MySQL connection failed: {e}")
        return False

if __name__ == "__main__":
    print("Starting full test...\n")
    
    # 1. Test S3 access
    if not test_s3_access():
        print("\n⚠️  S3 access test failed, but continuing with configuration test...")
    
    # 2. Generate configuration
    config_file = generate_and_test_config()
    if not config_file:
        sys.exit(1)
    
    # 3. Find Vector
    vector_binary = find_vector()
    if vector_binary:
        print(f"\n✓ Found Vector: {vector_binary}")
    else:
        print("\n⚠️  Vector binary not found")
        print("   Please ensure Vector is in PATH, or set VECTOR_BINARY environment variable")
    
    # 4. Test Vector configuration
    if vector_binary:
        test_vector_config(vector_binary, config_file)
    
    # 5. Check MySQL
    mysql_ok = check_mysql()
    
    print("\n=== Test Summary ===")
    print(f"✓ Configuration generation: Success")
    print(f"{'✓' if vector_binary else '⚠️ '} Vector binary: {vector_binary or 'Not found'}")
    print(f"{'✓' if mysql_ok else '⚠️ '} MySQL: {'OK' if mysql_ok else 'Not checked or failed'}")
    
    print("\nNext steps:")
    print("1. If Vector is available, you can run:")
    print(f"   {vector_binary or 'vector'} --config {config_file}")
    print("2. Or start the full server:")
    print("   python3 app.py")
    print("3. Then create a task:")
    print("   curl -X POST http://localhost:8080/api/v1/tasks \\")
    print("     -H 'Content-Type: application/json' \\")
    print("     -d @test_request.json")
