#!/usr/bin/env python3
"""
完整测试：从 S3 读取 slowlogs 并写入 MySQL
"""
import os
import sys
import time
import subprocess
import json
from pathlib import Path

# 设置环境变量
os.environ["AWS_ACCESS_KEY_ID"] = "ASIAYBEGSUMKNOBLWYE5"
os.environ["AWS_SECRET_ACCESS_KEY"] = "hemUNrcxvz3qD5d8nlvw8ldLdzJI/v9YX5R/rKRY"
os.environ["AWS_SESSION_TOKEN"] = "IQoJb3JpZ2luX2VjEF8aDmFwLW5vcnRoZWFzdC0xIkgwRgIhAOz0wL3K/As9Ka48eiYkSWOvKH7exXuPyg5ZDY0xGh2lAiEAhwKUDmDtFdP9si7BZ7LEdtin96MT3r1R5/s9cIPGmyEqiQMIKBABGgw1NTIxODU1MzczMDAiDCbOE7xD1M3oRqdjoirmAhdATcd981pRXI9WyUqVNr1qAPA4PjVXjutDB5RTRWKSZuE4stWQs0bogZ2zzlJY7iIXv0PnN1eC25WaEJ2vUjldPobsyKvjDqh/QjSxeGGJ+f0roVunx5Y0CGdaOKK8uqirxMrCzVfLByjIJXNXWuaRKTALADOHN6O2ymQa2yewFR47yb7DUJi8vgexMj81Mc6wnJ04JpeANKhGkZx2VIAchuXpiamkAG55YZQUde43stRy2cIO67HRIZAsqMzBuoj4YAI8jC00VlcGcBGLiD+hb30o/574gZQ+uHe4iUCikL2lTkk8gi/nJooa4WSzgXEifc6J6zfOl8PQBVXOP1mLKcCWhYo6C3XIAHabjPi6BlZ8VwV5mQUaQ2FOOucyNF4lVYhw2q+l/t+DsQTQd8eNC7o9CHeKlfmMcKG8trjSOTx+1cq4IoPPq5D1atx4CikA2t8jfeH5uAZ6k4Fqrf0eY89BvrwwlIiRzAY6owEJDT94Dd/dNLK4yZSwxzdNNBxk1HYEhKcoJ9Ae4o5UisoIVWRdzA++YPkKA6gr3kBGiCVoU1xJAN9ewRnzD52yLSOVPMq7vaCmlPtOu+hpD03ufbU8CWM4T+dnJAqXiJSw+9NcPfauHanUWtFi+QMwUDacEFLAkD2WtURytBFumGbancBaq8m0UcicDq4koh9r3GfwWPGNUkcaJsWJUriqqA30"
os.environ["AWS_REGION"] = "us-west-2"

sys.path.insert(0, os.path.dirname(__file__))

def find_vector():
    """查找 Vector 二进制"""
    import shutil
    vector = shutil.which("vector")
    if vector:
        return vector
    
    # 尝试项目目录
    project_root = Path(__file__).parent.parent
    for path in [
        project_root / "target" / "release" / "vector",
        project_root / "target" / "debug" / "vector",
    ]:
        if path.exists():
            return str(path)
    
    return None

def test_s3_access():
    """测试 S3 访问"""
    print("=== 测试 S3 访问 ===\n")
    try:
        import boto3
        s3 = boto3.client('s3', region_name='us-west-2')
        
        # 列出文件
        response = s3.list_objects_v2(
            Bucket='o11y-dev-shared-us-west-2',
            Prefix='deltalake/slowlogs/',
            MaxKeys=5
        )
        
        if 'Contents' in response:
            print(f"✓ 找到 {len(response['Contents'])} 个文件（前 5 个）:")
            for obj in response['Contents']:
                print(f"  - {obj['Key']} ({obj['Size']} bytes)")
            return True
        else:
            print("⚠️  未找到文件，但连接成功")
            return True
            
    except Exception as e:
        print(f"❌ S3 访问失败: {e}")
        return False

def generate_and_test_config():
    """生成并测试配置"""
    print("\n=== 生成 Vector 配置 ===\n")
    
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
        print(f"✓ 配置已保存到: {config_file}")
        print(f"\n配置摘要:")
        print(f"  - S3: o11y-dev-shared-us-west-2/deltalake/slowlogs/")
        print(f"  - 输出: /tmp/vector-output/test-001/")
        
        return str(config_file)
        
    except Exception as e:
        print(f"❌ 配置生成失败: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_vector_config(vector_binary, config_file):
    """测试 Vector 配置"""
    print("\n=== 测试 Vector 配置 ===\n")
    
    if not vector_binary:
        print("⚠️  Vector 二进制未找到，跳过配置测试")
        return False
    
    print(f"使用 Vector: {vector_binary}")
    
    try:
        # Dry-run 测试
        result = subprocess.run(
            [vector_binary, "--config", config_file, "--dry-run"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        
        if result.returncode == 0:
            print("✓ Vector dry-run 成功")
            if result.stdout:
                print("\n输出:")
                print(result.stdout[:500])  # 只显示前 500 字符
            return True
        else:
            print("❌ Vector dry-run 失败")
            print(f"返回码: {result.returncode}")
            if result.stderr:
                print("\n错误信息:")
                print(result.stderr[:1000])
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Vector dry-run 超时")
        return False
    except Exception as e:
        print(f"❌ Vector dry-run 异常: {e}")
        return False

def check_mysql():
    """检查 MySQL 连接和表"""
    print("\n=== 检查 MySQL ===\n")
    
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
        
        # 检查表是否存在
        cursor.execute("SHOW TABLES LIKE 'slowlogs'")
        if cursor.fetchone():
            print("✓ slowlogs 表存在")
            
            # 检查当前数据量
            cursor.execute("SELECT COUNT(*) FROM slowlogs")
            count = cursor.fetchone()[0]
            print(f"✓ 当前表中有 {count} 条记录")
        else:
            print("⚠️  slowlogs 表不存在，需要创建")
        
        cursor.close()
        conn.close()
        return True
        
    except ImportError:
        print("⚠️  pymysql 未安装，跳过 MySQL 检查")
        return None
    except Exception as e:
        print(f"❌ MySQL 连接失败: {e}")
        return False

if __name__ == "__main__":
    print("开始完整测试...\n")
    
    # 1. 测试 S3 访问
    if not test_s3_access():
        print("\n⚠️  S3 访问测试失败，但继续测试配置...")
    
    # 2. 生成配置
    config_file = generate_and_test_config()
    if not config_file:
        sys.exit(1)
    
    # 3. 查找 Vector
    vector_binary = find_vector()
    if vector_binary:
        print(f"\n✓ 找到 Vector: {vector_binary}")
    else:
        print("\n⚠️  Vector 二进制未找到")
        print("   请确保 Vector 在 PATH 中，或设置 VECTOR_BINARY 环境变量")
    
    # 4. 测试 Vector 配置
    if vector_binary:
        test_vector_config(vector_binary, config_file)
    
    # 5. 检查 MySQL
    mysql_ok = check_mysql()
    
    print("\n=== 测试总结 ===")
    print(f"✓ 配置生成: 成功")
    print(f"{'✓' if vector_binary else '⚠️ '} Vector 二进制: {vector_binary or '未找到'}")
    print(f"{'✓' if mysql_ok else '⚠️ '} MySQL: {'正常' if mysql_ok else '未检查或失败'}")
    
    print("\n下一步:")
    print("1. 如果 Vector 可用，可以运行:")
    print(f"   {vector_binary or 'vector'} --config {config_file}")
    print("2. 或者启动完整服务器:")
    print("   python3 app.py")
    print("3. 然后创建任务:")
    print("   curl -X POST http://localhost:8080/api/v1/tasks \\")
    print("     -H 'Content-Type: application/json' \\")
    print("     -d @test_request.json")
