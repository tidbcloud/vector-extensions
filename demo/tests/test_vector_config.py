#!/usr/bin/env python3
"""
测试 Vector 配置生成和验证
"""
import os
import sys
import toml
import json
from pathlib import Path

# 设置 AWS 凭证
os.environ["AWS_ACCESS_KEY_ID"] = "ASIAYBEGSUMKNOBLWYE5"
os.environ["AWS_SECRET_ACCESS_KEY"] = "hemUNrcxvz3qD5d8nlvw8ldLdzJI/v9YX5R/rKRY"
os.environ["AWS_SESSION_TOKEN"] = "IQoJb3JpZ2luX2VjEF8aDmFwLW5vcnRoZWFzdC0xIkgwRgIhAOz0wL3K/As9Ka48eiYkSWOvKH7exXuPyg5ZDY0xGh2lAiEAhwKUDmDtFdP9si7BZ7LEdtin96MT3r1R5/s9cIPGmyEqiQMIKBABGgw1NTIxODU1MzczMDAiDCbOE7xD1M3oRqdjoirmAhdATcd981pRXI9WyUqVNr1qAPA4PjVXjutDB5RTRWKSZuE4stWQs0bogZ2zzlJY7iIXv0PnN1eC25WaEJ2vUjldPobsyKvjDqh/QjSxeGGJ+f0roVunx5Y0CGdaOKK8uqirxMrCzVfLByjIJXNXWuaRKTALADOHN6O2ymQa2yewFR47yb7DUJi8vgexMj81Mc6wnJ04JpeANKhGkZx2VIAchuXpiamkAG55YZQUde43stRy2cIO67HRIZAsqMzBuoj4YAI8jC00VlcGcBGLiD+hb30o/574gZQ+uHe4iUCikL2lTkk8gi/nJooa4WSzgXEifc6J6zfOl8PQBVXOP1mLKcCWhYo6C3XIAHabjPi6BlZ8VwV5mQUaQ2FOOucyNF4lVYhw2q+l/t+DsQTQd8eNC7o9CHeKlfmMcKG8trjSOTx+1cq4IoPPq5D1atx4CikA2t8jfeH5uAZ6k4Fqrf0eY89BvrwwlIiRzAY6owEJDT94Dd/dNLK4yZSwxzdNNBxk1HYEhKcoJ9Ae4o5UisoIVWRdzA++YPkKA6gr3kBGiCVoU1xJAN9ewRnzD52yLSOVPMq7vaCmlPtOu+hpD03ufbU8CWM4T+dnJAqXiJSw+9NcPfauHanUWtFi+QMwUDacEFLAkD2WtURytBFumGbancBaq8m0UcicDq4koh9r3GfwWPGNUkcaJsWJUriqqA30"
os.environ["AWS_REGION"] = "us-west-2"

# 导入 app.py 中的函数
sys.path.insert(0, os.path.dirname(__file__))
from app import generate_vector_config

def test_config_generation():
    """测试配置生成"""
    print("=== 测试 Vector 配置生成 ===\n")
    
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
        
        print("✓ 配置生成成功\n")
        print("=== Vector 配置 ===")
        print(config_toml)
        
        # 保存到文件
        config_file = Path("/tmp/vector-test-config.toml")
        config_file.write_text(config_toml)
        print(f"\n✓ 配置已保存到: {config_file}")
        
        # 验证 TOML 格式
        try:
            config_dict = toml.loads(config_toml)
            print("✓ TOML 格式验证通过")
            
            # 检查关键配置
            print("\n=== 配置检查 ===")
            print(f"S3 Bucket: {config_dict['sources']['s3_slowlogs']['bucket']}")
            print(f"S3 Prefix: {config_dict['sources']['s3_slowlogs']['key_prefix']}")
            print(f"Transforms: {list(config_dict['transforms'].keys())}")
            print(f"Sinks: {list(config_dict['sinks'].keys())}")
            
            # 检查 split_lines transform
            if 'split_lines' in config_dict['transforms']:
                print(f"✓ split_lines transform 存在")
                split_config = config_dict['transforms']['split_lines']
                print(f"  - Type: {split_config['type']}")
                print(f"  - Field: {split_config.get('field', 'N/A')}")
                print(f"  - Separator: {repr(split_config.get('separator', 'N/A'))}")
            else:
                print("⚠️  split_lines transform 不存在")
            
        except Exception as e:
            print(f"❌ TOML 解析失败: {e}")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ 配置生成失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_vector_dry_run():
    """测试 Vector dry-run"""
    print("\n=== 测试 Vector Dry-Run ===\n")
    
    config_file = "/tmp/vector-test-config.toml"
    if not Path(config_file).exists():
        print("❌ 配置文件不存在，请先运行配置生成测试")
        return False
    
    # 查找 vector 二进制
    import shutil
    vector_binary = shutil.which("vector")
    if not vector_binary:
        # 尝试查找项目中的 vector
        project_root = Path(__file__).parent.parent
        for path in [project_root / "target" / "release" / "vector",
                     project_root / "target" / "debug" / "vector"]:
            if path.exists():
                vector_binary = str(path)
                break
    
    if not vector_binary:
        print("⚠️  Vector 二进制未找到，跳过 dry-run 测试")
        print("   请确保 Vector 在 PATH 中，或设置 VECTOR_BINARY 环境变量")
        return None
    
    print(f"使用 Vector: {vector_binary}")
    
    import subprocess
    try:
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
                print(result.stdout)
            return True
        else:
            print("❌ Vector dry-run 失败")
            print(f"返回码: {result.returncode}")
            if result.stderr:
                print("\n错误信息:")
                print(result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Vector dry-run 超时")
        return False
    except Exception as e:
        print(f"❌ Vector dry-run 异常: {e}")
        return False

if __name__ == "__main__":
    print("开始测试...\n")
    
    # 测试配置生成
    if not test_config_generation():
        sys.exit(1)
    
    # 测试 Vector dry-run
    result = test_vector_dry_run()
    if result is False:
        sys.exit(1)
    
    print("\n=== 测试完成 ===")
    print("\n下一步:")
    print("1. 确保 MySQL 正在运行")
    print("2. 运行: python3 app.py")
    print("3. 在另一个终端创建任务:")
    print("   curl -X POST http://localhost:8080/api/v1/tasks \\")
    print("     -H 'Content-Type: application/json' \\")
    print("     -d @test_request.json")
