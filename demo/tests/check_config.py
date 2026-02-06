#!/usr/bin/env python3
"""
快速检查配置生成逻辑
"""
import sys
import os

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(__file__))

# 模拟 toml 模块（如果不存在）
try:
    import toml
except ImportError:
    print("警告: toml 模块未安装，将使用简单输出")
    class toml:
        @staticmethod
        def dumps(d):
            import json
            return json.dumps(d, indent=2)

# 导入配置生成函数
try:
    from app import generate_vector_config
    
    print("=== 测试配置生成 ===\n")
    
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
    
    print("✓ 配置生成成功\n")
    print("=== 生成的配置 ===")
    print(config)
    
    # 检查关键部分
    print("\n=== 配置检查 ===")
    if "deltalake/slowlogs/" in config:
        print("✓ S3 prefix 正确: deltalake/slowlogs/")
    else:
        print("❌ S3 prefix 可能有问题")
    
    if "split_lines" in config:
        print("✓ split_lines transform 存在")
    else:
        print("❌ split_lines transform 缺失")
    
    if "decompress" in config:
        print("✓ decompress transform 存在")
    else:
        print("❌ decompress transform 缺失")
    
    print("\n配置已生成，可以保存到文件进行 Vector 测试")
    
except Exception as e:
    print(f"❌ 错误: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
