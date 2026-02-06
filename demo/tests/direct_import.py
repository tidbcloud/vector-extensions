#!/usr/bin/env python3
"""
直接从 S3 Parquet 文件读取 slowlogs 并写入 MySQL（用于快速测试）
"""
import os
import sys
import boto3
import pymysql
from datetime import datetime
from pathlib import Path

# 设置 AWS 凭证
os.environ["AWS_ACCESS_KEY_ID"] = "ASIAYBEGSUMKNOBLWYE5"
os.environ["AWS_SECRET_ACCESS_KEY"] = "hemUNrcxvz3qD5d8nlvw8ldLdzJI/v9YX5R/rKRY"
os.environ["AWS_SESSION_TOKEN"] = "IQoJb3JpZ2luX2VjEF8aDmFwLW5vcnRoZWFzdC0xIkgwRgIhAOz0wL3K/As9Ka48eiYkSWOvKH7exXuPyg5ZDY0xGh2lAiEAhwKUDmDtFdP9si7BZ7LEdtin96MT3r1R5/s9cIPGmyEqiQMIKBABGgw1NTIxODU1MzczMDAiDCbOE7xD1M3oRqdjoirmAhdATcd981pRXI9WyUqVNr1qAPA4PjVXjutDB5RTRWKSZuE4stWQs0bogZ2zzlJY7iIXv0PnN1eC25WaEJ2vUjldPobsyKvjDqh/QjSxeGGJ+f0roVunx5Y0CGdaOKK8uqirxMrCzVfLByjIJXNXWuaRKTALADOHN6O2ymQa2yewFR47yb7DUJi8vgexMj81Mc6wnJ04JpeANKhGkZx2VIAchuXpiamkAG55YZQUde43stRy2cIO67HRIZAsqMzBuoj4YAI8jC00VlcGcBGLiD+hb30o/574gZQ+uHe4iUCikL2lTkk8gi/nJooa4WSzgXEifc6J6zfOl8PQBVXOP1mLKcCWhYo6C3XIAHabjPi6BlZ8VwV5mQUaQ2FOOucyNF4lVYhw2q+l/t+DsQTQd8eNC7o9CHeKlfmMcKG8trjSOTx+1cq4IoPPq5D1atx4CikA2t8jfeH5uAZ6k4Fqrf0eY89BvrwwlIiRzAY6owEJDT94Dd/dNLK4yZSwxzdNNBxk1HYEhKcoJ9Ae4o5UisoIVWRdzA++YPkKA6gr3kBGiCVoU1xJAN9ewRnzD52yLSOVPMq7vaCmlPtOu+hpD03ufbU8CWM4T+dnJAqXiJSw+9NcPfauHanUWtFi+QMwUDacEFLAkD2WtURytBFumGbancBaq8m0UcicDq4koh9r3GfwWPGNUkcaJsWJUriqqA30"
os.environ["AWS_REGION"] = "us-west-2"

def list_parquet_files(bucket, prefix, max_files=10):
    """列出 S3 中的 Parquet 文件"""
    s3 = boto3.client('s3', region_name='us-west-2')
    files = []
    
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if 'part-' in key and key.endswith('.parquet'):
                files.append(key)
                if len(files) >= max_files:
                    return files
    
    return files

def read_parquet_from_s3(bucket, key):
    """从 S3 读取 Parquet 文件"""
    try:
        import pyarrow.parquet as pq
        import io
        
        s3 = boto3.client('s3', region_name='us-west-2')
        obj = s3.get_object(Bucket=bucket, Key=key)
        parquet_file = pq.ParquetFile(io.BytesIO(obj['Body'].read()))
        return parquet_file.read().to_pandas()
    except ImportError:
        print("需要安装 pyarrow: pip install pyarrow")
        return None
    except Exception as e:
        print(f"读取 Parquet 文件失败: {e}")
        return None

def import_to_mysql(df, mysql_connection, mysql_table, task_id="direct-import"):
    """将 DataFrame 导入 MySQL"""
    # 解析 MySQL 连接
    mysql_parts = mysql_connection.replace("mysql://", "").split("@")
    user_pass = mysql_parts[0].split(":")
    mysql_user, mysql_pass = user_pass
    host_port = mysql_parts[1].split("/")
    host_port_parts = host_port[0].split(":")
    mysql_host = host_port_parts[0]
    mysql_port = int(host_port_parts[1]) if len(host_port_parts) > 1 else 3306
    mysql_database = host_port[1]
    
    try:
        conn = pymysql.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_pass,
            database=mysql_database,
            charset='utf8mb4'
        )
        cursor = conn.cursor()
        
        total_imported = 0
        batch_size = 100
        
        # TiDB slowlog 是结构化数据，需要转换为文本格式
        # 或者直接存储为 JSON
        print("将结构化数据转换为文本格式...")
        
        for idx, row in df.iterrows():
            # 构建 slowlog 文本行（模拟 TiDB slowlog 格式）
            # 提取关键字段
            time_val = row.get('time', '')
            db = row.get('db', '')
            user = row.get('user', '')
            host = row.get('host', '')
            query_time = row.get('query_time', '')
            result_rows = row.get('result_rows', '')
            
            # 尝试找到 SQL 语句（可能在 prev_stmt 或其他字段）
            sql_stmt = row.get('prev_stmt', '') or row.get('digest', '')
            
            # 构建 slowlog 文本行
            log_line = f"# Time: {time_val}\n# User@Host: {user}[{user}] @ {host}\n# Query_time: {query_time}  Rows_examined: {result_rows}\n{sql_stmt}"
            
            # 或者存储为 JSON（包含所有字段）
            # log_line = json.dumps(row.to_dict())
            
            timestamp = datetime.now().isoformat()
            
            sql = f"INSERT INTO {mysql_table} (log_line, log_timestamp, task_id) VALUES (%s, %s, %s)"
            cursor.execute(sql, (log_line, timestamp, task_id))
            total_imported += 1
            
            if total_imported % batch_size == 0:
                conn.commit()
                print(f"✓ 已导入 {total_imported} 条记录...")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✓ 总共导入 {total_imported} 条记录到 MySQL")
        return total_imported
        
    except Exception as e:
        print(f"❌ MySQL 导入失败: {e}")
        import traceback
        traceback.print_exc()
        return 0

def main():
    bucket = "o11y-dev-shared-us-west-2"
    prefix = "deltalake/slowlogs/"
    mysql_connection = "mysql://root:root@localhost:3306/testdb"
    mysql_table = "slowlogs"
    
    print("=== 直接从 S3 Parquet 导入 Slowlogs 到 MySQL ===\n")
    
    # 1. 列出 Parquet 文件
    print("1. 查找 Parquet 文件...")
    files = list_parquet_files(bucket, prefix, max_files=5)
    if not files:
        print("❌ 未找到 Parquet 文件")
        return
    
    print(f"✓ 找到 {len(files)} 个 Parquet 文件")
    for f in files[:3]:
        print(f"  - {f}")
    
    # 2. 读取第一个文件
    print(f"\n2. 读取文件: {files[0]}")
    df = read_parquet_from_s3(bucket, files[0])
    if df is None:
        return
    
    print(f"✓ 读取成功，共 {len(df)} 行")
    print(f"✓ 列名: {list(df.columns)}")
    print(f"\n前 3 行数据:")
    print(df.head(3))
    
    # 3. 导入 MySQL
    print(f"\n3. 导入 MySQL...")
    total = import_to_mysql(df, mysql_connection, mysql_table)
    
    if total > 0:
        print(f"\n✓ 成功导入 {total} 条记录")
        print(f"\n验证:")
        print(f"  mysql -h localhost -u root -proot testdb -e 'SELECT COUNT(*) FROM slowlogs;'")
        print(f"  mysql -h localhost -u root -proot testdb -e 'SELECT * FROM slowlogs LIMIT 5;'")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n中断")
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()
