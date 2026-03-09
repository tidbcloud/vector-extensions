#!/usr/bin/env python3
"""
Directly read slowlogs from S3 Parquet files and write to MySQL (for quick testing)
"""
import os
import sys
import boto3
import pymysql
from datetime import datetime
from pathlib import Path

# Set AWS credentials

def list_parquet_files(bucket, prefix, max_files=10):
    """List Parquet files in S3"""
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
    """Read Parquet file from S3"""
    try:
        import pyarrow.parquet as pq
        import io
        
        s3 = boto3.client('s3', region_name='us-west-2')
        obj = s3.get_object(Bucket=bucket, Key=key)
        parquet_file = pq.ParquetFile(io.BytesIO(obj['Body'].read()))
        return parquet_file.read().to_pandas()
    except ImportError:
        print("Need to install pyarrow: pip install pyarrow")
        return None
    except Exception as e:
        print(f"Failed to read Parquet file: {e}")
        return None

def import_to_mysql(df, mysql_connection, mysql_table, task_id="direct-import"):
    """Import DataFrame to MySQL"""
    # Parse MySQL connection
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
        
        # TiDB slowlog is structured data, need to convert to text format
        # Or store directly as JSON
        print("Converting structured data to text format...")
        
        for idx, row in df.iterrows():
            # Build slowlog text line (simulating TiDB slowlog format)
            # Extract key fields
            time_val = row.get('time', '')
            db = row.get('db', '')
            user = row.get('user', '')
            host = row.get('host', '')
            query_time = row.get('query_time', '')
            result_rows = row.get('result_rows', '')
            
            # Try to find SQL statement (may be in prev_stmt or other fields)
            sql_stmt = row.get('prev_stmt', '') or row.get('digest', '')
            
            # Build slowlog text line
            log_line = f"# Time: {time_val}\n# User@Host: {user}[{user}] @ {host}\n# Query_time: {query_time}  Rows_examined: {result_rows}\n{sql_stmt}"
            
            # Or store as JSON (includes all fields)
            # log_line = json.dumps(row.to_dict())
            
            timestamp = datetime.now().isoformat()
            
            sql = f"INSERT INTO {mysql_table} (log_line, log_timestamp, task_id) VALUES (%s, %s, %s)"
            cursor.execute(sql, (log_line, timestamp, task_id))
            total_imported += 1
            
            if total_imported % batch_size == 0:
                conn.commit()
                print(f"✓ Imported {total_imported} records...")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✓ Total imported {total_imported} records to MySQL")
        return total_imported
        
    except Exception as e:
        print(f"❌ MySQL import failed: {e}")
        import traceback
        traceback.print_exc()
        return 0

def main():
    bucket = "o11y-dev-shared-us-west-2"
    prefix = "deltalake/slowlogs/"
    mysql_connection = "mysql://root:root@localhost:3306/testdb"
    mysql_table = "slowlogs"
    
    print("=== Direct Import Slowlogs from S3 Parquet to MySQL ===\n")
    
    # 1. List Parquet files
    print("1. Finding Parquet files...")
    files = list_parquet_files(bucket, prefix, max_files=5)
    if not files:
        print("❌ No Parquet files found")
        return
    
    print(f"✓ Found {len(files)} Parquet files")
    for f in files[:3]:
        print(f"  - {f}")
    
    # 2. Read first file
    print(f"\n2. Reading file: {files[0]}")
    df = read_parquet_from_s3(bucket, files[0])
    if df is None:
        return
    
    print(f"✓ Read successfully, {len(df)} rows")
    print(f"✓ Column names: {list(df.columns)}")
    print(f"\nFirst 3 rows:")
    print(df.head(3))
    
    # 3. Import to MySQL
    print(f"\n3. Importing to MySQL...")
    total = import_to_mysql(df, mysql_connection, mysql_table)
    
    if total > 0:
        print(f"\n✓ Successfully imported {total} records")
        print(f"\nVerification:")
        print(f"  mysql -h localhost -u root -proot testdb -e 'SELECT COUNT(*) FROM slowlogs;'")
        print(f"  mysql -h localhost -u root -proot testdb -e 'SELECT * FROM slowlogs LIMIT 5;'")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
