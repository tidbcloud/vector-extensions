#!/usr/bin/env python3
"""
Parquet S3 Processor - Vector exec source script
This script processes Parquet files from S3 and outputs JSON Lines to stdout.

This is a demo implementation that will be converted to a proper Vector plugin later.
The script is executed by Vector's exec source to handle data acquisition.
"""
import sys
import json
import os
import boto3
import pyarrow.parquet as pq
from datetime import datetime

# Configuration from environment variables (set by Vector or the management API)
S3_BUCKET = os.environ.get('S3_BUCKET', '')
S3_PREFIX = os.environ.get('S3_PREFIX', '')
S3_REGION = os.environ.get('S3_REGION', 'us-west-2')
START_TIME = os.environ.get('START_TIME', None)
END_TIME = os.environ.get('END_TIME', None)
TASK_ID = os.environ.get('TASK_ID', 'default')  # Task ID for database tracking

# AWS credentials from environment (inherited from Vector process)
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN


def process_parquet_files():
    """Download and process Parquet files from S3, output JSON Lines to stdout"""
    if not S3_BUCKET or not S3_PREFIX:
        print("Error: S3_BUCKET and S3_PREFIX must be set", file=sys.stderr)
        sys.exit(1)
    
    s3 = boto3.client('s3', region_name=S3_REGION)
    
    # List Parquet files
    parquet_files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if 'part-' in key and key.endswith('.parquet'):
                # Filter by date if time range provided
                if START_TIME or END_TIME:
                    if 'date=' in key:
                        date_str = key.split('date=')[1].split('/')[0]
                        try:
                            file_date = datetime.strptime(date_str, '%Y%m%d')
                            if START_TIME:
                                start_dt = datetime.fromisoformat(START_TIME.replace('Z', '+00:00'))
                                if file_date < start_dt.date():
                                    continue
                            if END_TIME:
                                end_dt = datetime.fromisoformat(END_TIME.replace('Z', '+00:00'))
                                if file_date > end_dt.date():
                                    continue
                        except:
                            pass  # Include if date parsing fails
                parquet_files.append(key)
    
    if not parquet_files:
        print("No Parquet files found", file=sys.stderr)
        return
    
    # Process each Parquet file
    for parquet_key in parquet_files:
        try:
            # Download to memory - need to read into BytesIO for ParquetFile to work
            import io
            obj = s3.get_object(Bucket=S3_BUCKET, Key=parquet_key)
            # Read entire file into memory (ParquetFile needs seekable stream)
            parquet_bytes = io.BytesIO(obj['Body'].read())
            parquet_data = pq.ParquetFile(parquet_bytes)
            df = parquet_data.read().to_pandas()
            
            # Filter by time range if provided (row-level filtering)
            if START_TIME or END_TIME:
                if 'time' in df.columns:
                    if START_TIME:
                        start_ts = datetime.fromisoformat(START_TIME.replace('Z', '+00:00')).timestamp()
                        df = df[df['time'] >= start_ts]
                    if END_TIME:
                        end_ts = datetime.fromisoformat(END_TIME.replace('Z', '+00:00')).timestamp()
                        df = df[df['time'] <= end_ts]
            
            # Convert each row to slowlog text format and output as JSON Lines
            for _, row in df.iterrows():
                time_val = row.get('time', '')
                db = row.get('db', '')
                user = row.get('user', '')
                host = row.get('host', '')
                query_time = row.get('query_time', '')
                result_rows = row.get('result_rows', '')
                sql_stmt = str(row.get('prev_stmt', '')) or str(row.get('digest', ''))
                
                log_line = f"# Time: {time_val} | DB: {db} | User: {user}@{host} | Query_time: {query_time} | Rows: {result_rows} | SQL: {sql_stmt}"
                
                event = {
                    "message": log_line,
                    "timestamp": datetime.fromtimestamp(time_val).isoformat() if time_val else datetime.now().isoformat(),
                    "source": parquet_key,
                    "task_id": TASK_ID,  # Add task_id for database tracking
                }
                print(json.dumps(event))
                
        except Exception as e:
            print(f"Error processing {parquet_key}: {e}", file=sys.stderr)
            continue


if __name__ == "__main__":
    process_parquet_files()
