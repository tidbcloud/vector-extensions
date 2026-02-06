#!/usr/bin/env python3
"""
MySQL Writer - Vector exec sink script
This script receives JSON Lines from stdin and writes them to MySQL.

This is a demo implementation that will be converted to a proper Vector plugin later.
The script is executed by Vector's exec sink to handle data output.
"""
import sys
import json
import os
import pymysql
from datetime import datetime

# Configuration from environment variables (set by Vector or the management API)
MYSQL_HOST = os.environ.get('MYSQL_HOST', 'localhost')
MYSQL_PORT = int(os.environ.get('MYSQL_PORT', '3306'))
MYSQL_USER = os.environ.get('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', '')
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE', 'testdb')
MYSQL_TABLE = os.environ.get('MYSQL_TABLE', 'slowlogs')
TASK_ID = os.environ.get('TASK_ID', '')


def write_to_mysql():
    """Read JSON Lines from stdin and write to MySQL"""
    # Connect to MySQL
    try:
        conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            charset='utf8mb4'
        )
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error connecting to MySQL: {e}", file=sys.stderr)
        sys.exit(1)
    
    batch_size = 100
    batch = []
    total_imported = 0
    
    try:
        # Read JSON Lines from stdin (Vector exec sink sends data here)
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            
            try:
                # Parse JSON event
                event = json.loads(line)
                
                # Extract message field (the slowlog line)
                message = event.get('message', '')
                if not message:
                    # Try other common fields
                    message = event.get('log', event.get('text', line))
                
                # Get timestamp
                timestamp_str = event.get('timestamp')
                if timestamp_str:
                    try:
                        # Convert ISO 8601 to MySQL DATETIME format
                        ts_str = timestamp_str.replace('Z', '+00:00')
                        dt = datetime.fromisoformat(ts_str)
                        # Convert to MySQL datetime format: YYYY-MM-DD HH:MM:SS
                        mysql_timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        mysql_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                else:
                    mysql_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # Prepare insert statement
                sql = f"INSERT INTO {MYSQL_TABLE} (log_line, log_timestamp, task_id) VALUES (%s, %s, %s)"
                batch.append((message, mysql_timestamp, TASK_ID))
                
                # Batch insert for efficiency
                if len(batch) >= batch_size:
                    cursor.executemany(sql, batch)
                    conn.commit()
                    total_imported += len(batch)
                    print(f"Imported {len(batch)} lines (total: {total_imported})", file=sys.stderr)
                    batch = []
                    
            except json.JSONDecodeError as e:
                # If not JSON, insert as plain text
                sql = f"INSERT INTO {MYSQL_TABLE} (log_line, log_timestamp, task_id) VALUES (%s, %s, %s)"
                mysql_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                batch.append((line, mysql_timestamp, TASK_ID))
                
                if len(batch) >= batch_size:
                    cursor.executemany(sql, batch)
                    conn.commit()
                    total_imported += len(batch)
                    print(f"Imported {len(batch)} lines (total: {total_imported})", file=sys.stderr)
                    batch = []
            except Exception as e:
                print(f"Error processing line: {e}", file=sys.stderr)
                continue
        
        # Insert remaining batch
        if batch:
            sql = f"INSERT INTO {MYSQL_TABLE} (log_line, log_timestamp, task_id) VALUES (%s, %s, %s)"
            cursor.executemany(sql, batch)
            conn.commit()
            total_imported += len(batch)
            print(f"Imported final {len(batch)} lines (total: {total_imported})", file=sys.stderr)
        
        print(f"Finished importing {total_imported} total lines to MySQL table {MYSQL_TABLE}", file=sys.stderr)
        
    except KeyboardInterrupt:
        # Insert remaining batch on interrupt
        if batch:
            sql = f"INSERT INTO {MYSQL_TABLE} (log_line, log_timestamp, task_id) VALUES (%s, %s, %s)"
            cursor.executemany(sql, batch)
            conn.commit()
            total_imported += len(batch)
        print(f"Interrupted. Imported {total_imported} total lines", file=sys.stderr)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    write_to_mysql()
