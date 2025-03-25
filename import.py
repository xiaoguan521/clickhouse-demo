import os
import glob
import pandas as pd
from datetime import datetime
import clickhouse_connect
from concurrent.futures import ThreadPoolExecutor
import threading
import time
from tqdm import tqdm

# 创建线程本地存储
thread_local = threading.local()

def get_client():
    if not hasattr(thread_local, "client"):
        thread_local.client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='default',
            password='yourpassword'
        )
    return thread_local.client

def create_table():
    client = get_client()
    client.command('''
        CREATE TABLE IF NOT EXISTS api_metrics (
            service_name String,
            endpoint String,
            timestamp DateTime,
            cpm Float32,
            latency Float32,
            query_start_time DateTime,
            query_end_time DateTime
        ) ENGINE = MergeTree()
        ORDER BY timestamp
    ''')
    print("Table created/verified successfully")

def parse_datetime(dt_str):
    try:
        return datetime.strptime(dt_str, '%Y-%m-%d %H%M')
    except Exception as e:
        print(f"Error parsing date: {dt_str}, error: {str(e)}")
        return None

def process_csv(file):
    try:
        df = pd.read_csv(file, 
            names=[
                'service_name', 'endpoint', 'timestamp', 'cpm', 
                'latency', 'query_start_time', 'query_end_time'
            ],
            skiprows=1
        )
        
        # 转换时间列
        for col in ['timestamp', 'query_start_time', 'query_end_time']:
            df[col] = df[col].apply(parse_datetime)
        
        # 转换数值列为float类型
        df['cpm'] = df['cpm'].astype(float)
        df['latency'] = df['latency'].astype(float)
        
        # 删除包含无效时间的行
        df = df.dropna()
        
        return df
    except Exception as e:
        print(f"Error processing {file}: {str(e)}")
        return None

def import_dataframe(df, client):
    try:
        client.insert_df('api_metrics', df)
        return True
    except Exception as e:
        print(f"Error importing data: {str(e)}")
        return False

def import_file(file):
    try:
        client = get_client()
        
        # 处理 CSV 文件
        df = process_csv(file)
        if df is None:
            return (file, False, "Failed to process CSV")
        
        # 分批导入数据（每批 1000 行）
        batch_size = 1000
        success = True
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i + batch_size]
            if not import_dataframe(batch_df, client):
                success = False
                break
        
        return (file, success, None if success else "Import failed")
    except Exception as e:
        return (file, False, str(e))

def count_csv_files():
    # 直接统计所有CSV文件
    csv_files = glob.glob(os.path.join(CSV_DIR, '**', '*明细.csv'), recursive=True)
    total_files = len(csv_files)
    print(f"\nFound {total_files} CSV files")
    return csv_files

def main():
    try:
        # 首先创建表
        create_table()
        
        # 创建日志目录
        os.makedirs('import_logs', exist_ok=True)
        
        # 统计文件数
        csv_files = count_csv_files()
        if not csv_files:
            print("No CSV files found!")
            return
            
        # 添加用户确认步骤
        user_input = input("Do you want to proceed with the import? (y/n): ").lower()
        if user_input != 'y':
            print("Import cancelled by user")
            return

        # 记录开始时间
        start_time = time.time()
        processed_files = 0
        success_count = 0
        error_count = 0
        
        # 创建日志文件
        success_log = open('import_logs/success.log', 'w', encoding='utf-8')
        error_log = open('import_logs/error.log', 'w', encoding='utf-8')
        
        try:
            with ThreadPoolExecutor(max_workers=8) as executor:
                pbar = tqdm(
                    total=len(csv_files), 
                    desc="Processing files",
                    bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {postfix}]'
                )
                
                def completion_callback(future):
                    nonlocal processed_files, success_count, error_count
                    processed_files += 1
                    pbar.update(1)
                    
                    # 获取处理结果并立即写入日志
                    file, success, error = future.result()
                    if success:
                        success_log.write(f"{file} imported successfully\n")
                        success_log.flush()
                        success_count += 1
                    else:
                        error_message = f"Error importing {file}: {error}\n"
                        error_log.write(error_message)
                        error_log.flush()
                        error_count += 1
                    
                    # 更新进度条
                    elapsed_time = time.time() - start_time
                    files_per_second = processed_files / elapsed_time
                    pbar.set_postfix({
                        'Speed': f'{files_per_second:.2f} files/sec',
                        'Success': success_count,
                        'Failed': error_count
                    })
                
                # 逐个提交任务并立即处理结果
                for file in csv_files:
                    future = executor.submit(import_file, file)
                    future.add_done_callback(completion_callback)
                
                # 等待所有任务完成
                executor.shutdown(wait=True)
                pbar.close()
            
            # 计算总耗时
            total_time = time.time() - start_time
            
            print(f"""
            Import completed:
            - Total files: {len(csv_files)}
            - Successfully imported: {success_count}
            - Failed: {error_count}
            - Total time: {total_time:.2f} seconds
            - Average speed: {len(csv_files)/total_time:.2f} files/second
            """)
            
        finally:
            # 确保日志文件被关闭
            success_log.close()
            error_log.close()

    except Exception as e:
        print(f"Main process error: {str(e)}")

# 指定CSV文件目录
CSV_DIR = r'E:\监控数据'  # 使用原始字符串标记

if __name__ == "__main__":
    main()