import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime
import clickhouse_connect
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import time
from tqdm import tqdm
import psutil
import gc

# 全局配置参数
CONFIG = {
    'batch_size': 10000,  # 增大批处理大小
    'max_workers': None,  # 自动根据CPU核心数设置
    'ch_settings': {
        'host': 'localhost',
        'port': 8123,
        'username': 'default',
        'password': 'yourpassword',
        'connect_timeout': 10,
        'send_receive_timeout': 300,  # 增加超时时间
        'compression': True,  # 启用压缩
        'settings': {
            'max_insert_threads': 4,  # ClickHouse服务器端使用的插入线程
            'max_insert_block_size': 1000000,
            'input_format_parallel_parsing': 1
        }
    }
}

# 如果未指定workers数量，根据CPU数量设置
if CONFIG['max_workers'] is None:
    CONFIG['max_workers'] = max(1, min(multiprocessing.cpu_count() - 1, 16))  # 保留一个核心给操作系统

def get_client():
    """获取ClickHouse客户端连接"""
    return clickhouse_connect.get_client(**CONFIG['ch_settings'])

def create_table():
    """创建数据表"""
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
        PARTITION BY toYYYYMM(timestamp)  
        ORDER BY timestamp
        SETTINGS index_granularity = 8192
    ''')
    print("Table created/verified successfully")

def batch_parse_datetime(dt_series):
    """批量处理日期时间列"""
    def parser(x):
        if pd.isna(x):
            return None
        try:
            return datetime.strptime(str(x), '%Y-%m-%d %H%M')
        except:
            return None
    
    # 使用向量化操作处理
    return pd.to_datetime(dt_series, format='%Y-%m-%d %H%M', errors='coerce')

def process_csv(file):
    """处理CSV文件并返回DataFrame"""
    try:
        # 使用更高效的CSV读取方式
        df = pd.read_csv(
            file,
            names=['service_name', 'endpoint', 'timestamp', 'cpm', 'latency', 'query_start_time', 'query_end_time'],
            skiprows=1,
            dtype={
                'service_name': str,
                'endpoint': str,
                'cpm': np.float32,  # 明确指定数据类型，减少转换开销
                'latency': np.float32
            }
        )
        
        # 批量转换时间列
        for col in ['timestamp', 'query_start_time', 'query_end_time']:
            df[col] = batch_parse_datetime(df[col])
        
        # 删除包含无效数据的行
        df = df.dropna()
        
        # 主动垃圾回收
        gc.collect()
        
        return df
    except Exception as e:
        print(f"Error processing {file}: {str(e)}")
        return None

def chunk_dataframe(df, chunk_size):
    """将DataFrame分成多个块"""
    for i in range(0, len(df), chunk_size):
        yield df.iloc[i:i + chunk_size]

def import_file_process(file):
    """作为单独进程处理和导入文件"""
    try:
        client = get_client()
        memory_usage_before = psutil.Process().memory_info().rss / (1024 * 1024)
        
        # 处理CSV文件
        df = process_csv(file)
        if df is None:
            return (file, False, "Failed to process CSV")
        
        file_size = os.path.getsize(file) / (1024 * 1024)  # MB
        row_count = len(df)
        
        # 使用更大的批次导入数据
        success = True
        chunk_size = CONFIG['batch_size']
        
        # 尝试批量插入
        try:
            client.insert_df('api_metrics', df)
        except Exception as e:
            # 如果批量插入失败，尝试分块插入
            success = True
            for chunk in chunk_dataframe(df, chunk_size):
                try:
                    client.insert_df('api_metrics', chunk)
                except Exception as e:
                    success = False
                    print(f"Error importing chunk: {str(e)}")
                    break
        
        # 清理内存
        del df
        gc.collect()
        
        memory_usage_after = psutil.Process().memory_info().rss / (1024 * 1024)
        
        result = {
            'file': file,
            'success': success,
            'error': None if success else "Import failed",
            'rows': row_count,
            'file_size_mb': file_size,
            'memory_delta_mb': memory_usage_after - memory_usage_before
        }
        
        return result
    except Exception as e:
        return {
            'file': file,
            'success': False,
            'error': str(e),
            'rows': 0,
            'file_size_mb': 0,
            'memory_delta_mb': 0
        }

def count_csv_files():
    """统计CSV文件并返回路径列表"""
    csv_files = glob.glob(os.path.join(CSV_DIR, '**', '*明细.csv'), recursive=True)
    total_files = len(csv_files)
    print(f"\n找到 {total_files} 个CSV文件")
    return csv_files

def main():
    try:
        # 创建表结构
        create_table()
        
        # 创建日志目录
        os.makedirs('import_logs', exist_ok=True)
        
        # 统计文件数
        csv_files = count_csv_files()
        if not csv_files:
            print("未找到CSV文件!")
            return
            
        # 添加用户确认步骤
        print(f"将使用 {CONFIG['max_workers']} 个并行进程导入数据，每批 {CONFIG['batch_size']} 行")
        user_input = input("是否继续导入? (y/n): ").lower()
        if user_input != 'y':
            print("导入已被用户取消")
            return

        # 记录开始时间
        start_time = time.time()
        processed_files = 0
        success_count = 0
        error_count = 0
        total_rows = 0
        
        # 创建日志文件
        success_log = open('import_logs/success.log', 'w', encoding='utf-8')
        error_log = open('import_logs/error.log', 'w', encoding='utf-8')
        stats_log = open('import_logs/stats.log', 'w', encoding='utf-8')
        
        try:
            print(f"开始导入 {len(csv_files)} 个文件...")
            
            with ProcessPoolExecutor(max_workers=CONFIG['max_workers']) as executor:
                pbar = tqdm(
                    total=len(csv_files), 
                    desc="处理文件中",
                    bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {postfix}]'
                )
                
                # 创建任务列表
                future_to_file = {executor.submit(import_file_process, file): file for file in csv_files}
                
                # 处理结果
                for future in tqdm(
                    future_to_file, 
                    total=len(future_to_file), 
                    desc="导入进度",
                    leave=False
                ):
                    try:
                        # 获取结果
                        result = future.result()
                        processed_files += 1
                        pbar.update(1)
                        
                        # 更新统计信息
                        if result['success']:
                            success_log.write(f"{result['file']} 成功导入，行数: {result['rows']}\n")
                            success_log.flush()
                            success_count += 1
                            total_rows += result['rows']
                        else:
                            error_message = f"导入错误 {result['file']}: {result['error']}\n"
                            error_log.write(error_message)
                            error_log.flush()
                            error_count += 1
                        
                        # 记录详细统计信息
                        stats_log.write(f"{result['file']},{result['success']},{result['rows']},{result['file_size_mb']:.2f},{result['memory_delta_mb']:.2f}\n")
                        stats_log.flush()
                        
                        # 更新进度条
                        elapsed_time = time.time() - start_time
                        files_per_second = processed_files / max(0.1, elapsed_time)
                        rows_per_second = total_rows / max(0.1, elapsed_time)
                        pbar.set_postfix({
                            '速度': f'{files_per_second:.2f} 文件/秒',
                            '行/秒': f'{rows_per_second:.0f}',
                            '成功': success_count,
                            '失败': error_count
                        })
                    except Exception as e:
                        print(f"处理结果时出错: {str(e)}")
                
                pbar.close()
            
            # 计算总耗时
            total_time = time.time() - start_time
            
            print(f"""
            导入完成:
            - 总文件数: {len(csv_files)}
            - 成功导入: {success_count}
            - 失败: {error_count}
            - 总行数: {total_rows}
            - 总耗时: {total_time:.2f} 秒
            - 平均速度: {len(csv_files)/total_time:.2f} 文件/秒
            - 数据导入速度: {total_rows/total_time:.2f} 行/秒
            """)
            
        finally:
            # 确保日志文件被关闭
            success_log.close()
            error_log.close()
            stats_log.close()

    except Exception as e:
        print(f"主程序错误: {str(e)}")

# 指定CSV文件目录
CSV_DIR = r'E:\监控数据'  # 使用原始字符串标记

if __name__ == "__main__":
    main()