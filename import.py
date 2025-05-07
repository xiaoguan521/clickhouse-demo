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
    'max_workers': 16,  # 降低并发进程数，减轻资源压力
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
    },
    'batch_cooldown': 15,  # 每批次处理后的冷却时间（秒）
    'connection_retry_base_delay': 3  # 连接重试基础延迟（秒）
}

# 如果未指定workers数量，根据CPU数量设置
if CONFIG['max_workers'] is None:
    CONFIG['max_workers'] = max(1, min(multiprocessing.cpu_count() - 1, 16))  # 保留一个核心给操作系统

BATCH_FILE_COUNT = 10000  # 每批导入文件数量，减小单批压力

def get_client():
    """获取ClickHouse客户端连接"""
    return clickhouse_connect.get_client(**CONFIG['ch_settings'])

def create_table():
    """创建数据表"""
    client = get_client()
    try:
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
    finally:
        # 确保连接关闭
        client.close()

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

def import_file_process(file, batch_id=0, file_index=0):
    """作为单独进程处理和导入文件"""
    client = None
    max_retries = 3  # 最大重试次数
    retry_delay = CONFIG['connection_retry_base_delay']  # 基础重试间隔秒数
    
    try:
        for attempt in range(1, max_retries + 1):
            try:
                if client is None:
                    client = get_client()
                
                memory_usage_before = psutil.Process().memory_info().rss / (1024 * 1024)
                # 处理CSV文件
                df = process_csv(file)
                if df is None:
                    if client:
                        client.close()
                    return (file, False, "Failed to process CSV")
                
                file_size = os.path.getsize(file) / (1024 * 1024)  # MB
                row_count = len(df)
                success = True
                chunk_size = CONFIG['batch_size']
                
                try:
                    client.insert_df('api_metrics', df)
                except Exception as e:
                    # 如果批量插入失败，尝试分块插入
                    success = True
                    for chunk in chunk_dataframe(df, chunk_size):
                        try:
                            client.insert_df('api_metrics', chunk)
                        except Exception as e:
                            # 检查是否为Http Driver Exception或Broken pipe，若是则重建连接
                            if 'Http Driver Exception' in str(e) or 'HTTP' in str(e) or 'Broken pipe' in str(e):
                                if attempt < max_retries:
                                    print(f"[批次{batch_id}][{file_index}/{BATCH_FILE_COUNT}][{file}] Http异常，第{attempt}次重试并重建连接...")
                                    time.sleep(retry_delay * attempt)  # 指数退避策略
                                    # 关闭旧连接
                                    if client:
                                        try:
                                            client.close()
                                        except:
                                            pass
                                    client = get_client()  # 强制重建连接
                                    break  # 跳出for chunk，进入下一个attempt
                                else:
                                    success = False
                                    print(f"[批次{batch_id}][{file_index}/{BATCH_FILE_COUNT}][{file}] Http异常，已达最大重试次数，放弃。")
                                    break
                            else:
                                success = False
                                print(f"Error importing chunk: {str(e)}")
                                break
                    if not success:
                        break  # 跳出重试循环
                
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
                # 检查是否为Http Driver Exception或Broken pipe，若是则重建连接
                if 'Http Driver Exception' in str(e) or 'HTTP' in str(e) or 'Broken pipe' in str(e):
                    if attempt < max_retries:
                        print(f"[批次{batch_id}][{file_index}/{BATCH_FILE_COUNT}][{file}] Http异常，第{attempt}次重试并重建连接...")
                        time.sleep(retry_delay * attempt)  # 指数退避策略
                        # 关闭旧连接
                        if client:
                            try:
                                client.close()
                            except:
                                pass
                        client = get_client()  # 强制重建连接
                        continue
                    else:
                        print(f"[批次{batch_id}][{file_index}/{BATCH_FILE_COUNT}][{file}] Http异常，已达最大重试次数，放弃。")
                        
                return {
                    'file': file,
                    'success': False,
                    'error': str(e),
                    'rows': 0,
                    'file_size_mb': 0,
                    'memory_delta_mb': 0
                }
    finally:
        # 确保连接关闭
        if client:
            try:
                client.close()
            except:
                pass

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
        print(f"将使用 {CONFIG['max_workers']} 个并行进程导入数据，每批 {CONFIG['batch_size']} 行，每批 {BATCH_FILE_COUNT} 个文件")
        print(f"每批处理后休息 {CONFIG['batch_cooldown']} 秒，以释放连接资源")
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
            
            # 添加总进度条
            with tqdm(total=len(csv_files), desc="整体进度", bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {postfix}]') as total_pbar:
                for batch_idx, i in enumerate(range(0, len(csv_files), BATCH_FILE_COUNT)):
                    batch_files = csv_files[i:i+BATCH_FILE_COUNT]
                    batch_start_time = time.time()
                    
                    print(f"\n开始处理第 {batch_idx+1} 批次，共 {len(batch_files)} 个文件")
                    
                    # 批次内的处理逻辑
                    with ProcessPoolExecutor(max_workers=CONFIG['max_workers']) as executor:
                        # 为每个文件提供批次索引和文件索引
                        future_to_file = {executor.submit(import_file_process, file, batch_idx+1, idx+1): file 
                                        for idx, file in enumerate(batch_files)}
                        
                        for idx, future in enumerate(tqdm(future_to_file, total=len(future_to_file), 
                                                      desc=f"批次{batch_idx+1}进度", leave=False)):
                            try:
                                result = future.result()
                                processed_files += 1
                                total_pbar.update(1)
                                
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
                                    
                                stats_log.write(f"{result['file']},{result['success']},{result['rows']},{result['file_size_mb']:.2f},{result['memory_delta_mb']:.2f}\n")
                                stats_log.flush()
                                
                                elapsed_time = time.time() - start_time
                                files_per_second = processed_files / max(0.1, elapsed_time)
                                rows_per_second = total_rows / max(0.1, elapsed_time)
                                
                                total_pbar.set_postfix({
                                    '速度': f'{files_per_second:.2f} 文件/秒',
                                    '行/秒': f'{rows_per_second:.0f}',
                                    '成功': success_count,
                                    '失败': error_count
                                })
                            except Exception as e:
                                print(f"处理结果时出错: {str(e)}")
                    
                    # 每个批次结束后的休息时间
                    batch_time = time.time() - batch_start_time
                    cool_down = CONFIG['batch_cooldown']
                    print(f"\n批次 {batch_idx+1} 完成，处理时间: {batch_time:.2f} 秒，休息 {cool_down} 秒以释放连接资源...")
                    time.sleep(cool_down)
                    
                    # 主动触发垃圾回收
                    gc.collect()
            
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
CSV_DIR = r'/home/clickhouse/test/data'  # 使用原始字符串标记

if __name__ == "__main__":
    main()