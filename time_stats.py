from clickhouse_connect import get_client
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta

def create_client():
    try:
        # 创建客户端连接
        client = get_client(
            host='localhost',
            port=8123,  # 使用 HTTP 接口
            username='default',
            password='yourpassword'
        )
        return client
    except Exception as e:
        print(f"连接失败: {e}")
        raise

def get_top_10_by_timestamp():
    client = create_client()
    
    # 查询每个时间点的请求次数总和的前10名
    query = '''
    SELECT 
        timestamp,
        sum(cpm) as total_cpm
    FROM api_metrics
    GROUP BY timestamp
    ORDER BY total_cpm DESC
    LIMIT 10
    '''
    
    result = client.query(query)
    
    # 转换为DataFrame
    df = pd.DataFrame(result.result_rows, columns=[
        'timestamp', 'total_cpm'
    ])
    
    return df

def plot_time_series_top_10(df):
    # 设置中文字体
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
    
    # 创建图形
    plt.figure(figsize=(15, 8))
    
    # 创建时间序列图
    plt.plot(df['timestamp'], df['total_cpm'], marker='o')
    
    # 设置标题和标签
    plt.title('Top 10 时间点CPM统计', fontsize=14, pad=20)
    plt.xlabel('时间', fontsize=12)
    plt.ylabel('每分钟请求次数 (CPM)', fontsize=12)
    
    # 调整x轴标签角度
    plt.xticks(rotation=45)
    
    # 调整布局
    plt.tight_layout()
    
    # 保存图片
    plt.savefig('time_series_top_10_cpm.png', dpi=300, bbox_inches='tight')
    plt.close()

def main():
    try:
        print("开始统计时间序列 Top 10 CPM...")
        
        # 获取统计数据
        df = get_top_10_by_timestamp()
        
        # 打印统计结果
        print("\n时间序列 Top 10 统计结果：")
        print("=" * 60)
        print(f"{'时间戳':<30} {'总CPM':<10}")
        print("-" * 60)
        
        for _, row in df.iterrows():
            print(f"{row['timestamp'].strftime('%Y-%m-%d %H:%M:%S'):<30} {row['total_cpm']:<10.2f}")
        
        print("=" * 60)
        
        # 生成可视化图表
        print("\n正在生成统计图表...")
        plot_time_series_top_10(df)
        print("统计图表已保存为 time_series_top_10_cpm.png")
        
    except Exception as e:
        print(f"统计过程中发生错误: {str(e)}")

if __name__ == "__main__":
    main() 