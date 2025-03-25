from clickhouse_connect import get_client
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns

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

def get_top_10_endpoints():
    client = create_client()
    
    # 查询每分钟请求次数总和的前10名
    query = '''
    SELECT 
        service_name,
        endpoint,
        sum(cpm) as total_cpm,
        avg(latency) as avg_latency,
        count() as total_requests
    FROM api_metrics
    GROUP BY service_name, endpoint
    ORDER BY total_cpm DESC
    LIMIT 10
    '''
    
    result = client.query(query)
    
    # 转换为DataFrame
    df = pd.DataFrame(result.result_rows, columns=[
        'service_name', 'endpoint', 'total_cpm', 'avg_latency', 'total_requests'
    ])
    
    return df

def plot_top_10_cpm(df):
    # 设置中文字体
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
    
    # 创建图形
    plt.figure(figsize=(15, 8))
    
    # 创建条形图
    sns.barplot(data=df, x='total_cpm', y='endpoint', hue='service_name')
    
    # 设置标题和标签
    plt.title('Top 10 接口每分钟请求次数统计', fontsize=14, pad=20)
    plt.xlabel('每分钟请求次数 (CPM)', fontsize=12)
    plt.ylabel('接口路径', fontsize=12)
    
    # 调整布局
    plt.tight_layout()
    
    # 保存图片
    plt.savefig('top_10_cpm.png', dpi=300, bbox_inches='tight')
    plt.close()

def main():
    try:
        print("开始统计 Top 10 接口请求次数...")
        
        # 获取统计数据
        df = get_top_10_endpoints()
        
        # 打印统计结果
        print("\nTop 10 接口统计结果：")
        print("=" * 100)
        print(f"{'服务名称':<20} {'接口路径':<50} {'总CPM':<10} {'平均延迟(ms)':<15} {'总请求数':<10}")
        print("-" * 100)
        
        for _, row in df.iterrows():
            print(f"{row['service_name']:<20} {row['endpoint']:<50} {row['total_cpm']:<10.2f} {row['avg_latency']:<15.2f} {row['total_requests']:<10}")
        
        print("=" * 100)
        
        # 生成可视化图表
        print("\n正在生成统计图表...")
        plot_top_10_cpm(df)
        print("统计图表已保存为 top_10_cpm.png")
        
    except Exception as e:
        print(f"统计过程中发生错误: {str(e)}")

if __name__ == "__main__":
    main() 