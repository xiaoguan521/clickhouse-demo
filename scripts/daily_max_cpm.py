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

def get_daily_max_cpm():
    client = create_client()
    
    # 查询每天每分钟的CPM总和的最大值
    query = '''
    WITH daily_minute_cpm AS (
        SELECT 
            toDate(timestamp) as date,
            toStartOfMinute(timestamp) as minute,
            sum(cpm) as total_cpm
        FROM api_metrics
        GROUP BY date, minute
    )
    SELECT 
        date,
        minute,
        total_cpm
    FROM daily_minute_cpm
    WHERE (date, total_cpm) IN (
        SELECT 
            date,
            max(total_cpm)
        FROM daily_minute_cpm
        GROUP BY date
    )
    ORDER BY date
    '''
    
    result = client.query(query)
    
    # 转换为DataFrame
    df = pd.DataFrame(result.result_rows, columns=[
        'date', 'minute', 'total_cpm'
    ])
    
    return df

def plot_daily_max_cpm(df):
    # 设置中文字体
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
    
    # 创建图形
    plt.figure(figsize=(15, 8))
    
    # 创建柱状图
    plt.bar(df['date'], df['total_cpm'])
    
    # 设置标题和标签
    plt.title('每日最大请求量统计', fontsize=14, pad=20)
    plt.xlabel('日期', fontsize=12)
    plt.ylabel('所有接口每分钟请求总量', fontsize=12)
    
    # 调整x轴标签角度
    plt.xticks(rotation=45)
    
    # 在柱子上添加具体数值
    for i, v in enumerate(df['total_cpm']):
        plt.text(df['date'].iloc[i], v, f'{v:.0f}', 
                ha='center', va='bottom')
    
    # 调整布局
    plt.tight_layout()
    
    # 保存图片
    plt.savefig('daily_max_cpm.png', dpi=300, bbox_inches='tight')
    plt.close()

def main():
    try:
        print("开始统计每日最大请求量...")
        
        # 获取统计数据
        df = get_daily_max_cpm()
        
        # 打印统计结果
        print("\n每日最大请求量统计结果：")
        print("=" * 80)
        print(f"{'日期':<12} {'时间':<20} {'最大请求总量':<10}")
        print("-" * 80)
        
        for _, row in df.iterrows():
            print(f"{row['date'].strftime('%Y-%m-%d'):<12} {row['minute'].strftime('%H:%M:%S'):<20} {row['total_cpm']:<10.2f}")
        
        print("=" * 80)
        
        # 生成可视化图表
        print("\n正在生成统计图表...")
        plot_daily_max_cpm(df)
        print("统计图表已保存为 daily_max_cpm.png")
        
    except Exception as e:
        print(f"统计过程中发生错误: {str(e)}")

if __name__ == "__main__":
    main() 