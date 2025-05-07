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
    # 设置中文字体，使用系统已安装的字体
    try:
        # 首先尝试使用matplotlib字体管理器直接加载字体文件
        import os
        import matplotlib.font_manager as fm
        
        # 定义可能的字体文件路径 - 根据系统上实际安装的字体
        possible_font_paths = [
            # Noto Sans CJK 字体路径
            '/usr/share/fonts/google-noto-cjk/NotoSansCJK-Regular.ttc',
            '/usr/share/fonts/google-noto-cjk/NotoSansCJK-Medium.ttc',
            '/usr/share/fonts/google-noto-cjk/NotoSansCJK-Bold.ttc',
            
            # CESI 字体路径
            '/usr/share/fonts/cesi/CESI_HT_GB18030.TTF',
            '/usr/share/fonts/cesi/CESI_SS_GB18030.TTF',
            '/usr/share/fonts/cesi/CESI_KT_GB18030.TTF',
            
            # Droid Sans Fallback
            '/usr/share/fonts/google-droid-fonts/DroidSansFallback.ttf',
        ]
        
        # 尝试找到第一个存在的字体文件
        font_path = None
        for path in possible_font_paths:
            if os.path.exists(path):
                font_path = path
                break
                
        if font_path:
            # 如果找到字体文件，直接使用FontProperties
            prop = fm.FontProperties(fname=font_path)
            plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
            
            # 创建图形
            plt.figure(figsize=(15, 8))
            
            # 创建时间序列图
            plt.plot(df['timestamp'], df['total_cpm'], marker='o')
            
            # 设置标题和标签 - 使用字体属性
            plt.title('Top 10 时间点CPM统计', fontproperties=prop, fontsize=14, pad=20)
            plt.xlabel('时间', fontproperties=prop, fontsize=12)
            plt.ylabel('每分钟请求次数 (CPM)', fontproperties=prop, fontsize=12)
            
            # 调整x轴标签角度
            plt.xticks(rotation=45)
            
            print(f"成功使用字体文件: {font_path}")
        else:
            # 如果没有找到字体文件，回退到尝试按名称设置字体
            raise FileNotFoundError("未找到指定字体文件，将尝试使用系统字体名称")
            
    except Exception as e:
        # 如果上面的方法失败，尝试第二种方法：按字体名称设置
        print(f"使用字体文件失败: {e}，尝试按字体名称设置...")
        try:
            # 设置字体名称列表 - 基于系统已安装的字体
            plt.rcParams['font.sans-serif'] = [
                'Noto Sans CJK SC',  # 首选思源黑体简体中文
                'CESI黑体-GB18030',   # CESI黑体
                'CESI宋体-GB18030',   # CESI宋体
                'Droid Sans Fallback' # 备用
            ]
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
            
            print("成功使用字体名称设置字体")
            
        except Exception as e2:
            print(f"按字体名称设置也失败: {e2}")
            # 回退到最基本的设置
            plt.figure(figsize=(15, 8))
            plt.plot(df['timestamp'], df['total_cpm'], marker='o')
            plt.title('Top 10 时间点CPM统计（可能显示不正常）', fontsize=14, pad=20)
            plt.xlabel('时间', fontsize=12)
            plt.ylabel('每分钟请求次数 (CPM)', fontsize=12)
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