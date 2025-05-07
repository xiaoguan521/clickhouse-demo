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
            
            # 创建柱状图
            plt.bar(df['date'], df['total_cpm'])
            
            # 设置标题和标签 - 使用字体属性
            plt.title('每日最大请求量统计', fontproperties=prop, fontsize=14, pad=20)
            plt.xlabel('日期', fontproperties=prop, fontsize=12)
            plt.ylabel('所有接口每分钟请求总量', fontproperties=prop, fontsize=12)
            
            # 调整x轴标签角度
            plt.xticks(rotation=45)
            
            # 在柱子上添加具体数值
            for i, v in enumerate(df['total_cpm']):
                plt.text(df['date'].iloc[i], v, f'{v:.0f}', 
                        ha='center', va='bottom')
            
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
            
            print("成功使用字体名称设置字体")
            
        except Exception as e2:
            print(f"按字体名称设置也失败: {e2}")
            # 回退到最基本的设置
            plt.figure(figsize=(15, 8))
            plt.bar(df['date'], df['total_cpm'])
            plt.title('每日最大请求量统计（可能显示不正常）', fontsize=14, pad=20)
            plt.xlabel('日期', fontsize=12)
            plt.ylabel('所有接口每分钟请求总量', fontsize=12)
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