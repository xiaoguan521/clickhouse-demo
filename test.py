from clickhouse_connect import get_client
from datetime import datetime

# 测试连接脚本
def test_clickhouse_connection():
    try:
        # 创建客户端连接
        client = get_client(
            host='localhost',
            port=8123,  # 使用 HTTP 接口
            username='default',
            password='yourpassword'
        )
        
        # 测试查询
        result = client.query('SHOW DATABASES')
        print("Connected successfully!")
        print("Available databases:", result.result_rows)
        
        # 创建测试表
        client.command('''
            CREATE TABLE IF NOT EXISTS test_table (
                timestamp DateTime,
                api_name String,
                cpm Float32
            ) ENGINE = MergeTree()
            ORDER BY timestamp
        ''')
        print("Test table created successfully!")
        
        # 插入测试数据
        test_data = [
            (datetime.now(), 'test_api', 2.5)
        ]
        client.insert('test_table', test_data, column_names=['timestamp', 'api_name', 'cpm'])
        print("Test data inserted successfully!")
        
        # 查询数据
        query_result = client.query('SELECT * FROM test_table ORDER BY timestamp DESC LIMIT 5')
        print("\nQuery Results:")
        for row in query_result.result_rows:
            print(f"Timestamp: {row[0]}, API: {row[1]}, CPM: {row[2]}")
            
    except Exception as e:
        print(f"Operation failed: {e}")

if __name__ == "__main__":
    test_clickhouse_connection()