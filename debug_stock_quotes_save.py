from backtest_gui.db.database import Database
import pandas as pd
from datetime import datetime

# 创建一些测试数据
data = {
    'time': [1716998400000, 1717084800000, 1717344000000],
    'date': [pd.to_datetime('2024-05-30'), pd.to_datetime('2024-05-31'), pd.to_datetime('2024-06-03')],
    'open': [0.619, 0.613, 0.612],
    'high': [0.619, 0.617, 0.615],
    'low': [0.610, 0.611, 0.606],
    'close': [0.613, 0.613, 0.610],
    'volume': [1000, 2000, 3000],
    'amount': [613, 1226, 1830]
}

df = pd.DataFrame(data)

# 连接数据库
db = Database()
if db.connect():
    print("数据库连接成功")
    
    try:
        # 首先检查stock_quotes表是否存在，如果不存在则创建
        check_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'stock_quotes'
        )
        """
        result = db.execute_query(check_table_sql)
        table_exists = result[0][0] if result else False
        print(f"stock_quotes表是否存在: {table_exists}")
        
        if not table_exists:
            create_table_sql = """
            CREATE TABLE stock_quotes (
                id SERIAL PRIMARY KEY,
                fund_code VARCHAR(20) NOT NULL,
                data_level VARCHAR(10) NOT NULL,
                date TIMESTAMP,
                time BIGINT,
                open FLOAT NOT NULL,
                high FLOAT NOT NULL,
                low FLOAT NOT NULL,
                close FLOAT NOT NULL,
                volume FLOAT,
                amount FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            db.execute_query(create_table_sql)
            print("创建stock_quotes表成功")
        
        # 准备插入数据
        conn = db.get_connection()
        cursor = conn.cursor()
        
        try:
            # 插入每一行数据
            rows_inserted = 0
            for _, row in df.iterrows():
                # 构建INSERT语句
                insert_sql = """
                INSERT INTO stock_quotes 
                (fund_code, data_level, date, time, open, high, low, close, volume, amount, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """
                
                # 准备数据
                fund_code_val = '515170'
                data_level_val = 'day'
                date_val = row['date']
                time_val = row['time']
                open_val = float(row['open'])
                high_val = float(row['high'])
                low_val = float(row['low'])
                close_val = float(row['close'])
                volume_val = float(row['volume'])
                amount_val = float(row['amount'])
                
                # 执行插入
                cursor.execute(insert_sql, (
                    fund_code_val, data_level_val, date_val, time_val, open_val, high_val, 
                    low_val, close_val, volume_val, amount_val
                ))
                rows_inserted += 1
                print(f"插入第 {rows_inserted} 行数据")
            
            # 提交事务
            conn.commit()
            print(f"成功保存 {rows_inserted} 条记录到表 stock_quotes")
            
            # 查询保存的数据
            query = "SELECT * FROM stock_quotes"
            saved_data = db.execute_query(query)
            print(f"\n保存的数据 ({len(saved_data)} 条记录):")
            for row in saved_data:
                print(row)
                
        except Exception as e:
            conn.rollback()
            print(f"保存数据到数据库失败: {str(e)}")
            import traceback
            traceback.print_exc()
        finally:
            cursor.close()
            db.release_connection(conn)
            
    except Exception as e:
        print(f"操作数据库时发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
else:
    print("数据库连接失败") 