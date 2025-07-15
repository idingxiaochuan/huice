#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
验证时间戳修复
"""

import pandas as pd
import psycopg2
import traceback

def connect_to_db():
    """连接到数据库"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="huice",
            user="postgres",
            password="postgres"
        )
        print("数据库连接成功")
        return conn
    except Exception as e:
        print(f"数据库连接失败: {str(e)}")
        traceback.print_exc()
        return None

def test_timestamp_conversion():
    """测试时间戳转换"""
    print("=== 测试时间戳转换 ===")
    
    # 创建测试数据
    timestamps = [1716998400000, 1717084800000, 1717344000000]  # 毫秒时间戳
    
    print("方法1: 直接使用pd.to_datetime(timestamp)，不指定单位")
    for ts in timestamps:
        date1 = pd.to_datetime(ts)
        print(f"时间戳 {ts} -> {date1} (年份: {date1.year})")
    
    print("\n方法2: 使用pd.to_datetime(timestamp, unit='ms')，指定单位为毫秒")
    for ts in timestamps:
        date2 = pd.to_datetime(ts, unit='ms')
        print(f"时间戳 {ts} -> {date2} (年份: {date2.year})")
    
    print("\n方法3: 先将毫秒转换为秒，再使用pd.to_datetime(timestamp/1000, unit='s')")
    for ts in timestamps:
        date3 = pd.to_datetime(ts / 1000, unit='s')
        print(f"时间戳 {ts} -> {date3} (年份: {date3.year})")

def test_data_save():
    """测试数据保存"""
    print("\n=== 测试数据保存 ===")
    
    # 创建测试数据
    data = {
        'time': [1716998400000, 1717084800000, 1717344000000],  # 毫秒时间戳
        'open': [0.619, 0.613, 0.612],
        'high': [0.619, 0.617, 0.615],
        'low': [0.610, 0.611, 0.606],
        'close': [0.613, 0.613, 0.610],
        'volume': [1000, 2000, 3000],
        'amount': [613, 1226, 1830],
        'symbol': ['515170.SH', '515170.SH', '515170.SH'],
        'freq': ['day', 'day', 'day']
    }
    
    df = pd.DataFrame(data)
    
    # 转换时间戳为日期
    df['date'] = pd.to_datetime(df['time'] / 1000, unit='s')
    
    print("测试数据:")
    print(df[['time', 'date', 'symbol', 'freq']].head())
    
    # 保存到数据库
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # 创建测试表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS verify_timestamps (
            id SERIAL PRIMARY KEY,
            fund_code VARCHAR(20) NOT NULL,
            data_level VARCHAR(10) NOT NULL,
            date TIMESTAMP NOT NULL,
            time BIGINT,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume FLOAT,
            amount FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # 清除可能存在的测试数据
        cursor.execute("DELETE FROM verify_timestamps")
        conn.commit()
        
        # 插入数据
        rows_inserted = 0
        for _, row in df.iterrows():
            # 构建INSERT语句
            insert_sql = """
            INSERT INTO verify_timestamps 
            (fund_code, data_level, date, time, open, high, low, close, volume, amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # 准备数据
            fund_code_val = row['symbol'].split('.')[0]
            data_level_val = row['freq']
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
        
        # 提交事务
        conn.commit()
        print(f"成功插入 {rows_inserted} 条记录")
        
        # 查询插入的数据
        cursor.execute("""
        SELECT id, fund_code, data_level, date, time 
        FROM verify_timestamps 
        ORDER BY id
        """)
        
        data = cursor.fetchall()
        print("\n查询结果:")
        for row in data:
            id_val, fund_code, data_level, date, time_val = row
            print(f"ID: {id_val}, 基金: {fund_code}, 级别: {data_level}, 日期: {date}, 时间戳: {time_val}")
            
            # 验证日期是否正确
            correct_date = pd.to_datetime(time_val / 1000, unit='s')
            print(f"  - 从时间戳计算的日期: {correct_date}")
            print(f"  - 日期是否匹配: {date.strftime('%Y-%m-%d %H:%M:%S') == correct_date.strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"保存数据到数据库失败: {str(e)}")
        traceback.print_exc()
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def main():
    """主函数"""
    test_timestamp_conversion()
    test_data_save()

if __name__ == "__main__":
    main()
