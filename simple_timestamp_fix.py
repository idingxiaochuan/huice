#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
简单的时间戳转换和数据库保存测试
"""

import pandas as pd
import psycopg2
import traceback
from datetime import datetime

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

def create_test_data():
    """创建测试数据"""
    # 创建毫秒级时间戳，模拟QMT返回的数据
    timestamps = [1716998400000, 1717084800000, 1717344000000]
    
    data = {
        'time': timestamps,
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
    print("测试数据创建完成")
    return df

def test_timestamp_conversion(df):
    """测试时间戳转换"""
    print("\n=== 测试时间戳转换 ===")
    
    # 测试不同的转换方法
    print("方法1: 直接使用pd.to_datetime(timestamp)，不指定单位")
    date1 = pd.to_datetime(df['time'].iloc[0])
    print(f"结果: {date1} (年份: {date1.year})")
    
    print("\n方法2: 使用pd.to_datetime(timestamp, unit='ms')，指定单位为毫秒")
    date2 = pd.to_datetime(df['time'].iloc[0], unit='ms')
    print(f"结果: {date2} (年份: {date2.year})")
    
    print("\n方法3: 先将毫秒转换为秒，再使用pd.to_datetime(timestamp/1000, unit='s')")
    date3 = pd.to_datetime(df['time'].iloc[0] / 1000, unit='s')
    print(f"结果: {date3} (年份: {date3.year})")
    
    # 使用正确的方法添加日期列
    print("\n添加正确的日期列")
    df['date'] = pd.to_datetime(df['time'] / 1000, unit='s')
    print(df[['time', 'date', 'symbol', 'freq']].head())
    
    return df

def save_to_db(df):
    """保存数据到数据库"""
    print("\n=== 保存数据到数据库 ===")
    
    conn = connect_to_db()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # 创建测试表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS simple_test (
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
        cursor.execute("DELETE FROM simple_test")
        conn.commit()
        
        # 插入数据
        rows_inserted = 0
        for _, row in df.iterrows():
            # 构建INSERT语句
            insert_sql = """
            INSERT INTO simple_test 
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
        FROM simple_test 
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
        
        return True
    except Exception as e:
        print(f"保存数据到数据库失败: {str(e)}")
        traceback.print_exc()
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def main():
    """主函数"""
    # 创建测试数据
    df = create_test_data()
    
    # 测试时间戳转换
    df = test_timestamp_conversion(df)
    
    # 保存到数据库
    save_to_db(df)
    
    print("\n=== 结论 ===")
    print("1. 问题根本原因:")
    print("   - QMT系统返回的是毫秒级时间戳(如1716998400000)")
    print("   - 代码中使用pd.to_datetime(timestamp)没有指定时间单位")
    print("   - Pandas默认将时间戳解释为纳秒，导致日期接近1970年")
    print("\n2. 正确的转换方法:")
    print("   - 将毫秒时间戳除以1000转换为秒级")
    print("   - 使用pd.to_datetime(timestamp / 1000, unit='s')")
    print("   - 或者直接使用pd.to_datetime(timestamp, unit='ms')")
    print("\n3. 修复方法:")
    print("   - 在所有处理时间戳的地方统一使用正确的转换方法")
    print("   - 确保在保存到数据库前正确转换时间戳")

if __name__ == "__main__":
    main() 