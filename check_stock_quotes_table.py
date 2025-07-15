#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检查stock_quotes表的结构和数据，并尝试修复日期问题
"""

import psycopg2
import pandas as pd
from datetime import datetime
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

def check_table_structure():
    """检查表结构"""
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # 获取表结构
        cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'stock_quotes' 
        ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print("=== stock_quotes表结构 ===")
        for col in columns:
            print(f"{col[0]}: {col[1]}")
        
        # 查看数据示例
        cursor.execute("""
        SELECT * FROM stock_quotes 
        ORDER BY id 
        LIMIT 5
        """)
        
        data = cursor.fetchall()
        print("\n=== 数据示例 ===")
        for row in data:
            print(row)
    except Exception as e:
        print(f"查询失败: {str(e)}")
        traceback.print_exc()
    finally:
        conn.close()

def fix_dates():
    """修复日期问题"""
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # 获取所有1970年的记录
        cursor.execute("""
        SELECT id, fund_code, data_level, date, time 
        FROM stock_quotes 
        WHERE date < '2000-01-01' 
        ORDER BY id
        """)
        
        records = cursor.fetchall()
        print(f"查询到 {len(records)} 条记录")
        
        # 修复日期
        fixed_count = 0
        for record in records:
            id_val, fund_code, data_level, date, time_val = record
            
            # 使用正确的方法计算日期
            correct_date = pd.to_datetime(time_val / 1000, unit='s')
            
            # 更新记录
            cursor.execute("""
            UPDATE stock_quotes 
            SET date = %s 
            WHERE id = %s
            """, (correct_date, id_val))
            
            fixed_count += 1
            
            # 每100条记录提交一次事务
            if fixed_count % 100 == 0:
                conn.commit()
                print(f"已修复 {fixed_count} 条记录")
        
        # 提交剩余的事务
        conn.commit()
        print(f"共修复 {fixed_count} 条记录")
        
        # 检查是否还有1970年的记录
        cursor.execute("""
        SELECT COUNT(*) 
        FROM stock_quotes 
        WHERE date < '2000-01-01'
        """)
        
        count = cursor.fetchone()[0]
        if count == 0:
            print("所有日期都已修复")
        else:
            print(f"仍有 {count} 条记录的日期是1970年")
            
    except Exception as e:
        print(f"修复失败: {str(e)}")
        traceback.print_exc()
        conn.rollback()
    finally:
        conn.close()

def check_insert_process():
    """检查插入过程"""
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # 创建测试数据
        data = {
            'time': [1716998400000, 1717084800000, 1717344000000],
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
        
        print("=== 测试数据 ===")
        print(df[['time', 'date', 'symbol', 'freq', 'open', 'close']].head())
        
        # 插入测试数据
        fund_code = '515170'
        data_level = 'day'
        
        for _, row in df.iterrows():
            # 构建INSERT语句
            insert_sql = """
            INSERT INTO stock_quotes_test 
            (fund_code, data_level, date, time, open, high, low, close, volume, amount, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """
            
            # 准备数据
            fund_code_val = fund_code
            data_level_val = data_level
            date_val = row['date']
            time_val = row['time']
            open_val = float(row['open'])
            high_val = float(row['high'])
            low_val = float(row['low'])
            close_val = float(row['close'])
            volume_val = float(row['volume'])
            amount_val = float(row['amount'])
            
            # 创建测试表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_quotes_test (
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
            
            # 执行插入
            cursor.execute(insert_sql, (
                fund_code_val, data_level_val, date_val, time_val, open_val, high_val, 
                low_val, close_val, volume_val, amount_val
            ))
        
        # 提交事务
        conn.commit()
        
        # 查询插入的数据
        cursor.execute("""
        SELECT id, fund_code, data_level, date, time 
        FROM stock_quotes_test 
        ORDER BY id
        """)
        
        data = cursor.fetchall()
        print("\n=== 插入的数据 ===")
        for row in data:
            id_val, fund_code, data_level, date, time_val = row
            print(f"ID: {id_val}, 基金: {fund_code}, 级别: {data_level}, 日期: {date}, 时间戳: {time_val}")
            
    except Exception as e:
        print(f"测试失败: {str(e)}")
        traceback.print_exc()
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    # 检查表结构
    check_table_structure()
    
    # 修复日期
    fix_dates()
    
    # 可选：检查插入过程
    # check_insert_process() 