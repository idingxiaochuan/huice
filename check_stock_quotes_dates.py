#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检查stock_quotes表中的日期是否正确
"""

import psycopg2
from datetime import datetime
import pandas as pd

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
        return None

def check_dates():
    """检查日期"""
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # 查询所有记录
        cursor.execute("SELECT id, time, date, fund_code, data_level FROM stock_quotes LIMIT 10")
        rows = cursor.fetchall()
        print(f"查询到 {len(rows)} 条记录")
        
        # 检查日期
        incorrect_count = 0
        for row in rows:
            id_val, time_val, date_val, fund_code, data_level = row
            
            # 检查日期是否是1970年
            if date_val.year == 1970:
                incorrect_count += 1
                print(f"记录 {id_val} (基金: {fund_code}, 级别: {data_level}) 的日期不正确: {date_val}")
                
                # 计算正确的日期
                correct_date = datetime.fromtimestamp(time_val / 1000)
                print(f"  - 时间戳: {time_val}")
                print(f"  - 正确日期应为: {correct_date}")
                print()
            else:
                print(f"记录 {id_val} (基金: {fund_code}, 级别: {data_level}) 的日期正确: {date_val}")
                print(f"  - 时间戳: {time_val}")
                print()
        
        # 统计1970年的记录数
        cursor.execute("SELECT COUNT(*) FROM stock_quotes WHERE date < '2000-01-01'")
        count = cursor.fetchone()[0]
        
        if count == 0:
            print("所有日期都是正确的，没有1970年的记录")
        else:
            print(f"仍有 {count} 条记录的日期是1970年")
            
    except Exception as e:
        print(f"检查日期失败: {str(e)}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    check_dates() 