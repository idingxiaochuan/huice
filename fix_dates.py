#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
修复数据库中的日期
将stock_quotes表中的日期从1970年修复为正确的日期
"""

import pandas as pd
import psycopg2
from datetime import datetime
import traceback
import sys

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

def fix_dates():
    """修复日期"""
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # 查询所有记录
        cursor.execute("SELECT id, time, date FROM stock_quotes")
        rows = cursor.fetchall()
        print(f"查询到 {len(rows)} 条记录")
        
        # 修复日期
        fixed_count = 0
        for row in rows:
            id_val, time_val, date_val = row
            
            # 检查日期是否需要修复
            if date_val.year == 1970:
                # 将毫秒时间戳转换为秒级，再转换为datetime
                correct_date = datetime.fromtimestamp(time_val / 1000)
                
                # 更新记录
                cursor.execute(
                    "UPDATE stock_quotes SET date = %s WHERE id = %s",
                    (correct_date, id_val)
                )
                fixed_count += 1
                
                # 每1000条记录提交一次事务
                if fixed_count % 1000 == 0:
                    conn.commit()
                    print(f"已修复 {fixed_count} 条记录")
        
        # 提交事务
        conn.commit()
        print(f"共修复 {fixed_count} 条记录")
        
        # 检查修复结果
        cursor.execute("SELECT COUNT(*) FROM stock_quotes WHERE date < '2000-01-01'")
        count = cursor.fetchone()[0]
        if count == 0:
            print("所有日期都已修复")
        else:
            print(f"仍有 {count} 条记录的日期未修复")
            
    except Exception as e:
        print(f"修复日期失败: {str(e)}")
        traceback.print_exc()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    fix_dates() 