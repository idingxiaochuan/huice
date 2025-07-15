#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检查数据库表结构和数据
"""

from backtest_gui.db.database import Database
import pandas as pd

def check_table_structure():
    """检查表结构"""
    db = Database()
    if not db.connect():
        print("数据库连接失败")
        return
    
    try:
        # 检查stock_quotes表结构
        query = """
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'stock_quotes' 
        ORDER BY ordinal_position
        """
        columns = db.execute_query(query)
        
        print("=== stock_quotes表结构 ===")
        for col in columns:
            print(f"{col[0]}: {col[1]}")
            
        # 检查表中的数据
        query = """
        SELECT id, fund_code, data_level, date, time 
        FROM stock_quotes 
        ORDER BY id 
        LIMIT 5
        """
        data = db.execute_query(query)
        
        print("\n=== stock_quotes表数据示例 ===")
        for row in data:
            print(f"ID: {row[0]}, 基金: {row[1]}, 级别: {row[2]}, 日期: {row[3]}, 时间戳: {row[4]}")
            
        # 检查日期为1970年的记录数量
        query = """
        SELECT COUNT(*) 
        FROM stock_quotes 
        WHERE date < '2000-01-01'
        """
        count = db.execute_query(query)[0][0]
        print(f"\n1970年的记录数量: {count}")
        
        # 检查日期正常的记录数量
        query = """
        SELECT COUNT(*) 
        FROM stock_quotes 
        WHERE date >= '2000-01-01'
        """
        count = db.execute_query(query)[0][0]
        print(f"2000年以后的记录数量: {count}")
        
        # 查看最新插入的记录
        query = """
        SELECT id, fund_code, data_level, date, time 
        FROM stock_quotes 
        ORDER BY id DESC 
        LIMIT 5
        """
        data = db.execute_query(query)
        
        print("\n=== 最新插入的记录 ===")
        for row in data:
            print(f"ID: {row[0]}, 基金: {row[1]}, 级别: {row[2]}, 日期: {row[3]}, 时间戳: {row[4]}")
            
    except Exception as e:
        print(f"查询失败: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_table_structure() 