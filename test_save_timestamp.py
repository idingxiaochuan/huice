#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试保存时间戳到数据库
"""

import pandas as pd
import numpy as np
from datetime import datetime
from backtest_gui.db.database import Database
import traceback

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

print("=== 测试数据 ===")
print(df)

# 添加date列，使用正确的转换方法
df['date'] = pd.to_datetime(df['time'] / 1000, unit='s')
print("\n=== 添加date列后 ===")
print(df)

# 连接数据库
try:
    db = Database()
    if db.connect():
        print("\n数据库连接成功")
        
        # 清除可能存在的测试数据
        print("\n清除已存在的测试数据...")
        db.execute_query("DELETE FROM market_data WHERE symbol = '515170.SH'")
        
        # 保存数据到数据库
        print("\n保存数据到数据库...")
        result = db.save_market_data(df)
        
        if result:
            print("保存成功")
            
            # 查询保存的数据
            print("\n查询保存的数据...")
            query = "SELECT * FROM market_data WHERE symbol = '515170.SH' ORDER BY date"
            saved_data = db.execute_query(query)
            
            print(f"\n保存的数据 ({len(saved_data)} 条记录):")
            for row in saved_data:
                print(row)
                
            # 检查日期是否正确
            print("\n检查日期是否正确:")
            for i, row in enumerate(saved_data):
                db_date = row[2]  # date列
                expected_date = df['date'].iloc[i]
                print(f"记录 {i+1}:")
                print(f"  - 期望日期: {expected_date}")
                print(f"  - 数据库日期: {db_date}")
                print(f"  - 是否匹配: {db_date.year == expected_date.year and db_date.month == expected_date.month and db_date.day == expected_date.day}")
        else:
            print("保存失败")
    else:
        print("数据库连接失败")
except Exception as e:
    print(f"测试过程中发生错误: {str(e)}")
    traceback.print_exc() 