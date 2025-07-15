#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试时间戳转换修复
"""

import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入时间工具模块
from backtest_gui.utils.time_utils import convert_timestamp_to_datetime

def test_timestamp_conversion():
    """测试各种时间戳的转换"""
    print("=== 测试时间戳转换 ===")
    
    # 创建测试数据
    test_cases = [
        # 毫秒级时间戳 (QMT返回的格式)
        1716998400000,  # 2024-05-29 16:00:00
        
        # 秒级时间戳
        1716998400,     # 2024-05-29 16:00:00
        
        # 字符串形式的时间戳
        "1716998400000",
        
        # 字符串形式的日期
        "2024-05-29 16:00:00",
        
        # 无效数据
        None,
        np.nan,
        "invalid",
        
        # 边界情况
        0,              # 1970-01-01 00:00:00
        1000000000000,  # 2001-09-09 01:46:40
        9999999999999   # 2286-11-20 17:46:39
    ]
    
    # 测试每个时间戳
    for ts in test_cases:
        try:
            result = convert_timestamp_to_datetime(ts)
            print(f"时间戳 {ts} -> {result}")
        except Exception as e:
            print(f"时间戳 {ts} 转换失败: {str(e)}")
    
    # 测试DataFrame批量转换
    print("\n=== 测试DataFrame批量转换 ===")
    df = pd.DataFrame({
        'time': [1716998400000, 1717084800000, 1717171200000, 1717257600000, 1717344000000]
    })
    
    print("原始DataFrame:")
    print(df)
    
    # 逐行转换
    dates = []
    for i, ts in enumerate(df['time']):
        date = convert_timestamp_to_datetime(ts)
        dates.append(date)
    
    df['date'] = dates
    
    print("转换后DataFrame:")
    print(df)
    
    # 验证转换后的日期是否正确
    print("\n=== 验证转换结果 ===")
    for i, row in df.iterrows():
        expected = pd.to_datetime(row['time'] / 1000, unit='s')
        actual = row['date']
        is_correct = expected == actual
        print(f"行 {i}: 时间戳 {row['time']} -> {actual}, 预期 {expected}, 正确: {is_correct}")

def test_database_save():
    """测试数据库保存"""
    print("\n=== 测试数据库保存 ===")
    
    # 导入数据库模块
    from backtest_gui.db.database import Database
    
    # 创建测试数据
    df = pd.DataFrame({
        'symbol': ['TEST.SH'] * 5,
        'time': [1716998400000, 1717084800000, 1717171200000, 1717257600000, 1717344000000],
        'open': [100.0, 101.0, 102.0, 103.0, 104.0],
        'high': [105.0, 106.0, 107.0, 108.0, 109.0],
        'low': [95.0, 96.0, 97.0, 98.0, 99.0],
        'close': [103.0, 104.0, 105.0, 106.0, 107.0],
        'volume': [10000, 11000, 12000, 13000, 14000],
        'amount': [1000000.0, 1100000.0, 1200000.0, 1300000.0, 1400000.0],
        'freq': ['1min'] * 5
    })
    
    print("测试数据:")
    print(df)
    
    # 连接数据库
    db = Database()
    if not db.connect():
        print("连接数据库失败")
        return
    
    # 保存数据
    result = db.save_market_data(df)
    print(f"保存结果: {'成功' if result else '失败'}")
    
    # 查询保存的数据
    if result:
        query = "SELECT * FROM market_data WHERE symbol = 'TEST.SH' ORDER BY date"
        data = db.execute_query(query)
        if data:
            print(f"查询到 {len(data)} 条记录:")
            for row in data:
                print(f"ID: {row['id']}, 日期: {row['date']}, 收盘价: {row['close']}")
        else:
            print("未查询到数据")

if __name__ == "__main__":
    test_timestamp_conversion()
    test_database_save() 