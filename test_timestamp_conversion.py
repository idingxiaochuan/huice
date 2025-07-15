#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试时间戳转换问题
"""

import pandas as pd
import numpy as np
from datetime import datetime

# 测试数据 - 使用示例中的时间戳
time_stamps = [1716998400000, 1717084800000, 1717344000000, 1717430400000, 1717516800000]

print("=== 时间戳转换测试 ===")
print("原始毫秒时间戳:", time_stamps[0])

# 测试不同的转换方法
print("\n方法1: 直接使用pd.to_datetime(timestamp)，不指定单位")
try:
    date1 = pd.to_datetime(time_stamps[0])
    print(f"结果: {date1} (年份: {date1.year})")
except Exception as e:
    print(f"错误: {str(e)}")

print("\n方法2: 使用pd.to_datetime(timestamp, unit='ms')，指定单位为毫秒")
try:
    date2 = pd.to_datetime(time_stamps[0], unit='ms')
    print(f"结果: {date2} (年份: {date2.year})")
except Exception as e:
    print(f"错误: {str(e)}")

print("\n方法3: 先将毫秒转换为秒，再使用pd.to_datetime(timestamp/1000, unit='s')")
try:
    date3 = pd.to_datetime(time_stamps[0] / 1000, unit='s')
    print(f"结果: {date3} (年份: {date3.year})")
except Exception as e:
    print(f"错误: {str(e)}")

print("\n方法4: 使用datetime.fromtimestamp()")
try:
    date4 = datetime.fromtimestamp(time_stamps[0] / 1000)
    print(f"结果: {date4} (年份: {date4.year})")
except Exception as e:
    print(f"错误: {str(e)}")

# 测试所有时间戳
print("\n=== 测试所有时间戳 ===")
print("使用正确的方法: pd.to_datetime(timestamp / 1000, unit='s')")

for ts in time_stamps:
    correct_date = pd.to_datetime(ts / 1000, unit='s')
    print(f"时间戳 {ts} -> {correct_date}")

# 测试DataFrame转换
print("\n=== 测试DataFrame转换 ===")
df = pd.DataFrame({'time': time_stamps})
df['date'] = pd.to_datetime(df['time'] / 1000, unit='s')
print(df) 