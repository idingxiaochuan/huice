#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
时间工具模块，提供统一的时间戳转换函数
"""

import pandas as pd
import datetime
import pytz

def convert_timestamp_to_datetime(timestamp):
    """
    将时间戳转换为datetime对象，自动检测毫秒级时间戳
    
    Args:
        timestamp: 时间戳，可能是秒级或毫秒级
        
    Returns:
        datetime: 转换后的datetime对象
    """
    try:
        # 检测是否为毫秒级时间戳（大于10000000000）
        if timestamp > 10000000000:
            # 毫秒级时间戳，转换为秒级
            dt = pd.to_datetime(timestamp / 1000, unit='s')
        else:
            # 秒级时间戳
            dt = pd.to_datetime(timestamp, unit='s')
        
        # 将UTC时间转换为中国时区（东八区，UTC+8）
        china_tz = pytz.timezone('Asia/Shanghai')
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=pytz.UTC)
        dt = dt.astimezone(china_tz)
        
        return dt
    except Exception as e:
        print(f"时间戳转换错误: {e}, 时间戳值: {timestamp}")
        return None

def convert_datetime_to_timestamp(dt):
    """
    将datetime对象转换为毫秒级时间戳
    
    Args:
        dt: datetime对象
        
    Returns:
        int: 毫秒级时间戳
    """
    if isinstance(dt, datetime.datetime):
        # 转换为毫秒级时间戳
        return int(dt.timestamp() * 1000)
    elif isinstance(dt, str):
        # 如果是字符串，先转换为datetime
        try:
            dt_obj = pd.to_datetime(dt)
            return int(dt_obj.timestamp() * 1000)
        except:
            return None
    else:
        return None

def format_datetime(dt, format_str='%Y-%m-%d %H:%M:%S'):
    """
    格式化datetime对象为字符串
    
    Args:
        dt: datetime对象
        format_str: 格式化字符串
        
    Returns:
        str: 格式化后的字符串
    """
    if dt is None:
        return None
    try:
        return dt.strftime(format_str)
    except Exception as e:
        print(f"日期格式化错误: {e}, 日期值: {dt}")
        return None

def is_valid_date(dt):
    """
    检查日期是否有效（不是1970年附近）
    
    Args:
        dt: datetime对象或时间戳
        
    Returns:
        bool: 是否有效
    """
    if isinstance(dt, (int, float)):
        # 如果是时间戳，先转换为datetime
        dt = convert_timestamp_to_datetime(dt)
    
    if isinstance(dt, datetime.datetime):
        # 检查年份是否在合理范围内
        return dt.year > 2000 and dt.year < 2100
    else:
        return False

def get_current_timestamp():
    """
    获取当前时间的毫秒级时间戳
    
    Returns:
        int: 毫秒级时间戳
    """
    return int(datetime.datetime.now().timestamp() * 1000)

def get_date_range(start_date, end_date=None, fmt='%Y%m%d'):
    """
    获取日期范围
    
    Args:
        start_date: 开始日期，字符串或datetime对象
        end_date: 结束日期，字符串或datetime对象，默认为当前日期
        fmt: 日期格式，默认为'%Y%m%d'
        
    Returns:
        list: 日期字符串列表
    """
    if isinstance(start_date, str):
        start_date = datetime.datetime.strptime(start_date, fmt)
    
    if end_date is None:
        end_date = datetime.datetime.now()
    elif isinstance(end_date, str):
        end_date = datetime.datetime.strptime(end_date, fmt)
    
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime(fmt))
        current_date += datetime.timedelta(days=1)
    
    return date_list

# 测试函数
def test_time_utils():
    """测试时间工具函数"""
    print("=== 测试时间工具函数 ===")
    
    # 测试时间戳转换
    timestamp = 1716998400000  # 2024-05-29 16:00:00
    dt = convert_timestamp_to_datetime(timestamp)
    print(f"时间戳 {timestamp} -> {dt}")
    
    # 测试datetime转时间戳
    timestamp2 = convert_datetime_to_timestamp(dt)
    print(f"datetime {dt} -> {timestamp2}")
    
    # 测试格式化
    formatted = format_datetime(dt)
    print(f"格式化: {formatted}")
    
    # 测试日期有效性检查
    valid = is_valid_date(dt)
    print(f"日期有效性: {valid}")
    
    # 测试获取当前时间戳
    current_ts = get_current_timestamp()
    current_dt = convert_timestamp_to_datetime(current_ts)
    print(f"当前时间戳: {current_ts} -> {current_dt}")
    
    # 测试获取日期范围
    date_range = get_date_range("20240501", "20240510")
    print(f"日期范围: {date_range}")

if __name__ == "__main__":
    test_time_utils() 