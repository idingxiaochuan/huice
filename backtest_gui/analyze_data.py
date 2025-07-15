#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
分析股票数据，查找异常值
"""
import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

# 数据库连接参数
db_params = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'huice',
    'user': 'postgres',
    'password': 'postgres'
}

def connect_db():
    """连接到数据库"""
    try:
        conn = psycopg2.connect(**db_params)
        return conn
    except Exception as e:
        print(f"数据库连接错误: {str(e)}")
        return None

def load_data(fund_code='159920', data_level='1min', start_date='2012-10-22', end_date='2013-01-01'):
    """加载指定时间段的数据"""
    conn = connect_db()
    if not conn:
        return None
    
    try:
        # 创建查询
        query = """
        SELECT date, open, high, low, close, volume, amount
        FROM stock_quotes
        WHERE fund_code = %s AND data_level = %s 
        AND date BETWEEN %s AND %s
        ORDER BY date
        """
        
        # 执行查询
        df = pd.read_sql(query, conn, params=(fund_code, data_level, start_date, end_date))
        
        # 设置日期为索引
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        return df
    except Exception as e:
        print(f"数据加载错误: {str(e)}")
        return None
    finally:
        conn.close()

def analyze_data(df):
    """分析数据，查找异常值"""
    if df is None or len(df) == 0:
        print("没有数据可分析")
        return
    
    # 打印基本统计信息
    print("数据基本统计:")
    print(f"数据点数量: {len(df)}")
    print(f"收盘价范围: {df['close'].min():.4f} - {df['close'].max():.4f}")
    print(f"收盘价平均值: {df['close'].mean():.4f}")
    print(f"收盘价中位数: {df['close'].median():.4f}")
    print(f"收盘价标准差: {df['close'].std():.4f}")
    
    # 计算每日价格变化百分比
    df['price_change'] = df['close'].pct_change() * 100
    
    # 查找异常的价格变化
    threshold = 5.0  # 超过5%的价格变化视为异常
    anomalies = df[abs(df['price_change']) > threshold]
    
    if len(anomalies) > 0:
        print(f"\n发现 {len(anomalies)} 个异常价格变化 (超过 {threshold}%):")
        for date, row in anomalies.iterrows():
            print(f"日期: {date}, 收盘价: {row['close']:.4f}, 变化: {row['price_change']:.2f}%")
    else:
        print(f"\n没有发现异常价格变化 (超过 {threshold}%)")
    
    # 绘制价格图表
    plt.figure(figsize=(12, 6))
    plt.subplot(2, 1, 1)
    plt.plot(df.index, df['close'], 'b-')
    plt.title('收盘价走势')
    plt.grid(True)
    
    # 标记异常点
    if len(anomalies) > 0:
        plt.scatter(anomalies.index, anomalies['close'], color='red', s=50, zorder=5)
    
    # 绘制价格变化百分比
    plt.subplot(2, 1, 2)
    plt.plot(df.index, df['price_change'], 'g-')
    plt.title('价格变化百分比')
    plt.axhline(y=0, color='k', linestyle='-', alpha=0.3)
    plt.axhline(y=threshold, color='r', linestyle='--', alpha=0.5)
    plt.axhline(y=-threshold, color='r', linestyle='--', alpha=0.5)
    plt.grid(True)
    
    plt.tight_layout()
    plt.savefig('price_analysis.png')
    print("\n分析图表已保存为 'price_analysis.png'")
    
    # 检查是否有归一化迹象
    first_price = df['close'].iloc[0]
    normalized_like = df['close'] / first_price
    
    print("\n检查是否有归一化迹象:")
    print(f"首个价格: {first_price:.4f}")
    print(f"归一化后的价格范围: {normalized_like.min():.4f} - {normalized_like.max():.4f}")
    
    # 检查价格是否集中在1.0附近
    near_one = ((normalized_like > 0.9) & (normalized_like < 1.1)).mean() * 100
    print(f"价格在首个价格的±10%范围内的比例: {near_one:.2f}%")
    
    # 检查是否有大量相同价格
    value_counts = df['close'].value_counts()
    most_common = value_counts.iloc[0]
    most_common_pct = (most_common / len(df)) * 100
    most_common_value = value_counts.index[0]
    
    print(f"\n最常见价格: {most_common_value:.4f} (出现 {most_common} 次, 占比 {most_common_pct:.2f}%)")
    
    # 如果有大量相同价格，可能是数据问题
    if most_common_pct > 50:
        print("警告: 数据中有大量相同价格，可能是数据质量问题")

def main():
    # 加载数据
    print("正在加载数据...")
    df = load_data()
    
    if df is not None:
        # 分析数据
        analyze_data(df)
    
if __name__ == "__main__":
    main() 