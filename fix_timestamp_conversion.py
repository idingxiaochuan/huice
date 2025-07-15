#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
修复时间戳转换问题

这个脚本分析了时间戳转换问题的根本原因，并提供了解决方案。
问题在于QMT系统返回的是毫秒级时间戳，但在某些地方没有正确转换为日期。

主要修复点:
1. 在backtest_gui/fund_data_fetcher.py中的_save_market_data_to_db方法
2. 在backtest_gui/utils/db_connector.py中的save_to_database方法
3. 在backtest_gui/minute_data_fetcher.py中的时间戳转换代码
"""

import os
import pandas as pd
import traceback
import sys

def test_timestamp_conversion():
    """测试不同的时间戳转换方法"""
    print("=== 测试时间戳转换 ===")
    
    # 创建测试数据
    timestamps = [1716998400000, 1717084800000, 1717344000000]  # 毫秒时间戳
    
    print("方法1: 直接使用pd.to_datetime(timestamp)，不指定单位")
    for ts in timestamps:
        date1 = pd.to_datetime(ts)
        print(f"时间戳 {ts} -> {date1} (年份: {date1.year})")
    
    print("\n方法2: 使用pd.to_datetime(timestamp, unit='ms')，指定单位为毫秒")
    for ts in timestamps:
        date2 = pd.to_datetime(ts, unit='ms')
        print(f"时间戳 {ts} -> {date2} (年份: {date2.year})")
    
    print("\n方法3: 先将毫秒转换为秒，再使用pd.to_datetime(timestamp/1000, unit='s')")
    for ts in timestamps:
        date3 = pd.to_datetime(ts / 1000, unit='s')
        print(f"时间戳 {ts} -> {date3} (年份: {date3.year})")
    
    print("\n结论: 方法2和方法3都能正确处理毫秒时间戳，但方法1会错误地将时间戳解释为纳秒，导致日期接近1970年")

def fix_fund_data_fetcher():
    """修复fund_data_fetcher.py中的问题"""
    file_path = "backtest_gui/fund_data_fetcher.py"
    
    if not os.path.exists(file_path):
        print(f"错误: 文件 {file_path} 不存在")
        return False
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 检查是否有需要修复的代码
        if "df['date'] = pd.to_datetime(df['time'])" in content:
            # 修复代码
            content = content.replace(
                "df['date'] = pd.to_datetime(df['time'])",
                "df['date'] = pd.to_datetime(df['time'] / 1000, unit='s')"
            )
            print(f"修复了 {file_path} 中的时间戳转换代码")
            
            # 保存修改后的文件
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            return True
        else:
            print(f"{file_path} 中没有发现需要修复的代码")
            return False
    
    except Exception as e:
        print(f"修复 {file_path} 时出错: {str(e)}")
        traceback.print_exc()
        return False

def fix_db_connector():
    """修复db_connector.py中的问题"""
    file_path = "backtest_gui/utils/db_connector.py"
    
    if not os.path.exists(file_path):
        print(f"错误: 文件 {file_path} 不存在")
        return False
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 检查save_to_database函数中是否需要修复
        if "row.get('date')" in content and "# 确保日期正确转换" not in content:
            # 在save_to_database函数中添加日期转换代码
            modified_content = content.replace(
                "rows = []",
                """rows = []
        
        # 确保日期正确转换
        if 'time' in df.columns and 'date' not in df.columns:
            # 检查time列的类型
            if df['time'].dtype == 'int64' or df['time'].dtype == 'float64':
                # 时间戳格式转换为datetime，注意QMT返回的时间戳是毫秒级的
                df['date'] = pd.to_datetime(df['time'] / 1000, unit='s')
                print(f"转换时间戳示例: {df['time'].iloc[0]} -> {df['date'].iloc[0]}")"""
            )
            
            print(f"修复了 {file_path} 中的save_to_database函数")
            
            # 保存修改后的文件
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(modified_content)
            
            return True
        else:
            print(f"{file_path} 中没有发现需要修复的代码或已经修复")
            return False
    
    except Exception as e:
        print(f"修复 {file_path} 时出错: {str(e)}")
        traceback.print_exc()
        return False

def fix_minute_data_fetcher():
    """修复minute_data_fetcher.py中的问题"""
    file_path = "backtest_gui/minute_data_fetcher.py"
    
    if not os.path.exists(file_path):
        print(f"错误: 文件 {file_path} 不存在")
        return False
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 检查是否有需要修复的代码
        if "df['date'] = pd.to_datetime(df['time'])" in content:
            # 修复代码
            content = content.replace(
                "df['date'] = pd.to_datetime(df['time'])",
                "df['date'] = pd.to_datetime(df['time'] / 1000, unit='s')"
            )
            print(f"修复了 {file_path} 中的时间戳转换代码")
            
            # 保存修改后的文件
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            return True
        else:
            print(f"{file_path} 中没有发现需要修复的代码")
            return False
    
    except Exception as e:
        print(f"修复 {file_path} 时出错: {str(e)}")
        traceback.print_exc()
        return False

def create_test_script():
    """创建测试脚本验证修复"""
    file_path = "verify_timestamp_fix.py"
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write("""#!/usr/bin/env python
# -*- coding: utf-8 -*-
\"\"\"
验证时间戳修复
\"\"\"

import pandas as pd
import psycopg2
import traceback

def connect_to_db():
    \"\"\"连接到数据库\"\"\"
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

def test_timestamp_conversion():
    \"\"\"测试时间戳转换\"\"\"
    print("=== 测试时间戳转换 ===")
    
    # 创建测试数据
    timestamps = [1716998400000, 1717084800000, 1717344000000]  # 毫秒时间戳
    
    print("方法1: 直接使用pd.to_datetime(timestamp)，不指定单位")
    for ts in timestamps:
        date1 = pd.to_datetime(ts)
        print(f"时间戳 {ts} -> {date1} (年份: {date1.year})")
    
    print("\\n方法2: 使用pd.to_datetime(timestamp, unit='ms')，指定单位为毫秒")
    for ts in timestamps:
        date2 = pd.to_datetime(ts, unit='ms')
        print(f"时间戳 {ts} -> {date2} (年份: {date2.year})")
    
    print("\\n方法3: 先将毫秒转换为秒，再使用pd.to_datetime(timestamp/1000, unit='s')")
    for ts in timestamps:
        date3 = pd.to_datetime(ts / 1000, unit='s')
        print(f"时间戳 {ts} -> {date3} (年份: {date3.year})")

def test_data_save():
    \"\"\"测试数据保存\"\"\"
    print("\\n=== 测试数据保存 ===")
    
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
    
    # 转换时间戳为日期
    df['date'] = pd.to_datetime(df['time'] / 1000, unit='s')
    
    print("测试数据:")
    print(df[['time', 'date', 'symbol', 'freq']].head())
    
    # 保存到数据库
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # 创建测试表
        cursor.execute(\"\"\"
        CREATE TABLE IF NOT EXISTS verify_timestamps (
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
        \"\"\")
        
        # 清除可能存在的测试数据
        cursor.execute("DELETE FROM verify_timestamps")
        conn.commit()
        
        # 插入数据
        rows_inserted = 0
        for _, row in df.iterrows():
            # 构建INSERT语句
            insert_sql = \"\"\"
            INSERT INTO verify_timestamps 
            (fund_code, data_level, date, time, open, high, low, close, volume, amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            \"\"\"
            
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
        cursor.execute(\"\"\"
        SELECT id, fund_code, data_level, date, time 
        FROM verify_timestamps 
        ORDER BY id
        \"\"\")
        
        data = cursor.fetchall()
        print("\\n查询结果:")
        for row in data:
            id_val, fund_code, data_level, date, time_val = row
            print(f"ID: {id_val}, 基金: {fund_code}, 级别: {data_level}, 日期: {date}, 时间戳: {time_val}")
            
            # 验证日期是否正确
            correct_date = pd.to_datetime(time_val / 1000, unit='s')
            print(f"  - 从时间戳计算的日期: {correct_date}")
            print(f"  - 日期是否匹配: {date.strftime('%Y-%m-%d %H:%M:%S') == correct_date.strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"保存数据到数据库失败: {str(e)}")
        traceback.print_exc()
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def main():
    \"\"\"主函数\"\"\"
    test_timestamp_conversion()
    test_data_save()

if __name__ == "__main__":
    main()
""")
        
        print(f"创建了测试脚本 {file_path}")
        return True
    
    except Exception as e:
        print(f"创建测试脚本时出错: {str(e)}")
        traceback.print_exc()
        return False

def main():
    """主函数"""
    print("=== 时间戳转换问题修复工具 ===\n")
    
    # 测试时间戳转换
    test_timestamp_conversion()
    
    print("\n=== 开始修复代码 ===")
    
    # 修复fund_data_fetcher.py
    fix_fund_data_fetcher()
    
    # 修复db_connector.py
    fix_db_connector()
    
    # 修复minute_data_fetcher.py
    fix_minute_data_fetcher()
    
    # 创建测试脚本
    create_test_script()
    
    print("\n=== 修复总结 ===")
    print("1. 问题根本原因:")
    print("   - QMT系统返回的是毫秒级时间戳(如1716998400000)")
    print("   - 代码中使用pd.to_datetime(timestamp)没有指定时间单位")
    print("   - Pandas默认将时间戳解释为纳秒，导致日期接近1970年")
    print("\n2. 修复方法:")
    print("   - 将毫秒时间戳除以1000转换为秒级")
    print("   - 使用正确的转换方法: pd.to_datetime(timestamp / 1000, unit='s')")
    print("   - 修改了所有保存数据到数据库前的时间戳转换代码")
    print("\n3. 验证方法:")
    print("   - 运行创建的verify_timestamp_fix.py脚本")
    print("   - 检查数据库中的日期是否正确(应为2024年而非1970年)")
    print("\n修复完成！请运行 python verify_timestamp_fix.py 验证修复效果")

if __name__ == "__main__":
    main() 