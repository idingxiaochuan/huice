#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
调试时间戳转换和保存的过程
"""

import pandas as pd
import numpy as np
from datetime import datetime
import psycopg2
import traceback
import sys

# 设置调试日志
def setup_debug_logging():
    import logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger('debug_timestamp')

logger = setup_debug_logging()

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
        logger.info("数据库连接成功")
        return conn
    except Exception as e:
        logger.error(f"数据库连接失败: {str(e)}")
        traceback.print_exc()
        return None

def create_test_data():
    """创建测试数据"""
    # 创建毫秒级时间戳，模拟QMT返回的数据
    timestamps = [1716998400000, 1717084800000, 1717344000000, 1717430400000, 1717516800000]
    
    data = {
        'time': timestamps,
        'open': [0.619, 0.613, 0.612, 0.610, 0.620],
        'high': [0.619, 0.617, 0.615, 0.619, 0.620],
        'low': [0.610, 0.611, 0.606, 0.608, 0.610],
        'close': [0.613, 0.613, 0.610, 0.618, 0.612],
        'volume': [1000, 2000, 3000, 4000, 5000],
        'amount': [613, 1226, 1830, 2472, 3060],
        'symbol': ['515170.SH', '515170.SH', '515170.SH', '515170.SH', '515170.SH'],
        'freq': ['day', 'day', 'day', 'day', 'day']
    }
    
    df = pd.DataFrame(data)
    logger.info("测试数据创建完成")
    return df

def test_timestamp_conversion(df):
    """测试时间戳转换"""
    logger.info("=== 测试时间戳转换 ===")
    logger.info(f"原始数据类型: {df['time'].dtype}")
    logger.info(f"原始时间戳示例: {df['time'].iloc[0]}")
    
    # 测试不同的转换方法
    logger.info("\n方法1: 直接使用pd.to_datetime(timestamp)，不指定单位")
    try:
        date1 = pd.to_datetime(df['time'].iloc[0])
        logger.info(f"结果: {date1} (年份: {date1.year})")
    except Exception as e:
        logger.error(f"错误: {str(e)}")
    
    logger.info("\n方法2: 使用pd.to_datetime(timestamp, unit='ms')，指定单位为毫秒")
    try:
        date2 = pd.to_datetime(df['time'].iloc[0], unit='ms')
        logger.info(f"结果: {date2} (年份: {date2.year})")
    except Exception as e:
        logger.error(f"错误: {str(e)}")
    
    logger.info("\n方法3: 先将毫秒转换为秒，再使用pd.to_datetime(timestamp/1000, unit='s')")
    try:
        date3 = pd.to_datetime(df['time'].iloc[0] / 1000, unit='s')
        logger.info(f"结果: {date3} (年份: {date3.year})")
    except Exception as e:
        logger.error(f"错误: {str(e)}")
    
    # 将正确的方法应用到整个DataFrame
    logger.info("\n应用正确的方法到整个DataFrame")
    df['date'] = pd.to_datetime(df['time'] / 1000, unit='s')
    logger.info(f"转换后的数据:\n{df[['time', 'date', 'symbol', 'freq']].head()}")
    
    return df

def test_database_save(df):
    """测试保存到数据库"""
    logger.info("=== 测试保存到数据库 ===")
    
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # 创建测试表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS debug_timestamps (
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
        conn.commit()
        
        # 清除可能存在的测试数据
        cursor.execute("DELETE FROM debug_timestamps")
        conn.commit()
        
        # 检查日期列是否存在
        if 'date' not in df.columns:
            logger.error("错误: DataFrame中没有date列")
            return
        
        logger.info(f"DataFrame中的date列类型: {df['date'].dtype}")
        logger.info(f"DataFrame中的date列示例: {df['date'].iloc[0]}")
        
        # 插入数据
        rows_inserted = 0
        for _, row in df.iterrows():
            # 构建INSERT语句
            insert_sql = """
            INSERT INTO debug_timestamps 
            (fund_code, data_level, date, time, open, high, low, close, volume, amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
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
            
            # 打印要插入的数据
            logger.info(f"插入数据: fund_code={fund_code_val}, data_level={data_level_val}, date={date_val}, time={time_val}")
            
            # 执行插入
            cursor.execute(insert_sql, (
                fund_code_val, data_level_val, date_val, time_val, open_val, high_val, 
                low_val, close_val, volume_val, amount_val
            ))
            rows_inserted += 1
        
        # 提交事务
        conn.commit()
        logger.info(f"成功插入 {rows_inserted} 条记录")
        
        # 查询插入的数据
        cursor.execute("""
        SELECT id, fund_code, data_level, date, time 
        FROM debug_timestamps 
        ORDER BY id
        """)
        
        data = cursor.fetchall()
        logger.info(f"\n查询结果 ({len(data)} 条记录):")
        for row in data:
            id_val, fund_code, data_level, date, time_val = row
            logger.info(f"ID: {id_val}, 基金: {fund_code}, 级别: {data_level}, 日期: {date}, 时间戳: {time_val}")
            
            # 验证日期是否正确
            correct_date = pd.to_datetime(time_val / 1000, unit='s')
            logger.info(f"  - 从时间戳计算的日期: {correct_date}")
            logger.info(f"  - 日期是否匹配: {date.strftime('%Y-%m-%d %H:%M:%S') == correct_date.strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        logger.error(f"保存数据到数据库失败: {str(e)}")
        traceback.print_exc()
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def test_real_data_save():
    """测试实际数据保存流程"""
    logger.info("=== 测试实际数据保存流程 ===")
    
    try:
        # 导入实际的数据获取和保存类
        from backtest_gui.fund_data_fetcher import FundDataFetcher
        from PyQt5.QtCore import QCoreApplication
        import sys
        
        # 创建应用
        app = QCoreApplication(sys.argv)
        
        # 创建数据获取器
        fetcher = FundDataFetcher()
        
        # 设置信号处理函数
        def on_progress(current, total, message):
            logger.info(f"进度: {current}/{total} - {message}")
        
        def on_completed(success, message, data):
            logger.info(f"完成: {'成功' if success else '失败'} - {message}")
            if data is not None:
                logger.info(f"获取到 {len(data)} 条数据")
                logger.info("数据示例:")
                logger.info(data.head())
                
                # 检查日期是否正确
                if 'date' in data.columns:
                    logger.info("\n检查日期是否正确:")
                    for i, (_, row) in enumerate(data.head().iterrows()):
                        logger.info(f"记录 {i+1}:")
                        logger.info(f"  - 时间戳: {row['time']}")
                        logger.info(f"  - 日期: {row['date']}")
                        logger.info(f"  - 年份: {row['date'].year}")
            
            # 退出应用
            app.quit()
        
        def on_error(message):
            logger.error(f"错误: {message}")
            app.quit()
        
        fetcher.progress_signal.connect(on_progress)
        fetcher.completed_signal.connect(on_completed)
        fetcher.error_signal.connect(on_error)
        
        # 测试获取数据
        symbol = "515170.SH"  # 测试用ETF
        start_date = "20240501"  # 2024年5月1日
        end_date = "20240610"    # 2024年6月10日
        data_level = "day"       # 日线数据
        
        logger.info(f"开始获取 {symbol} 的 {data_level} 数据，日期范围: {start_date} - {end_date}")
        fetcher.fetch_data(symbol, start_date=start_date, end_date=end_date, data_level=data_level, save_to_db=True)
        
        # 运行应用
        sys.exit(app.exec_())
        
    except Exception as e:
        logger.error(f"测试实际数据保存流程失败: {str(e)}")
        traceback.print_exc()

def main():
    """主函数"""
    # 测试模拟数据
    df = create_test_data()
    df = test_timestamp_conversion(df)
    test_database_save(df)
    
    # 测试实际数据保存流程
    # test_real_data_save()

if __name__ == "__main__":
    main() 