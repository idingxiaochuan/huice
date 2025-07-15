#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试实际数据获取和保存过程，验证时间戳修复是否有效
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import traceback
import psycopg2
from PyQt5.QtCore import QCoreApplication, QThread, pyqtSignal

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
    return logging.getLogger('test_real_data')

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

def create_test_table():
    """创建测试表"""
    conn = connect_to_db()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # 创建测试表，模拟stock_quotes表结构
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS test_real_data (
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(fund_code, data_level, date)
        )
        """)
        
        # 清除可能存在的测试数据
        cursor.execute("DELETE FROM test_real_data")
        conn.commit()
        
        logger.info("测试表创建成功")
        return True
    except Exception as e:
        logger.error(f"创建测试表失败: {str(e)}")
        traceback.print_exc()
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def create_test_data():
    """创建模拟QMT返回的测试数据"""
    # 创建毫秒级时间戳，模拟QMT返回的数据
    timestamps = [
        1716998400000,  # 2024-05-29 16:00:00
        1717084800000,  # 2024-05-30 16:00:00
        1717344000000,  # 2024-06-02 16:00:00
        1717430400000,  # 2024-06-03 16:00:00
        1717516800000   # 2024-06-04 16:00:00
    ]
    
    data = {
        'time': timestamps,
        'open': [0.619, 0.613, 0.612, 0.610, 0.620],
        'high': [0.619, 0.617, 0.615, 0.619, 0.620],
        'low': [0.610, 0.611, 0.606, 0.608, 0.610],
        'close': [0.613, 0.613, 0.610, 0.618, 0.612],
        'volume': [1000, 2000, 3000, 4000, 5000],
        'amount': [613, 1226, 1830, 2472, 3060]
    }
    
    df = pd.DataFrame(data)
    logger.info("测试数据创建完成")
    return df

class TestDataProcessor:
    """测试数据处理类，模拟实际的数据处理流程"""
    
    def __init__(self, symbol="515170.SH", data_level="day"):
        self.symbol = symbol
        self.data_level = data_level
    
    def process_data(self, df):
        """处理数据，模拟实际的数据处理流程"""
        logger.info("=== 开始处理数据 ===")
        
        # 添加符号和周期列
        df['symbol'] = self.symbol
        df['freq'] = self.data_level
        
        # 添加日期列，使用正确的转换方法
        if 'time' in df.columns:
            if isinstance(df['time'].iloc[0], (int, float)):
                # 时间戳格式转换为datetime，注意QMT返回的时间戳是毫秒级的
                # 将毫秒时间戳转换为秒级，再转换为datetime
                df['date'] = pd.to_datetime(df['time'] / 1000, unit='s')
                logger.info(f"转换时间戳示例: {df['time'].iloc[0]} -> {df['date'].iloc[0]}")
        
        # 转换为所需格式的字符串
        if self.data_level in ['1d', 'day', 'DAY']:
            df['date_str'] = df['date'].dt.strftime('%Y-%m-%d')
        else:
            df['date_str'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info("数据处理完成")
        return df
    
    def save_to_db(self, df):
        """保存数据到数据库，模拟实际的保存流程"""
        logger.info("=== 开始保存数据到数据库 ===")
        
        if df is None or df.empty:
            logger.error("没有数据可保存")
            return False
        
        # 检查并修复日期列，确保日期是正确的
        if 'time' in df.columns and 'date' not in df.columns:
            # 检查time列的类型
            if isinstance(df['time'].iloc[0], (int, float)):
                # 时间戳格式转换为datetime，注意QMT返回的时间戳是毫秒级的
                df['date'] = pd.to_datetime(df['time'] / 1000, unit='s')
                logger.info(f"保存前修复时间戳: {df['time'].iloc[0]} -> {df['date'].iloc[0]}")
        
        # 提取基金代码（去掉后缀）
        fund_code = self.symbol.split('.')[0]
        
        conn = connect_to_db()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # 插入每一行数据
            rows_inserted = 0
            rows_updated = 0
            for _, row in df.iterrows():
                # 构建INSERT语句，使用ON CONFLICT DO UPDATE子句处理唯一约束冲突
                insert_sql = """
                INSERT INTO test_real_data 
                (fund_code, data_level, date, time, open, high, low, close, volume, amount, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (fund_code, data_level, date) DO UPDATE SET
                time = EXCLUDED.time,
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                created_at = CURRENT_TIMESTAMP
                """
                
                # 准备数据
                fund_code_val = fund_code
                data_level_val = self.data_level
                date_val = row['date'] if 'date' in row else None
                time_val = row['time'] if 'time' in row else None
                open_val = float(row['open'])
                high_val = float(row['high'])
                low_val = float(row['low'])
                close_val = float(row['close'])
                volume_val = float(row['volume'])
                amount_val = float(row['amount']) if 'amount' in row and not pd.isna(row['amount']) else 0.0
                
                # 执行插入
                cursor.execute(insert_sql, (
                    fund_code_val, data_level_val, date_val, time_val, open_val, high_val, 
                    low_val, close_val, volume_val, amount_val
                ))
                
                if cursor.rowcount == 1:
                    rows_inserted += 1
                else:
                    rows_updated += 1
            
            # 提交事务
            conn.commit()
            
            logger.info(f"成功保存 {rows_inserted} 条新记录和更新 {rows_updated} 条记录到表 test_real_data")
            return True
        
        except Exception as e:
            logger.error(f"保存数据到数据库失败: {str(e)}")
            traceback.print_exc()
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    
    def verify_saved_data(self):
        """验证保存的数据"""
        logger.info("=== 验证保存的数据 ===")
        
        conn = connect_to_db()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # 查询保存的数据
            cursor.execute("""
            SELECT id, fund_code, data_level, date, time 
            FROM test_real_data 
            ORDER BY date
            """)
            
            data = cursor.fetchall()
            logger.info(f"查询到 {len(data)} 条记录")
            
            # 验证日期是否正确
            all_correct = True
            for row in data:
                id_val, fund_code, data_level, date, time_val = row
                logger.info(f"ID: {id_val}, 基金: {fund_code}, 级别: {data_level}, 日期: {date}, 时间戳: {time_val}")
                
                # 使用正确的方法计算日期
                correct_date = pd.to_datetime(time_val / 1000, unit='s')
                
                # 比较日期
                date_str = date.strftime('%Y-%m-%d %H:%M:%S')
                correct_date_str = correct_date.strftime('%Y-%m-%d %H:%M:%S')
                is_match = date_str == correct_date_str
                
                logger.info(f"  - 从时间戳计算的日期: {correct_date}")
                logger.info(f"  - 日期是否匹配: {is_match}")
                
                if not is_match:
                    all_correct = False
            
            if all_correct:
                logger.info("所有日期都正确匹配！")
            else:
                logger.error("存在日期不匹配的记录！")
            
            return all_correct
        
        except Exception as e:
            logger.error(f"验证数据失败: {str(e)}")
            traceback.print_exc()
            return False
        finally:
            if conn:
                conn.close()

def test_with_real_fetcher():
    """使用实际的FundDataFetcher测试"""
    try:
        # 导入实际的数据获取类
        from backtest_gui.fund_data_fetcher import FundDataFetcher
        
        logger.info("=== 使用实际的FundDataFetcher测试 ===")
        
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
                        
                        # 验证日期是否正确
                        correct_date = pd.to_datetime(row['time'] / 1000, unit='s')
                        logger.info(f"  - 从时间戳计算的日期: {correct_date}")
                        logger.info(f"  - 日期是否匹配: {row['date'].strftime('%Y-%m-%d %H:%M:%S') == correct_date.strftime('%Y-%m-%d %H:%M:%S')}")
            
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
        return app.exec_()
        
    except Exception as e:
        logger.error(f"使用实际的FundDataFetcher测试失败: {str(e)}")
        traceback.print_exc()
        return 1

def main():
    """主函数"""
    # 创建测试表
    if not create_test_table():
        return 1
    
    # 创建测试数据
    df = create_test_data()
    
    # 处理数据
    processor = TestDataProcessor()
    df = processor.process_data(df)
    
    # 保存数据到数据库
    if not processor.save_to_db(df):
        return 1
    
    # 验证保存的数据
    if not processor.verify_saved_data():
        return 1
    
    # 使用实际的FundDataFetcher测试
    # 注意：如果不想运行这部分，可以注释掉
    # return test_with_real_fetcher()
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 