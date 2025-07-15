#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据库连接器模块 - 用于管理数据库连接
"""
import os
import sys
import psycopg2
from psycopg2 import pool
import pandas as pd
import traceback
from datetime import datetime
from backtest_gui import settings
from backtest_gui.utils.time_utils import convert_timestamp_to_datetime

# 获取项目根目录
current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(os.path.dirname(current_dir))

# 将项目根目录添加到Python路径
if project_dir not in sys.path:
    sys.path.insert(0, project_dir)

# 导入设置
try:
    import settings
    DEFAULT_CONFIG = {
        'host': settings.DB_HOST,
        'port': settings.DB_PORT,
        'dbname': settings.DB_NAME,
        'user': settings.DB_USER,
        'password': settings.DB_PASSWORD
    }
except ImportError:
    print("警告: 无法导入settings.py，使用默认配置")
    DEFAULT_CONFIG = {
        'host': '127.0.0.1',
        'port': 5432,
        'dbname': 'huice',
        'user': 'postgres',
        'password': 'postgres'
    }

def get_database_connection():
    """获取数据库连接
    
    Returns:
        connection: 数据库连接对象
    """
    try:
        # 从配置中获取数据库连接参数
        db_host = getattr(settings, 'DB_HOST', 'localhost')
        db_port = getattr(settings, 'DB_PORT', 5432)
        db_name = getattr(settings, 'DB_NAME', 'huice')
        db_user = getattr(settings, 'DB_USER', 'postgres')
        db_password = getattr(settings, 'DB_PASSWORD', 'postgres')
        
        print(f"正在连接数据库 {db_name}...")
        print(f"连接信息: 主机={db_host}, 端口={db_port}, 用户={db_user}")
        
        # 创建数据库连接
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        
        print(f"成功连接到数据库 {db_name}")
        return conn
    except Exception as e:
        print(f"连接数据库失败: {str(e)}")
        traceback.print_exc()
        return None

def create_stock_table(conn, fund_code, level):
    """创建股票行情表，如果不存在
    
    Args:
        conn: 数据库连接
        fund_code: 基金代码
        level: 数据级别
    """
    try:
        cursor = conn.cursor()
        
        # 使用统一的stock_quotes表存储所有基金的行情数据
        # 这样便于查询和管理，并且可以避免表过多的问题
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_quotes (
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
        
        # 创建索引
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_stock_quotes_fund_date 
        ON stock_quotes(fund_code, data_level, date)
        """)
        
        # 提交事务
        conn.commit()
        
    except Exception as e:
        print(f"创建股票行情表失败: {str(e)}")
        traceback.print_exc()
        conn.rollback()

def save_to_database(conn, df, fund_code, table_name, level):
    """保存数据到数据库
    
    Args:
        conn: 数据库连接
        df: 数据DataFrame
        fund_code: 基金代码
        table_name: 表名
        level: 数据级别
    """
    try:
        cursor = conn.cursor()
        
        # 确保日期正确转换
        if 'time' in df.columns and 'date' not in df.columns:
            # 检查time列的类型
            if df['time'].dtype == 'int64' or df['time'].dtype == 'float64':
                # 时间戳格式转换为datetime，注意QMT返回的时间戳是毫秒级的
                df['date'] = df['time'].apply(convert_timestamp_to_datetime)
                print(f"转换时间戳示例: {df['time'].iloc[0]} -> {df['date'].iloc[0]}")
        
        # 准备数据插入
        rows = []
        for index, row in df.iterrows():
            rows.append((
                fund_code,
                level,
                row.get('date'),
                row.get('time', 0),
                row.get('open', 0.0),
                row.get('high', 0.0),
                row.get('low', 0.0),
                row.get('close', 0.0),
                row.get('volume', 0.0),
                row.get('amount', 0.0)
            ))
        
        # 批量插入数据
        insert_query = """
        INSERT INTO stock_quotes 
        (fund_code, data_level, date, time, open, high, low, close, volume, amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (fund_code, data_level, date) DO UPDATE
        SET time=EXCLUDED.time, 
            open=EXCLUDED.open, 
            high=EXCLUDED.high, 
            low=EXCLUDED.low, 
            close=EXCLUDED.close, 
            volume=EXCLUDED.volume, 
            amount=EXCLUDED.amount
        """
        
        # 使用executemany进行批量插入
        cursor.executemany(insert_query, rows)
        
        # 提交事务
        conn.commit()
        
        print(f"成功保存 {len(rows)} 条数据到表 {table_name}")
        
    except Exception as e:
        print(f"保存数据到数据库失败: {str(e)}")
        traceback.print_exc()
        conn.rollback()

class DBConnector:
    """数据库连接器类，管理PostgreSQL数据库连接池"""
    
    def __init__(self, config=None):
        """初始化数据库连接器
        
        Args:
            config: 数据库配置字典，包含host, port, dbname, user, password
        """
        self.config = config or DEFAULT_CONFIG
        
        self._connection_pool = None
        
    def init_pool(self, min_conn=1, max_conn=5):
        """初始化连接池
        
        Args:
            min_conn: 最小连接数
            max_conn: 最大连接数
            
        Returns:
            bool: 是否成功初始化
        """
        try:
            self._connection_pool = pool.ThreadedConnectionPool(
                min_conn,
                max_conn,
                host=self.config['host'],
                port=self.config['port'],
                dbname=self.config['dbname'],
                user=self.config['user'],
                password=self.config['password']
            )
            print(f"数据库连接池初始化完成，连接数: {max_conn}")
            return True
        except Exception as e:
            print(f"初始化数据库连接池失败: {str(e)}")
            return False
    
    def init_connection_pool(self, min_conn=1, max_conn=5):
        """初始化连接池(别名方法)
        
        Args:
            min_conn: 最小连接数
            max_conn: 最大连接数
            
        Returns:
            bool: 是否成功初始化
        """
        return self.init_pool(min_conn, max_conn)
            
    def get_connection(self):
        """获取数据库连接
        
        Returns:
            connection: 数据库连接对象
        
        Raises:
            Exception: 如果连接池未初始化或无法获取连接
        """
        if self._connection_pool is None:
            self.init_connection_pool()
            
        return self._connection_pool.getconn()
        
    def release_connection(self, connection):
        """归还连接到连接池
        
        Args:
            connection: 要归还的连接对象
        """
        if self._connection_pool is not None:
            self._connection_pool.putconn(connection)
    
    # 兼容旧代码的别名方法
    def return_connection(self, connection):
        """归还连接到连接池(别名方法)"""
        self.release_connection(connection)
            
    def close_all(self):
        """关闭所有连接和连接池"""
        if self._connection_pool is not None:
            self._connection_pool.closeall()
            self._connection_pool = None
            
    def test_connection(self):
        """测试数据库连接
        
        Returns:
            bool: 是否连接成功
        """
        try:
            # 尝试建立临时连接
            conn = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                dbname=self.config['dbname'],
                user=self.config['user'],
                password=self.config['password']
            )
            
            # 执行简单查询
            cursor = conn.cursor()
            cursor.execute('SELECT 1')
            result = cursor.fetchone()
            
            # 关闭连接
            cursor.close()
            conn.close()
            
            return result[0] == 1
        except Exception as e:
            print(f"测试数据库连接失败: {str(e)}")
            return False 