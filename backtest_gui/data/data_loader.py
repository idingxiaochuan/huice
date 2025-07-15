#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据加载器模块 - 用于从数据库加载历史行情数据
"""
import psycopg2
import pandas as pd
import datetime
import numpy as np
from PyQt5.QtCore import QThread, pyqtSignal
from concurrent.futures import ThreadPoolExecutor
import traceback


class DataLoader(QThread):
    """数据加载线程，用于异步加载历史行情数据"""
    
    # 定义信号
    data_loaded = pyqtSignal(object)  # 数据加载完成信号，传递一批数据
    loading_progress = pyqtSignal(int, int)  # 加载进度信号，传递当前进度和总数
    loading_finished = pyqtSignal()  # 加载完成信号
    loading_error = pyqtSignal(str)  # 加载错误信号，传递错误信息
    
    def __init__(self, db_config=None):
        """初始化数据加载器
        
        Args:
            db_config: 数据库配置字典，包含host, port, dbname, user, password
        """
        super().__init__()
        
        # 数据库连接配置
        self.db_config = db_config or {
            'host': '127.0.0.1',
            'port': 5432,
            'dbname': 'huice',
            'user': 'postgres',
            'password': 'postgres'
        }
        
        # 加载参数
        self.stock_code = None  # 股票代码
        self.start_date = None  # 起始日期
        self.end_date = None  # 结束日期
        self.batch_size = 500  # 每批加载的数据量，增大批量
        
        # 控制标志
        self.is_paused = False  # 是否暂停加载
        self.is_stopped = False  # 是否停止加载
        
        # 存储完整数据
        self.full_data = None
        
        # 线程池用于并行处理数据
        self.thread_pool = ThreadPoolExecutor(max_workers=4)
        
    def set_params(self, stock_code, start_date, end_date, batch_size=500):
        """设置加载参数
        
        Args:
            stock_code: 股票代码
            start_date: 起始日期
            end_date: 结束日期
            batch_size: 每批加载的数据量，默认增大到500
        """
        self.stock_code = stock_code
        self.start_date = start_date
        self.end_date = end_date
        self.batch_size = batch_size
        
    def get_earliest_date(self, stock_code):
        """获取指定股票代码的最早日期
        
        Args:
            stock_code: 股票代码
            
        Returns:
            最早的日期，格式为datetime对象
        """
        try:
            # 连接数据库
            conn = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                dbname=self.db_config['dbname'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            
            # 创建表名
            table_name = f"stock_1min_{stock_code.split('.')[0]}"
            
            # 查询最早日期
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT MIN(time) FROM {table_name}"
                )
                earliest_date = cur.fetchone()[0]
                
            # 关闭数据库连接
            conn.close()
            
            return earliest_date
            
        except Exception as e:
            print(f"获取最早日期错误: {str(e)}")
            return datetime.datetime.now() - datetime.timedelta(days=365)  # 默认返回一年前
            
    def load_all_data(self):
        """一次性加载所有数据
        
        Returns:
            加载的所有数据，pandas DataFrame对象
        """
        try:
            # 检查参数是否设置
            if not all([self.stock_code, self.start_date, self.end_date]):
                print("数据加载参数未设置完整")
                return None
            
            print(f"开始加载数据: 股票={self.stock_code}, 开始={self.start_date}, 结束={self.end_date}")
            
            # 连接数据库
            conn = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                dbname=self.db_config['dbname'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            
            # 创建表名
            table_name = f"stock_1min_{self.stock_code.split('.')[0]}"
            
            print(f"查询表: {table_name}")
            
            # 使用更高效的SQL查询，仅选择必要的列，添加索引提示
            query = (
                f"SELECT time as trade_time, open, high, low, close, volume "
                f"FROM {table_name} "
                f"WHERE time >= %s AND time <= %s "
                f"ORDER BY time"
            )
            
            # 打印查询参数
            print(f"查询参数: start_date={self.start_date}, end_date={self.end_date}")
            
            # 使用更高效的数据加载方式
            df = pd.read_sql(
                query, 
                conn, 
                params=(self.start_date, self.end_date),
                parse_dates=['trade_time']
            )
            
            # 打印加载结果
            print(f"查询结果: {len(df)} 行数据")
            
            # 设置索引为时间
            if not df.empty:
                # 确保数据按时间排序
                df = df.sort_values('trade_time')
                df.set_index('trade_time', inplace=True)
                
                # 打印时间范围
                print(f"数据时间范围: {df.index[0]} 到 {df.index[-1]}")
                
                # 数据类型优化
                df['open'] = df['open'].astype(np.float32)
                df['high'] = df['high'].astype(np.float32)
                df['low'] = df['low'].astype(np.float32)
                df['close'] = df['close'].astype(np.float32)
                df['volume'] = df['volume'].astype(np.int32)
                
                # 检查数据是否有效
                if df['close'].min() <= 0:
                    print(f"警告: 数据中存在无效价格 (最小值: {df['close'].min()})")
                
                # 检查数据是否有缺失
                missing = df.isna().sum()
                if missing.sum() > 0:
                    print(f"警告: 数据中存在缺失值: {missing}")
                    # 填充缺失值
                    df = df.fillna(method='ffill')
            else:
                print("警告: 查询结果为空")
            
            # 关闭数据库连接
            conn.close()
            
            return df
            
        except Exception as e:
            print(f"加载所有数据错误: {str(e)}")
            traceback.print_exc()
            return None
    
    def preprocess_data_batch(self, batch_df):
        """预处理数据批次以提高性能
        
        Args:
            batch_df: 数据批次
            
        Returns:
            处理后的数据批次
        """
        # 在实际应用中可以添加数据预处理步骤，如计算常用技术指标
        # 这里仅作为示例，实际上返回原始数据
        return batch_df
            
    def run(self):
        """线程运行函数，加载数据"""
        try:
            # 检查参数是否设置
            if not all([self.stock_code, self.start_date, self.end_date]):
                self.loading_error.emit("参数未设置")
                return
            
            # 一次性加载所有数据
            self.loading_progress.emit(0, 100)
            print("开始加载全部数据...")
            self.full_data = self.load_all_data()
            
            if self.full_data is None or len(self.full_data) == 0:
                self.loading_error.emit(f"未找到符合条件的数据")
                return
                
            # 计算总数据量
            total_count = len(self.full_data)
            print(f"数据加载完成，总行数: {total_count}")
            self.loading_progress.emit(10, 100)  # 更新进度到10%，表示数据加载完成
                
            # 分批发送数据，增大批量以减少处理次数
            batch_size = min(self.batch_size, total_count)
            num_batches = (total_count + batch_size - 1) // batch_size
            
            print(f"将数据分为 {num_batches} 批处理，每批 {batch_size} 行")
            
            for i in range(num_batches):
                # 处理暂停
                while self.is_paused and not self.is_stopped:
                    self.msleep(50)  # 减少暂停等待时间
                
                if self.is_stopped:
                    print("数据加载被停止")
                    break
                
                # 计算当前批次的起始和结束索引
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, total_count)
                
                # 获取当前批次数据
                batch_df = self.full_data.iloc[start_idx:end_idx]
                
                # 使用线程池预处理数据，提高性能
                processed_batch = self.preprocess_data_batch(batch_df)
                
                # 发送数据和进度
                print(f"发送批次 {i+1}/{num_batches}, 行数: {len(processed_batch)}")
                self.data_loaded.emit(processed_batch)
                
                # 修改进度计算，数据加载完成后只占10%，数据处理占90%
                # 数据处理的进度从10%到100%
                progress = int(10 + (i + 1) * 90 / num_batches)
                self.loading_progress.emit(progress, 100)
                
                # 减少加载间隔，提高处理速度
                self.msleep(10)
            
            # 发送加载完成信号
            if not self.is_stopped:
                print("所有数据批次发送完成")
                self.loading_finished.emit()
                
        except Exception as e:
            error_msg = f"数据加载错误: {str(e)}"
            print(error_msg)
            traceback.print_exc()
            self.loading_error.emit(error_msg)
        finally:
            # 关闭线程池
            self.thread_pool.shutdown(wait=False)
            
    def pause(self):
        """暂停加载"""
        self.is_paused = True
        
    def resume(self):
        """恢复加载"""
        self.is_paused = False
        
    def stop(self):
        """停止加载"""
        self.is_stopped = True 


class DBConnector:
    """数据库连接器，用于管理数据库连接池和执行查询"""
    
    def __init__(self, db_config=None):
        """初始化数据库连接器
        
        Args:
            db_config: 数据库配置，包含host, port, dbname, user, password
        """
        self.db_config = db_config or {
            'host': '127.0.0.1',
            'port': 5432,
            'dbname': 'postgres',
            'user': 'postgres',
            'password': 'huice'
        }
        self.connection_pool = []
        self._init_connection_pool()
        
    def _init_connection_pool(self):
        """初始化连接池"""
        try:
            # 清空现有连接池
            self.close_all()
            self.connection_pool = []
            
            # 创建新连接
            for _ in range(5):  # 创建5个连接
                try:
                    conn = psycopg2.connect(
                        host=self.db_config['host'],
                        port=self.db_config['port'],
                        dbname=self.db_config['dbname'],
                        user=self.db_config['user'],
                        password=self.db_config['password']
                    )
                    self.connection_pool.append(conn)
                except Exception as e:
                    print(f"创建数据库连接失败: {str(e)}")
            
            print(f"数据库连接池初始化完成，连接数: {len(self.connection_pool)}")
        except Exception as e:
            print(f"初始化连接池失败: {str(e)}")
            traceback.print_exc()
            
    def get_connection(self):
        """获取一个数据库连接
        
        Returns:
            connection: 数据库连接对象
        """
        try:
            # 如果连接池为空，初始化连接池
            if not self.connection_pool:
                self._init_connection_pool()
            
            # 从连接池获取一个连接
            if self.connection_pool:
                return self.connection_pool.pop()
            else:
                return None
        except Exception as e:
            print(f"获取数据库连接失败: {str(e)}")
            return None
    
    def execute_query(self, query, params=None):
        """执行SQL查询并返回结果
        
        Args:
            query: SQL查询语句
            params: 查询参数，可选
            
        Returns:
            list: 查询结果列表
        """
        conn = None
        try:
            conn = self.get_connection()
            if not conn:
                print("无法获取数据库连接")
                return []
                
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            # 获取列名
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # 获取结果
            rows = cursor.fetchall()
            
            # 将结果转换为字典列表
            result = []
            for row in rows:
                result.append(dict(zip(columns, row)))
                
            return result
        except Exception as e:
            print(f"执行查询失败: {str(e)}")
            return []
        finally:
            if conn:
                self.return_connection(conn)
    
    def execute_update(self, query, params=None):
        """执行SQL更新操作（INSERT, UPDATE, DELETE）
        
        Args:
            query: SQL更新语句
            params: 更新参数，可选
            
        Returns:
            int: 受影响的行数，-1表示失败
        """
        conn = None
        try:
            conn = self.get_connection()
            if not conn:
                print("无法获取数据库连接")
                return -1
                
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            # 获取受影响的行数
            affected_rows = cursor.rowcount
            
            # 提交事务
            conn.commit()
            
            return affected_rows
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"执行更新失败: {str(e)}")
            return -1
        finally:
            if conn:
                self.return_connection(conn)
            
    def return_connection(self, conn):
        """将连接归还到连接池
        
        Args:
            conn: 要归还的数据库连接
        """
        if conn and not conn.closed:
            self.connection_pool.append(conn)
    
    def close_all(self):
        """关闭所有连接"""
        for conn in self.connection_pool:
            try:
                if not conn.closed:
                    conn.close()
            except Exception as e:
                print(f"关闭连接失败: {str(e)}")
        
        self.connection_pool = [] 