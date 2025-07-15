import os
import time
import psycopg2
import psycopg2.pool
import psycopg2.extras
import traceback
import pandas as pd
from datetime import datetime

class Database:
    def __init__(self, host='localhost', port=5432, user='postgres', password='postgres', database='huice'):
        """初始化数据库连接参数"""
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.pool = None
        self._init_connection_pool()
    
    def _init_connection_pool(self):
        """初始化数据库连接池"""
        try:
            # 检查连接池是否已经存在
            if hasattr(self, 'pool') and self.pool:
                # 尝试检查连接池状态
                try:
                    conn = self.pool.getconn()
                    if conn:
                        # 连接池存在且能获取连接，直接返回
                        self.pool.putconn(conn)
                        return True
                except Exception:
                    # 连接池存在问题，需要重新创建
                    pass
            
            # 创建新的连接池
            self.pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=20,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            
            # 测试连接
            conn = self.pool.getconn()
            if conn:
                self.pool.putconn(conn)
                print(f"数据库连接池初始化完成，连接数: {self.pool.maxconn}")
                return True
            return False
        except Exception as e:
            print(f"初始化数据库连接池失败: {str(e)}")
            return False
            
    def connect(self):
        """连接到数据库"""
        try:
            if not hasattr(self, 'pool') or not self.pool:
                self._init_connection_pool()
                
            print(f"正在连接数据库 {self.database}...")
            print(f"连接信息: 主机={self.host}, 端口={self.port}, 用户={self.user}")
            
            # 尝试获取连接
            conn = self.pool.getconn()
            if conn:
                self.pool.putconn(conn)
                print(f"成功连接到数据库 {self.database}")
                
                # 初始化表结构
                self.init_tables()
                
                return True
            else:
                print("无法从连接池获取连接")
                return False
        except Exception as e:
            error_msg = f"连接数据库失败: {str(e)}"
            print(error_msg)
            traceback.print_exc()
            return False
            
    def get_connection(self):
        """获取数据库连接"""
        try:
            if not self.pool:
                self._init_connection_pool()
            return self.pool.getconn()
        except Exception as e:
            print(f"获取数据库连接失败: {str(e)}")
            return None
            
    def release_connection(self, conn):
        """释放数据库连接回连接池"""
        if conn and self.pool:
            self.pool.putconn(conn)
            
    def execute_query(self, query, params=None):
        """执行查询并返回结果"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            if not conn:
                return None
                
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute(query, params)
            
            # 检查是否是SELECT查询
            if query.strip().upper().startswith('SELECT'):
                result = cursor.fetchall()
                return result
            else:
                conn.commit()
                return cursor.rowcount
                
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"执行查询失败: {str(e)}")
            traceback.print_exc()
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.release_connection(conn)
                
    def init_tables(self):
        """初始化数据库表结构"""
        try:
            # 创建行情数据表
            create_market_data_table = """
            CREATE TABLE IF NOT EXISTS market_data (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(32) NOT NULL,
                date TIMESTAMP NOT NULL,
                open FLOAT NOT NULL,
                high FLOAT NOT NULL,
                low FLOAT NOT NULL,
                close FLOAT NOT NULL,
                volume BIGINT NOT NULL,
                amount FLOAT,
                freq VARCHAR(10) NOT NULL
            );
            """
            self.execute_query(create_market_data_table)
            
            # 添加索引
            create_market_data_index = """
            CREATE INDEX IF NOT EXISTS idx_market_data_symbol ON market_data (symbol);
            CREATE INDEX IF NOT EXISTS idx_market_data_date ON market_data (date);
            CREATE INDEX IF NOT EXISTS idx_market_data_symbol_date ON market_data (symbol, date);
            CREATE INDEX IF NOT EXISTS idx_market_data_symbol_freq ON market_data (symbol, freq);
            """
            self.execute_query(create_market_data_index)
            
            print("数据库表结构已更新")
            return True
        except Exception as e:
            print(f"初始化表结构失败: {str(e)}")
            traceback.print_exc()
            return False
            
    def save_market_data(self, df, symbol=None, freq=None):
        """保存行情数据到数据库"""
        if df is None or df.empty:
            print("没有行情数据可保存")
            return False
            
        # 如果传入了symbol和freq，添加到DataFrame
        if symbol and 'symbol' not in df.columns:
            df['symbol'] = symbol
        if freq and 'freq' not in df.columns:
            df['freq'] = freq
            
        # 确保有必要的列
        required_columns = ['symbol', 'freq', 'time', 'open', 'high', 'low', 'close', 'volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"行情数据缺少必要的列: {missing_columns}")
            return False
            
        try:
            # 打印数据类型和示例
            print(f"数据类型: {type(df)}")
            print(f"列名: {df.columns.tolist()}")
            print(f"数据示例:\n{df.head()}")
            
            # 添加date列 (将时间戳转换为日期)
            if 'date' not in df.columns and 'time' in df.columns:
                print(f"time列类型: {df['time'].dtype}")
                print(f"time列前5个值: {df['time'].head().tolist()}")
                
                # 确保time列是数值类型
                try:
                    if not pd.api.types.is_numeric_dtype(df['time']):
                        print("time列不是数值类型，尝试转换...")
                        df['time'] = pd.to_numeric(df['time'], errors='coerce')
                        print(f"转换后time列类型: {df['time'].dtype}")
                except Exception as e:
                    print(f"转换time列类型失败: {str(e)}")
                
                # 检查time列是否有NaN值
                if df['time'].isna().any():
                    print(f"警告: time列有 {df['time'].isna().sum()} 个NaN值")
                    # 填充NaN值
                    df = df.dropna(subset=['time'])
                    print(f"删除NaN后剩余记录数: {len(df)}")
                
                # 逐行转换时间戳，避免批量处理可能的错误
                print("开始逐行转换时间戳...")
                from backtest_gui.utils.time_utils import convert_timestamp_to_datetime
                dates = []
                for i, ts in enumerate(df['time']):
                    try:
                        date = convert_timestamp_to_datetime(ts)
                        if date is None:
                            print(f"警告: 第{i}行时间戳 {ts} 转换结果为None")
                            # 使用当前时间作为默认值
                            date = pd.Timestamp.now()
                        dates.append(date)
                    except Exception as e:
                        print(f"警告: 第{i}行时间戳 {ts} 转换失败: {str(e)}")
                        # 使用当前时间作为默认值
                        dates.append(pd.Timestamp.now())
                
                # 将转换后的日期添加到DataFrame
                df['date'] = dates
                print(f"时间戳转换完成，date列前5个值: {[str(d) for d in df['date'].head().tolist()]}")
            
            # 检查是否有无效数据
            invalid_rows = df[df['date'].isna()].shape[0]
            if invalid_rows > 0:
                print(f"警告: 有 {invalid_rows} 行数据的日期无效，将被过滤")
                df = df.dropna(subset=['date'])
            
            # 连接数据库
            conn = None
            cursor = None
            
            try:
                conn = self.get_connection()
                if not conn:
                    print("无法获取数据库连接")
                    return False
                    
                cursor = conn.cursor()
                
                # 插入数据
                rows_inserted = 0
                rows_updated = 0
                errors = 0
                
                print(f"开始保存数据到数据库，共 {len(df)} 条记录...")
                
                for i, row in df.iterrows():
                    try:
                        # 准备数据
                        symbol_val = row['symbol']
                        date_val = row['date']
                        freq_val = row['freq']
                        
                        # 检查日期值是否有效
                        if date_val is None:
                            print(f"警告: 第{i}行date值为None，跳过")
                            errors += 1
                            continue
                        
                        # 检查数值字段
                        try:
                            open_val = float(row['open'])
                            high_val = float(row['high'])
                            low_val = float(row['low'])
                            close_val = float(row['close'])
                            volume_val = float(row['volume'])
                            amount_val = float(row['amount']) if 'amount' in row and not pd.isna(row['amount']) else 0.0
                        except (ValueError, TypeError) as e:
                            print(f"警告: 第{i}行数值转换失败: {str(e)}，跳过")
                            errors += 1
                            continue
                        
                        # 检查是否已存在相同的记录
                        check_sql = """
                        SELECT id FROM market_data 
                        WHERE symbol = %s AND date = %s AND freq = %s
                        """
                        cursor.execute(check_sql, (symbol_val, date_val, freq_val))
                        existing_record = cursor.fetchone()
                        
                        if existing_record:
                            # 更新现有记录
                            update_sql = """
                            UPDATE market_data 
                            SET open = %s, high = %s, low = %s, close = %s, volume = %s, amount = %s
                            WHERE id = %s
                            """
                            cursor.execute(update_sql, (
                                open_val, high_val, low_val, close_val, volume_val, amount_val, existing_record[0]
                            ))
                            rows_updated += 1
                        else:
                            # 插入新记录
                            insert_sql = """
                            INSERT INTO market_data 
                            (symbol, date, open, high, low, close, volume, amount, freq)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            cursor.execute(insert_sql, (
                                symbol_val, date_val, open_val, high_val, low_val, close_val, volume_val, amount_val, freq_val
                            ))
                            rows_inserted += 1
                        
                        # 每1000条记录提交一次，避免事务过大
                        if (rows_inserted + rows_updated) % 1000 == 0:
                            conn.commit()
                            print(f"已处理 {rows_inserted + rows_updated} 条记录...")
                            
                    except Exception as e:
                        print(f"处理第{i}行数据失败: {str(e)}")
                        errors += 1
                
                # 提交事务
                conn.commit()
                
                print(f"成功保存 {rows_inserted} 条新记录和更新 {rows_updated} 条记录到表 market_data，失败 {errors} 条")
                return True
                
            except Exception as e:
                if conn:
                    conn.rollback()
                print(f"保存行情数据失败: {str(e)}")
                traceback.print_exc()
                return False
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    self.release_connection(conn)
                    
        except Exception as e:
            print(f"处理行情数据失败: {str(e)}")
            traceback.print_exc()
            return False
            
    def get_market_data(self, symbol, start_date=None, end_date=None, freq='1d'):
        """从数据库获取行情数据"""
        try:
            # 构建查询
            query = """
            SELECT * FROM market_data 
            WHERE symbol = %s AND freq = %s 
            """
            params = [symbol, freq]
            
            # 添加日期过滤
            if start_date:
                query += " AND date >= %s"
                params.append(start_date)
                
            if end_date:
                query += " AND date <= %s"
                params.append(end_date)
                
            # 按日期排序
            query += " ORDER BY date ASC"
            
            # 执行查询
            result = self.execute_query(query, params)
            
            if result:
                # 转换为DataFrame
                df = pd.DataFrame(result)
                print(f"从数据库获取了 {len(df)} 条 {symbol} 的行情数据")
                return df
            else:
                print(f"未找到 {symbol} 的行情数据")
                return None
                
        except Exception as e:
            print(f"获取行情数据失败: {str(e)}")
            traceback.print_exc()
            return None
            
    def get_symbols_list(self):
        """获取数据库中的所有股票代码"""
        try:
            query = "SELECT DISTINCT symbol FROM market_data"
            result = self.execute_query(query)
            
            if result:
                symbols = [row[0] for row in result]
                return symbols
            else:
                return []
                
        except Exception as e:
            print(f"获取股票列表失败: {str(e)}")
            return []
            
    def get_available_dates(self, symbol, freq='1d'):
        """获取指定股票的可用日期范围"""
        try:
            query = """
            SELECT MIN(date), MAX(date) FROM market_data 
            WHERE symbol = %s AND freq = %s
            """
            result = self.execute_query(query, (symbol, freq))
            
            if result and result[0][0] and result[0][1]:
                start_date = result[0][0].strftime('%Y-%m-%d')
                end_date = result[0][1].strftime('%Y-%m-%d')
                return (start_date, end_date)
            else:
                return (None, None)
                
        except Exception as e:
            print(f"获取日期范围失败: {str(e)}")
            return (None, None)
            
    def delete_market_data(self, symbol=None, freq=None):
        """删除行情数据"""
        try:
            query = "DELETE FROM market_data WHERE 1=1"
            params = []
            
            if symbol:
                query += " AND symbol = %s"
                params.append(symbol)
                
            if freq:
                query += " AND freq = %s"
                params.append(freq)
                
            rows_deleted = self.execute_query(query, params)
            print(f"成功删除 {rows_deleted} 条行情数据")
            return rows_deleted
            
        except Exception as e:
            print(f"删除行情数据失败: {str(e)}")
            traceback.print_exc()
            return 0 