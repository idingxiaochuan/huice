#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
回测数据管理器 - 用于处理回测数据的持久化和加载
"""
import os
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import traceback
from datetime import datetime
from backtest_gui.utils.db_connector import DBConnector


class BacktestDataManager:
    """回测数据管理器，用于保存和加载回测结果"""
    
    def __init__(self, db_connector=None):
        """初始化回测数据管理器
        
        Args:
            db_connector: 数据库连接器对象，如果为None则创建新的连接器
        """
        self.db_connector = db_connector or DBConnector()
        
        # 确保数据库连接池已初始化
        if not hasattr(self.db_connector, '_connection_pool') or self.db_connector._connection_pool is None:
            self.db_connector.init_connection_pool()
            
        self.backtest_id = None
        
        # 确保数据库表已创建
        self._ensure_tables_exist()
        
    def _ensure_tables_exist(self):
        """确保数据库表结构存在"""
        conn = None
        try:
            conn = self.db_connector.get_connection()
            cursor = conn.cursor()
            
            # 执行数据库表创建SQL脚本
            try:
                # 尝试导入SQL脚本
                import os
                from pathlib import Path
                
                # 获取当前文件所在目录
                current_dir = Path(os.path.dirname(os.path.abspath(__file__)))
                
                # SQL脚本路径
                sql_file = current_dir / 'db_schema.sql'
                
                if os.path.exists(sql_file):
                    with open(sql_file, 'r', encoding='utf-8') as f:
                        sql_script = f.read()
                        cursor.execute(sql_script)
                    
                    conn.commit()
                    print("数据库表结构已更新")
                else:
                    print(f"未找到SQL脚本文件: {sql_file}")
                    
            except Exception as e:
                print(f"执行SQL脚本错误: {str(e)}")
                traceback.print_exc()
            
        except Exception as e:
            print(f"检查数据库表结构错误: {str(e)}")
            traceback.print_exc()
        finally:
            if conn:
                self.db_connector.release_connection(conn)
                
    def _convert_numpy_types(self, value):
        """将NumPy类型转换为Python原生类型
        
        Args:
            value: 任意值
            
        Returns:
            转换后的值
        """
        # 检查是否是NumPy类型（具有item方法）
        if hasattr(value, 'item'):
            if isinstance(value.item(), (int, float, bool)):
                return value.item()
        return value
        
    def save_backtest_results(self, stock_code, start_date, end_date, initial_capital, 
                           final_capital, total_profit, total_profit_rate, trades, positions=None,
                           strategy_id=None, strategy_name=None, strategy_version_id=None):
        """保存回测结果到数据库
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            initial_capital: 初始资金
            final_capital: 最终资金
            total_profit: 总收益
            total_profit_rate: 总收益率
            trades: 交易记录列表
            positions: 持仓记录列表
            strategy_id: 策略ID
            strategy_name: 策略名称
            strategy_version_id: 策略版本ID
            
        Returns:
            int: 回测ID，失败返回None
        """
        try:
            conn = None
            try:
                conn = self.db_connector.get_connection()
                cursor = conn.cursor()
                
                # 确保表结构存在
                self._ensure_tables_exist()
                
                # 插入回测结果
                cursor.execute(
                    """
                    INSERT INTO backtest_results 
                    (stock_code, start_date, end_date, initial_capital, 
                     final_capital, total_profit, total_profit_rate, backtest_time,
                     strategy_id, strategy_name, strategy_version_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s)
                    RETURNING id
                    """,
                    (stock_code, start_date, end_date, initial_capital, 
                     final_capital, total_profit, total_profit_rate,
                     strategy_id, strategy_name, strategy_version_id)
                )
                
                backtest_id = cursor.fetchone()[0]
                
                # 插入交易记录
                if trades:
                    for trade in trades:
                        cursor.execute(
                            """
                            INSERT INTO backtest_trades 
                            (backtest_id, trade_time, trade_type, price, shares, amount, commission)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            (backtest_id, trade['time'], trade['type'], 
                             trade['price'], trade['shares'], trade['amount'],
                             trade['commission'])
                        )
                
                # 插入持仓记录
                if positions:
                    for position in positions:
                        cursor.execute(
                            """
                            INSERT INTO backtest_positions 
                            (backtest_id, position_time, shares, cost, market_value)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (backtest_id, position['time'], position['shares'], 
                             position['cost'], position['market_value'])
                        )
                
                conn.commit()
                print(f"成功保存回测结果，ID: {backtest_id}")
                return backtest_id
                
            except Exception as e:
                if conn:
                    conn.rollback()
                print(f"保存回测结果失败: {str(e)}")
                traceback.print_exc()
                return None
                
            finally:
                if conn:
                    self.db_connector.release_connection(conn)
                    
        except Exception as e:
            print(f"保存回测结果异常: {str(e)}")
            traceback.print_exc()
            return None
            
    def save_trades(self, trades):
        """保存交易记录
        
        Args:
            trades: 交易记录列表，每个记录为字典
            
        Returns:
            bool: 是否保存成功
        """
        if not self.backtest_id:
            print("错误: 未设置回测ID，无法保存交易记录")
            return False
            
        try:
            conn = None
            try:
                conn = self.db_connector.get_connection()
                cursor = conn.cursor()
                
                # 准备批量插入的数据
                trade_data = []
                for trade in trades:
                    trade_data.append((
                        self.backtest_id,
                        trade['time'],
                        trade['type'],
                        trade['price'],
                        trade['amount'],
                        trade['value'],
                        trade.get('level'),
                        trade.get('grid_type'),
                        trade.get('band_profit'),
                        trade.get('band_profit_rate'),
                        trade.get('remaining')
                    ))
                
                # 批量插入
                execute_values(
                    cursor,
                    """
                    INSERT INTO backtest_trades
                    (backtest_id, trade_time, trade_type, price, amount, trade_value, 
                     level, grid_type, band_profit, band_profit_rate, remaining)
                    VALUES %s
                    """,
                    trade_data
                )
                
                conn.commit()
                print(f"保存交易记录成功，数量: {len(trade_data)}")
                return True
                
            except Exception as e:
                print(f"保存交易记录失败: {str(e)}")
                traceback.print_exc()
                if conn:
                    conn.rollback()
                return False
            finally:
                if conn:
                    self.db_connector.release_connection(conn)
                    
        except Exception as e:
            print(f"保存交易记录异常: {str(e)}")
            traceback.print_exc()
            return False
            
    def save_paired_trades(self, paired_trades):
        """保存配对交易记录
        
        Args:
            paired_trades: 配对交易记录字典
            
        Returns:
            bool: 是否保存成功
        """
        if not self.backtest_id:
            print("错误: 未设置回测ID，无法保存配对交易记录")
            return False
            
        try:
            conn = None
            try:
                conn = self.db_connector.get_connection()
                cursor = conn.cursor()
                
                # 准备批量插入的数据
                paired_trade_data = []
                for key, pair in paired_trades.items():
                    buy_record = pair.get('buy')
                    sell_record = pair.get('sell')
                    status = pair.get('status', '进行中')
                    
                    if buy_record:
                        # 解析key中的level和grid_type
                        key_parts = key.split('_')
                        level = int(key_parts[0]) if key_parts[0].isdigit() else None
                        grid_type = key_parts[1] if len(key_parts) > 1 else None
                        
                        # 构建记录
                        record = [
                            self.backtest_id,
                            level,
                            grid_type,
                            buy_record['time'],
                            buy_record['price'],
                            buy_record['amount'],
                            buy_record['value'],
                            None,  # sell_time
                            None,  # sell_price
                            None,  # sell_amount
                            None,  # sell_value
                            buy_record['amount'],  # remaining (旧字段)
                            buy_record['amount'],  # remaining_shares (新字段)
                            None,  # band_profit
                            None,  # band_profit_rate (旧字段)
                            None,  # sell_band_profit_rate (新字段)
                            status
                        ]
                        
                        # 添加卖出信息（如果有）
                        if sell_record:
                            record[7] = sell_record['time']  # sell_time
                            record[8] = sell_record['price']  # sell_price
                            record[9] = sell_record['amount']  # sell_amount
                            record[10] = sell_record['value']  # sell_value
                            record[11] = sell_record.get('remaining', buy_record['amount'] - sell_record['amount'])  # remaining
                            # 计算正确的剩余份额
                            remaining_shares = buy_record['amount'] - sell_record['amount']
                            record.append(remaining_shares)  # remaining_shares
                            record[12] = sell_record.get('band_profit')  # band_profit
                            record[13] = sell_record.get('band_profit_rate')  # band_profit_rate
                            # 卖出部分的收益率
                            sell_band_profit_rate = sell_record.get('sell_band_profit_rate', sell_record.get('band_profit_rate'))
                            record.append(sell_band_profit_rate)  # sell_band_profit_rate
                        
                        paired_trade_data.append(tuple(record))
                
                # 批量插入
                if paired_trade_data:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO backtest_paired_trades
                        (backtest_id, level, grid_type, buy_time, buy_price, buy_amount, buy_value,
                         sell_time, sell_price, sell_amount, sell_value, remaining, remaining_shares, band_profit, 
                         band_profit_rate, sell_band_profit_rate, status)
                        VALUES %s
                        """,
                        paired_trade_data
                    )
                    
                    conn.commit()
                    print(f"保存配对交易记录成功，数量: {len(paired_trade_data)}")
                    return True
                else:
                    print("没有可保存的配对交易记录")
                    return True
                    
            except Exception as e:
                print(f"保存配对交易记录失败: {str(e)}")
                traceback.print_exc()
                if conn:
                    conn.rollback()
                return False
            finally:
                if conn:
                    self.db_connector.release_connection(conn)
                    
        except Exception as e:
            print(f"保存配对交易记录异常: {str(e)}")
            traceback.print_exc()
            return False
            
    def save_position(self, position_amount, position_cost, last_price, position_value):
        """保存持仓信息
        
        Args:
            position_amount: 持仓数量
            position_cost: 持仓成本
            last_price: 最后价格
            position_value: 持仓市值
            
        Returns:
            bool: 是否保存成功
        """
        if not self.backtest_id:
            print("错误: 未设置回测ID，无法保存持仓信息")
            return False
            
        try:
            # 将NumPy类型转换为Python原生类型
            position_amount = self._convert_numpy_types(position_amount)
            position_cost = self._convert_numpy_types(position_cost)
            last_price = self._convert_numpy_types(last_price)
            position_value = self._convert_numpy_types(position_value)
            
            conn = None
            try:
                conn = self.db_connector.get_connection()
                cursor = conn.cursor()
                
                # 插入持仓记录
                cursor.execute(
                    """
                    INSERT INTO backtest_positions
                    (backtest_id, position_amount, position_cost, last_price, position_value)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (self.backtest_id, position_amount, position_cost, last_price, position_value)
                )
                
                conn.commit()
                print(f"保存持仓信息成功")
                return True
                
            except Exception as e:
                print(f"保存持仓信息失败: {str(e)}")
                traceback.print_exc()
                if conn:
                    conn.rollback()
                return False
            finally:
                if conn:
                    self.db_connector.release_connection(conn)
                    
        except Exception as e:
            print(f"保存持仓信息异常: {str(e)}")
            traceback.print_exc()
            return False
            
    def save_nav_data(self, nav_df):
        """保存净值数据
        
        Args:
            nav_df: 净值数据DataFrame，索引为时间，包含nav列
            
        Returns:
            bool: 是否保存成功
        """
        if not self.backtest_id:
            print("错误: 未设置回测ID，无法保存净值数据")
            return False
            
        if nav_df is None or nav_df.empty:
            print("没有净值数据可保存")
            return False
            
        try:
            conn = None
            try:
                conn = self.db_connector.get_connection()
                cursor = conn.cursor()
                
                # 准备批量插入的数据
                nav_data = []
                for time, row in nav_df.iterrows():
                    # 将NumPy类型转换为Python原生类型
                    nav_value = self._convert_numpy_types(row['nav'])
                    nav_data.append((self.backtest_id, time, nav_value))
                
                # 批量插入
                execute_values(
                    cursor,
                    """
                    INSERT INTO backtest_nav
                    (backtest_id, time, nav)
                    VALUES %s
                    """,
                    nav_data
                )
                
                conn.commit()
                print(f"保存净值数据成功，点数: {len(nav_data)}")
                return True
                
            except Exception as e:
                print(f"保存净值数据失败: {str(e)}")
                traceback.print_exc()
                if conn:
                    conn.rollback()
                return False
            finally:
                if conn:
                    self.db_connector.release_connection(conn)
                    
        except Exception as e:
            print(f"保存净值数据异常: {str(e)}")
            traceback.print_exc()
            return False
    
    def load_backtest_results(self, stock_code):
        """加载指定基金的回测结果记录列表
        
        Args:
            stock_code: 基金代码
            
        Returns:
            list: 回测结果记录列表，每个记录为字典
        """
        try:
            conn = None
            try:
                conn = self.db_connector.get_connection()
                cursor = conn.cursor()
                
                # 查询回测结果
                cursor.execute(
                    """
                    SELECT id, stock_code, start_date, end_date, 
                           initial_capital, final_capital, total_profit, 
                           total_profit_rate, backtest_time, strategy_id, strategy_name
                    FROM backtest_results
                    WHERE stock_code = %s
                    ORDER BY backtest_time DESC
                    """,
                    (stock_code,)
                )
                
                # 获取列名
                columns = [desc[0] for desc in cursor.description]
                
                # 获取结果
                results = []
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                
                return results
                
            except Exception as e:
                print(f"加载回测结果失败: {str(e)}")
                traceback.print_exc()
                return []
                
            finally:
                if conn:
                    self.db_connector.release_connection(conn)
                    
        except Exception as e:
            print(f"加载回测结果异常: {str(e)}")
            traceback.print_exc()
            return []
    
    def load_backtest_data(self, backtest_id):
        """加载指定回测ID的所有数据
        
        Args:
            backtest_id: 回测ID
            
        Returns:
            dict: 包含所有回测数据的字典
                {
                    'results': 回测结果记录,
                    'trades': 交易记录列表,
                    'paired_trades': 配对交易记录字典,
                    'position': 持仓信息,
                    'nav_data': 净值数据DataFrame
                }
        """
        self.backtest_id = backtest_id
        
        # 收集所有数据
        result = {
            'results': None,
            'trades': [],
            'paired_trades': {},
            'position': None,
            'nav_data': None
        }
        
        try:
            conn = None
            try:
                conn = self.db_connector.get_connection()
                cursor = conn.cursor()
                
                # 1. 加载回测结果基本信息
                cursor.execute(
                    """
                    SELECT id, stock_code, start_date, end_date, 
                           initial_capital, final_capital, total_profit, 
                           total_profit_rate, backtest_time
                    FROM backtest_results
                    WHERE id = %s
                    """,
                    (backtest_id,)
                )
                
                row = cursor.fetchone()
                if row:
                    result['results'] = {
                        'id': row[0],
                        'stock_code': row[1],
                        'start_date': row[2],
                        'end_date': row[3],
                        'initial_capital': row[4],
                        'final_capital': row[5],
                        'total_profit': row[6],
                        'total_profit_rate': row[7],
                        'backtest_time': row[8]
                    }
                
                # 2. 加载交易记录
                cursor.execute(
                    """
                    SELECT id, trade_time, trade_type, price, amount, trade_value, 
                           level, grid_type, band_profit, band_profit_rate, remaining
                    FROM backtest_trades
                    WHERE backtest_id = %s
                    ORDER BY trade_time
                    """,
                    (backtest_id,)
                )
                
                trades = cursor.fetchall()
                for row in trades:
                    result['trades'].append({
                        'id': row[0],
                        'time': row[1],
                        'type': row[2],
                        'price': row[3],
                        'amount': row[4],
                        'value': row[5],
                        'level': row[6],
                        'grid_type': row[7],
                        'band_profit': row[8],
                        'band_profit_rate': row[9],
                        'remaining': row[10]
                    })
                
                # 3. 加载配对交易记录
                cursor.execute(
                    """
                    SELECT id, level, grid_type, buy_time, buy_price, buy_amount, buy_value,
                           sell_time, sell_price, sell_amount, sell_value, remaining, remaining_shares,
                           band_profit, band_profit_rate, sell_band_profit_rate, status
                    FROM backtest_paired_trades
                    WHERE backtest_id = %s
                    ORDER BY buy_time
                    """,
                    (backtest_id,)
                )
                
                paired_trades = cursor.fetchall()
                for row in paired_trades:
                    # 创建唯一键
                    key = f"{row[1]}_{row[2]}_{row[3].timestamp()}"
                    
                    # 构建买入记录
                    buy_record = {
                        'time': row[3],
                        'price': row[4],
                        'amount': row[5],
                        'value': row[6],
                        'level': row[1],
                        'grid_type': row[2]
                    }
                    
                    # 构建卖出记录（如果有）
                    sell_record = None
                    if row[7]:  # sell_time
                        sell_record = {
                            'time': row[7],
                            'price': row[8],
                            'amount': row[9],
                            'value': row[10],
                            'remaining': row[11],
                            'remaining_shares': row[12],  # 新增字段：剩余份额
                            'band_profit': row[13],
                            'band_profit_rate': row[14],  # 旧字段
                            'sell_band_profit_rate': row[15],  # 新字段：卖出部分收益率
                            'level': row[1],
                            'grid_type': row[2]
                        }
                    
                    # 添加到结果字典
                    result['paired_trades'][key] = {
                        'buy': buy_record,
                        'sell': sell_record,
                        'status': row[16]  # 状态字段索引更新
                    }
                
                # 4. 加载持仓信息
                cursor.execute(
                    """
                    SELECT id, position_amount, position_cost, last_price, position_value
                    FROM backtest_positions
                    WHERE backtest_id = %s
                    """,
                    (backtest_id,)
                )
                
                row = cursor.fetchone()
                if row:
                    result['position'] = {
                        'id': row[0],
                        'position_amount': row[1],
                        'position_cost': row[2],
                        'last_price': row[3],
                        'position_value': row[4]
                    }
                
                # 5. 加载净值数据
                cursor.execute(
                    """
                    SELECT time, nav
                    FROM backtest_nav
                    WHERE backtest_id = %s
                    ORDER BY time
                    """,
                    (backtest_id,)
                )
                
                nav_rows = cursor.fetchall()
                if nav_rows:
                    # 构建DataFrame
                    nav_data = pd.DataFrame([(row[0], row[1]) for row in nav_rows], 
                                          columns=['time', 'nav'])
                    nav_data.set_index('time', inplace=True)
                    result['nav_data'] = nav_data
                
                return result
                
            except Exception as e:
                print(f"加载回测数据失败: {str(e)}")
                traceback.print_exc()
                return result
            finally:
                if conn:
                    self.db_connector.release_connection(conn)
                    
        except Exception as e:
            print(f"加载回测数据异常: {str(e)}")
            traceback.print_exc()
            return result
            
    def delete_backtest(self, backtest_id):
        """删除指定回测ID的所有数据
        
        Args:
            backtest_id: 回测ID
            
        Returns:
            bool: 是否删除成功
        """
        try:
            conn = None
            try:
                conn = self.db_connector.get_connection()
                cursor = conn.cursor()
                
                # 删除回测结果记录（会级联删除所有关联数据）
                cursor.execute(
                    """
                    DELETE FROM backtest_results
                    WHERE id = %s
                    """,
                    (backtest_id,)
                )
                
                deleted_count = cursor.rowcount
                conn.commit()
                
                print(f"删除回测数据成功，ID: {backtest_id}, 删除记录数: {deleted_count}")
                return deleted_count > 0
                
            except Exception as e:
                print(f"删除回测数据失败: {str(e)}")
                traceback.print_exc()
                if conn:
                    conn.rollback()
                return False
            finally:
                if conn:
                    self.db_connector.release_connection(conn)
                    
        except Exception as e:
            print(f"删除回测数据异常: {str(e)}")
            traceback.print_exc()
            return False
            
    def delete_all_backtest_for_stock(self, stock_code):
        """删除指定基金的所有回测数据
        
        Args:
            stock_code: 基金代码
            
        Returns:
            bool: 是否删除成功
        """
        try:
            conn = None
            try:
                conn = self.db_connector.get_connection()
                cursor = conn.cursor()
                
                # 删除回测结果记录（会级联删除所有关联数据）
                cursor.execute(
                    """
                    DELETE FROM backtest_results
                    WHERE stock_code = %s
                    """,
                    (stock_code,)
                )
                
                deleted_count = cursor.rowcount
                conn.commit()
                
                print(f"删除基金回测数据成功，代码: {stock_code}, 删除记录数: {deleted_count}")
                return deleted_count > 0
                
            except Exception as e:
                print(f"删除基金回测数据失败: {str(e)}")
                traceback.print_exc()
                if conn:
                    conn.rollback()
                return False
            finally:
                if conn:
                    self.db_connector.release_connection(conn)
                    
        except Exception as e:
            print(f"删除基金回测数据异常: {str(e)}")
            traceback.print_exc()
            return False
    
    def load_backtest_info(self, backtest_id):
        """加载回测基本信息
        
        Args:
            backtest_id: 回测ID
            
        Returns:
            dict: 回测基本信息
        """
        try:
            conn = None
            try:
                conn = self.db_connector.get_connection()
                cursor = conn.cursor()
                
                # 查询回测基本信息
                cursor.execute(
                    """
                    SELECT id, stock_code, start_date, end_date, initial_capital, 
                           final_capital, total_profit, total_profit_rate, backtest_time,
                           strategy_id, strategy_name, strategy_version_id
                    FROM backtest_results
                    WHERE id = %s
                    """,
                    (backtest_id,)
                )
                
                # 获取列名
                columns = [desc[0] for desc in cursor.description]
                
                # 获取结果
                row = cursor.fetchone()
                if row:
                    return dict(zip(columns, row))
                else:
                    return None
                    
            except Exception as e:
                print(f"加载回测基本信息失败: {str(e)}")
                traceback.print_exc()
                return None
                
            finally:
                if conn:
                    self.db_connector.release_connection(conn)
                    
        except Exception as e:
            print(f"加载回测基本信息异常: {str(e)}")
            traceback.print_exc()
            return None
    
    def load_backtest_trades(self, backtest_id):
        """加载回测交易记录
        
        Args:
            backtest_id: 回测ID
            
        Returns:
            list: 交易记录列表
        """
        try:
            conn = None
            try:
                conn = self.db_connector.get_connection()
                cursor = conn.cursor()
                
                # 查询交易记录
                cursor.execute(
                    """
                    SELECT id, backtest_id, trade_time, trade_type, 
                           price, shares, amount, commission
                    FROM backtest_trades
                    WHERE backtest_id = %s
                    ORDER BY trade_time
                    """,
                    (backtest_id,)
                )
                
                # 获取列名
                columns = [desc[0] for desc in cursor.description]
                
                # 获取结果
                trades = []
                for row in cursor.fetchall():
                    trades.append(dict(zip(columns, row)))
                
                return trades
                
            except Exception as e:
                print(f"加载回测交易记录失败: {str(e)}")
                traceback.print_exc()
                return []
                
            finally:
                if conn:
                    self.db_connector.release_connection(conn)
                    
        except Exception as e:
            print(f"加载回测交易记录异常: {str(e)}")
            traceback.print_exc()
            return []
    
    def load_backtest_positions(self, backtest_id):
        """加载回测持仓记录
        
        Args:
            backtest_id: 回测ID
            
        Returns:
            list: 持仓记录列表
        """
        try:
            conn = None
            try:
                conn = self.db_connector.get_connection()
                cursor = conn.cursor()
                
                # 查询持仓记录
                cursor.execute(
                    """
                    SELECT id, backtest_id, position_time, 
                           shares, cost, market_value
                    FROM backtest_positions
                    WHERE backtest_id = %s
                    ORDER BY position_time
                    """,
                    (backtest_id,)
                )
                
                # 获取列名
                columns = [desc[0] for desc in cursor.description]
                
                # 获取结果
                positions = []
                for row in cursor.fetchall():
                    positions.append(dict(zip(columns, row)))
                
                return positions
                
            except Exception as e:
                print(f"加载回测持仓记录失败: {str(e)}")
                traceback.print_exc()
                return []
                
            finally:
                if conn:
                    self.db_connector.release_connection(conn)
                    
        except Exception as e:
            print(f"加载回测持仓记录异常: {str(e)}")
            traceback.print_exc()
            return []
    
    def load_stock_data(self, stock_code, data_granularity, start_date, end_date):
        """从数据库加载股票数据
        
        Args:
            stock_code: 股票代码
            data_granularity: 数据粒度 (1min, 5min, 15min, 30min, 60min, day, week, month)
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            DataFrame: 股票数据
        """
        try:
            conn = None
            try:
                conn = self.db_connector.get_connection()
                cursor = conn.cursor()
                
                # 处理股票代码，去掉可能的市场后缀
                code = stock_code.split('.')[0]
                
                # 检查统一的股票数据表是否存在
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'stock_quotes'
                    );
                    """
                )
                
                table_exists = cursor.fetchone()[0]
                if not table_exists:
                    print("数据表 stock_quotes 不存在")
                    
                    # 尝试查询旧表格式
                    table_name = f"stock_{data_granularity}_{code}"
                    if data_granularity == "day":
                        table_name = f"stock_day_{code}"
                    elif data_granularity == "week":
                        table_name = f"stock_week_{code}"
                    elif data_granularity == "month":
                        table_name = f"stock_month_{code}"
                    else:
                        table_name = f"stock_{data_granularity}_{code}"
                    
                    cursor.execute(
                        """
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = %s
                        );
                        """,
                        (table_name,)
                    )
                    
                    old_table_exists = cursor.fetchone()[0]
                    if not old_table_exists:
                        print(f"旧数据表 {table_name} 也不存在")
                        return None
                    
                    # 使用旧表查询数据
                    cursor.execute(
                        f"""
                        SELECT date AS time, open, high, low, close, volume, amount
                        FROM {table_name}
                        WHERE date BETWEEN %s AND %s
                        ORDER BY date
                        """,
                        (start_date, end_date)
                    )
                else:
                    # 从统一表查询数据
                    cursor.execute(
                        """
                        SELECT date AS time, open, high, low, close, volume, amount
                        FROM stock_quotes
                        WHERE fund_code = %s AND data_level = %s
                        AND date BETWEEN %s AND %s
                        ORDER BY date
                        """,
                        (code, data_granularity, start_date, end_date)
                    )
                
                # 获取结果
                rows = cursor.fetchall()
                if not rows:
                    print(f"未找到 {code} 在 {start_date} 至 {end_date} 期间的 {data_granularity} 数据")
                    return None
                
                # 转换为DataFrame
                df = pd.DataFrame(rows, columns=['date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
                
                print(f"成功从数据库加载 {code} 的 {data_granularity} 数据，共 {len(df)} 条记录")
                return df
                
            except Exception as e:
                print(f"加载股票数据失败: {str(e)}")
                traceback.print_exc()
                return None
                
            finally:
                if conn:
                    self.db_connector.release_connection(conn)
                    
        except Exception as e:
            print(f"加载股票数据异常: {str(e)}")
            traceback.print_exc()
            return None 