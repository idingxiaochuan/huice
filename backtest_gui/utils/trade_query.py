#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
交易查询工具 - 提供配对交易查询功能
"""
import pandas as pd
import traceback
from datetime import datetime


class TradeQuery:
    """交易查询工具类"""
    
    def __init__(self, db_connector=None):
        """初始化交易查询工具
        
        Args:
            db_connector: 数据库连接器对象
        """
        self.db_connector = db_connector
        
    def get_paired_trades(self, fund_code=None, strategy_id=None, start_date=None, end_date=None, level=None, status=None):
        """查询配对交易记录
        
        Args:
            fund_code: 基金代码，可选
            strategy_id: 策略ID，可选
            start_date: 开始日期，可选
            end_date: 结束日期，可选
            level: 网格级别，可选
            status: 交易状态，可选
            
        Returns:
            DataFrame: 配对交易记录
        """
        try:
            if not self.db_connector:
                print("无法查询配对交易：数据库连接器未初始化")
                return None
                
            conn = self.db_connector.get_connection()
            if not conn:
                print("无法获取数据库连接")
                return None
                
            try:
                cursor = conn.cursor()
                
                # 构建查询SQL
                query = """
                    SELECT 
                        pt.id, pt.backtest_id, pt.level, pt.grid_type, 
                        pt.buy_time, pt.buy_price, pt.buy_amount, pt.buy_value,
                        pt.sell_time, pt.sell_price, pt.sell_amount, pt.sell_value, 
                        pt.remaining, pt.remaining_shares, pt.band_profit, 
                        pt.band_profit_rate, pt.sell_band_profit_rate, pt.status,
                        br.stock_code, br.strategy_id, br.strategy_name,
                        fi.fund_name, fi.fund_type, fi.manager, fi.company
                    FROM 
                        backtest_paired_trades pt
                    JOIN 
                        backtest_results br ON pt.backtest_id = br.id
                    LEFT JOIN 
                        fund_info fi ON br.stock_code = fi.fund_code
                    WHERE 1=1
                """
                
                params = []
                
                # 添加查询条件
                if fund_code:
                    query += " AND br.stock_code = %s"
                    params.append(fund_code)
                    
                if strategy_id:
                    query += " AND br.strategy_id = %s"
                    params.append(strategy_id)
                    
                if start_date:
                    query += " AND pt.buy_time >= %s"
                    params.append(start_date)
                    
                if end_date:
                    query += " AND (pt.sell_time <= %s OR pt.sell_time IS NULL)"
                    params.append(end_date)
                    
                if level:
                    query += " AND pt.level = %s"
                    params.append(level)
                    
                if status:
                    query += " AND pt.status = %s"
                    params.append(status)
                    
                # 添加排序
                query += " ORDER BY pt.buy_time DESC, pt.level"
                
                # 执行查询
                cursor.execute(query, params)
                
                # 获取结果
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                
                # 转换为DataFrame
                if results:
                    df = pd.DataFrame(results, columns=columns)
                    print(f"查询到 {len(df)} 条配对交易记录")
                    return df
                else:
                    print("未查询到配对交易记录")
                    return pd.DataFrame()
                    
            except Exception as e:
                print(f"查询配对交易记录失败: {str(e)}")
                traceback.print_exc()
                return None
            finally:
                if conn:
                    self.db_connector.release_connection(conn)
                    
        except Exception as e:
            print(f"查询配对交易记录过程中出错: {str(e)}")
            traceback.print_exc()
            return None
            
    def get_grid_levels_for_fund(self, fund_code):
        """查询基金的网格级别配置
        
        Args:
            fund_code: 基金代码
            
        Returns:
            DataFrame: 网格级别配置
        """
        try:
            if not self.db_connector:
                print("无法查询网格级别：数据库连接器未初始化")
                return None
                
            conn = self.db_connector.get_connection()
            if not conn:
                print("无法获取数据库连接")
                return None
                
            try:
                cursor = conn.cursor()
                
                # 查询基金绑定的策略
                cursor.execute("""
                    SELECT fs.strategy_id, bs.name
                    FROM fund_strategy_bindings fs
                    JOIN band_strategies bs ON fs.strategy_id = bs.id
                    WHERE fs.fund_code = %s
                    ORDER BY fs.is_default DESC
                    LIMIT 1
                """, (fund_code,))
                
                strategy = cursor.fetchone()
                
                if not strategy:
                    print(f"基金 {fund_code} 没有绑定的波段策略")
                    return None
                    
                strategy_id = strategy[0]
                strategy_name = strategy[1]
                
                # 查询网格级别配置
                cursor.execute("""
                    SELECT gl.id, gl.level, gl.grid_type, gl.buy_price, gl.sell_price, 
                           gl.buy_shares, gl.sell_shares, bs.name as strategy_name
                    FROM grid_levels gl
                    JOIN band_strategies bs ON gl.strategy_id = bs.id
                    WHERE gl.strategy_id = %s
                    ORDER BY gl.level
                """, (strategy_id,))
                
                # 获取结果
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                
                # 转换为DataFrame
                if results:
                    df = pd.DataFrame(results, columns=columns)
                    print(f"查询到 {len(df)} 条网格级别配置")
                    return df
                else:
                    print(f"未查询到网格级别配置")
                    return pd.DataFrame()
                    
            except Exception as e:
                print(f"查询网格级别配置失败: {str(e)}")
                traceback.print_exc()
                return None
            finally:
                if conn:
                    self.db_connector.release_connection(conn)
                    
        except Exception as e:
            print(f"查询网格级别配置过程中出错: {str(e)}")
            traceback.print_exc()
            return None
            
    def get_backtest_summary(self, fund_code=None, strategy_id=None, start_date=None, end_date=None, backtest_id=None):
        """查询回测汇总信息
        
        Args:
            fund_code: 基金代码，可选
            strategy_id: 策略ID，可选
            start_date: 开始日期，可选
            end_date: 结束日期，可选
            backtest_id: 回测ID，可选
            
        Returns:
            DataFrame: 回测汇总信息
        """
        try:
            if not self.db_connector:
                print("无法查询回测汇总：数据库连接器未初始化")
                return None
                
            conn = self.db_connector.get_connection()
            if not conn:
                print("无法获取数据库连接")
                return None
                
            try:
                cursor = conn.cursor()
                
                # 构建查询SQL
                query = """
                    SELECT 
                        br.id, br.stock_code, br.start_date, br.end_date, 
                        br.initial_capital, br.final_capital, br.total_profit, br.total_profit_rate,
                        br.backtest_time, br.strategy_id, br.strategy_name,
                        fi.fund_name, fi.fund_type, fi.manager, fi.company,
                        (SELECT COUNT(*) FROM backtest_paired_trades WHERE backtest_id = br.id) as trade_count,
                        (SELECT COUNT(*) FROM backtest_paired_trades WHERE backtest_id = br.id AND status = '已完成') as completed_trades,
                        (SELECT COUNT(*) FROM backtest_paired_trades WHERE backtest_id = br.id AND status = '进行中') as open_trades
                    FROM 
                        backtest_results br
                    LEFT JOIN 
                        fund_info fi ON br.stock_code = fi.fund_code
                    WHERE 1=1
                """
                
                params = []
                
                # 添加查询条件
                if backtest_id:
                    query += " AND br.id = %s"
                    params.append(backtest_id)
                
                if fund_code:
                    query += " AND br.stock_code = %s"
                    params.append(fund_code)
                    
                if strategy_id:
                    query += " AND br.strategy_id = %s"
                    params.append(strategy_id)
                    
                if start_date:
                    query += " AND br.start_date >= %s"
                    params.append(start_date)
                    
                if end_date:
                    query += " AND br.end_date <= %s"
                    params.append(end_date)
                    
                # 添加排序
                query += " ORDER BY br.backtest_time DESC"
                
                # 执行查询
                cursor.execute(query, params)
                
                # 获取结果
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                
                # 转换为DataFrame
                if results:
                    df = pd.DataFrame(results, columns=columns)
                    print(f"查询到 {len(df)} 条回测汇总记录")
                    return df
                else:
                    print("未查询到回测汇总记录")
                    return pd.DataFrame()
                    
            except Exception as e:
                print(f"查询回测汇总信息失败: {str(e)}")
                traceback.print_exc()
                return None
            finally:
                if conn:
                    self.db_connector.release_connection(conn)
                    
        except Exception as e:
            print(f"查询回测汇总信息过程中出错: {str(e)}")
            traceback.print_exc()
            return None 

    def get_max_capital_used(self, backtest_id):
        """获取回测的最高占用资金
        
        Args:
            backtest_id: 回测ID
            
        Returns:
            float: 最高占用资金
        """
        try:
            conn = self.db_connector.get_connection()
            if not conn:
                print("无法获取数据库连接")
                return 0.0
                
            cursor = conn.cursor()
            
            # 获取所有配对交易记录，按时间排序
            cursor.execute("""
                SELECT id, buy_time, buy_value, sell_time, sell_value, remaining
                FROM backtest_paired_trades
                WHERE backtest_id = %s
                ORDER BY buy_time
            """, (backtest_id,))
            
            trades = cursor.fetchall()
            if not trades:
                return 0.0
                
            # 跟踪每个时间点的资金占用
            time_points = []  # 时间点列表
            capital_used = []  # 对应的资金占用
            
            # 初始资金占用为0
            current_capital_used = 0.0
            
            # 处理每笔交易
            for trade in trades:
                trade_id = trade[0]
                buy_time = trade[1]
                buy_value = float(trade[2]) if trade[2] is not None else 0.0
                sell_time = trade[3]
                sell_value = float(trade[4]) if trade[4] is not None else 0.0
                remaining = int(trade[5]) if trade[5] is not None else 0
                
                # 买入时增加资金占用
                current_capital_used += buy_value
                time_points.append(buy_time)
                capital_used.append(current_capital_used)
                
                # 卖出时减少资金占用
                if sell_time:
                    # 如果有剩余股数，只减去卖出部分的资金占用
                    if remaining > 0:
                        # 计算卖出比例
                        sell_ratio = sell_value / buy_value
                        # 减去卖出部分的资金占用
                        current_capital_used -= buy_value * sell_ratio
                    else:
                        # 全部卖出，减去全部买入金额
                        current_capital_used -= buy_value
                    
                    time_points.append(sell_time)
                    capital_used.append(current_capital_used)
            
            # 找出最大资金占用
            if capital_used:
                max_capital_used = max(capital_used)
                print(f"回测ID {backtest_id} 的最高资金占用: {max_capital_used:.2f}")
                return max_capital_used
            else:
                return 0.0
            
        except Exception as e:
            print(f"获取最高占用资金失败: {str(e)}")
            traceback.print_exc()
            return 0.0
        finally:
            if conn:
                self.db_connector.release_connection(conn)
    
    def get_buy_count(self, backtest_id):
        """获取回测的买入次数
        
        Args:
            backtest_id: 回测ID
            
        Returns:
            int: 买入次数
        """
        try:
            conn = self.db_connector.get_connection()
            if not conn:
                print("无法获取数据库连接")
                return 0
                
            cursor = conn.cursor()
            
            # 查询买入次数（从配对交易记录中获取）
            cursor.execute("""
                SELECT COUNT(*) FROM backtest_paired_trades 
                WHERE backtest_id = %s
            """, (backtest_id,))
            
            result = cursor.fetchone()
            if not result:
                return 0
                
            return int(result[0])
            
        except Exception as e:
            print(f"获取买入次数失败: {str(e)}")
            traceback.print_exc()
            return 0
        finally:
            if conn:
                self.db_connector.release_connection(conn)
    
    def get_sell_count(self, backtest_id):
        """获取回测的卖出次数
        
        Args:
            backtest_id: 回测ID
            
        Returns:
            int: 卖出次数
        """
        try:
            conn = self.db_connector.get_connection()
            if not conn:
                print("无法获取数据库连接")
                return 0
                
            cursor = conn.cursor()
            
            # 查询卖出次数（从配对交易记录中获取已完成的交易）
            cursor.execute("""
                SELECT COUNT(*) FROM backtest_paired_trades 
                WHERE backtest_id = %s AND sell_time IS NOT NULL
            """, (backtest_id,))
            
            result = cursor.fetchone()
            if not result:
                return 0
                
            return int(result[0])
            
        except Exception as e:
            print(f"获取卖出次数失败: {str(e)}")
            traceback.print_exc()
            return 0
        finally:
            if conn:
                self.db_connector.release_connection(conn)
    
    def get_avg_cost(self, backtest_id):
        """获取回测的平均成本价
        
        Args:
            backtest_id: 回测ID
            
        Returns:
            float: 平均成本价
        """
        try:
            conn = self.db_connector.get_connection()
            if not conn:
                print("无法获取数据库连接")
                return 0.0
                
            cursor = conn.cursor()
            
            # 查询所有买入记录
            cursor.execute("""
                SELECT SUM(remaining) AS total_shares, SUM(remaining * buy_price) AS total_cost
                FROM backtest_paired_trades
                WHERE backtest_id = %s AND remaining > 0
            """, (backtest_id,))
            
            result = cursor.fetchone()
            if not result or not result[0] or float(result[0]) == 0:
                return 0.0
                
            total_shares = float(result[0])
            total_cost = float(result[1])
            
            # 计算平均成本
            avg_cost = total_cost / total_shares if total_shares > 0 else 0.0
            
            return avg_cost
            
        except Exception as e:
            print(f"获取平均成本价失败: {str(e)}")
            traceback.print_exc()
            return 0.0
        finally:
            if conn:
                self.db_connector.release_connection(conn)
    
    def get_xirr_value(self, backtest_id):
        """获取回测的交易专用XIRR值
        
        Args:
            backtest_id: 回测ID
            
        Returns:
            float: XIRR百分比值
        """
        try:
            # 尝试从数据库中获取已计算的XIRR值
            conn = self.db_connector.get_connection()
            if not conn:
                print("无法获取数据库连接")
                return None
                
            cursor = conn.cursor()
            
            # 查询XIRR值
            cursor.execute("""
                SELECT xirr_value FROM backtest_xirr 
                WHERE backtest_id = %s AND xirr_type = 'trades_only'
                ORDER BY calculation_time DESC
                LIMIT 1
            """, (backtest_id,))
            
            result = cursor.fetchone()
            if result and result[0] is not None:
                return float(result[0])  # 返回百分比形式的XIRR值
                
            # 如果数据库中没有，则尝试计算
            print(f"数据库中没有回测ID={backtest_id}的XIRR值，尝试计算...")
            from backtest_gui.utils.xirr_calculator_trades_only import XIRRCalculatorTradesOnly
            calculator = XIRRCalculatorTradesOnly(self.db_connector)
            result = calculator.calculate_backtest_xirr(backtest_id)
            
            if result and 'xirr_value' in result and result['xirr_value'] is not None:
                return float(result['xirr_value'])  # 返回百分比形式的XIRR值
                
            return None
            
        except Exception as e:
            print(f"获取XIRR值失败: {str(e)}")
            traceback.print_exc()
            return None
        finally:
            if conn:
                self.db_connector.release_connection(conn) 