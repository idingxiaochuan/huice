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
                        pt.remaining, pt.band_profit, pt.band_profit_rate, pt.status,
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