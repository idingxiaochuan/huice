#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
简化版XIRR计算模块 - 计算回测交易的年化收益率，不依赖scipy
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import traceback
import decimal

class XIRRCalculatorSimple:
    """简化版XIRR计算器，用于计算回测交易的年化收益率"""
    
    def __init__(self, db_connector=None):
        """初始化XIRR计算器
        
        Args:
            db_connector: 数据库连接器
        """
        self.db_connector = db_connector
        
    def _xnpv(self, rate, cashflows):
        """计算XNPV (扩展净现值)
        
        Args:
            rate: 折现率
            cashflows: 现金流列表，格式为 [(datetime, amount), ...]
            
        Returns:
            float: XNPV值
        """
        try:
            chron_order = sorted(cashflows, key=lambda x: x[0])
            t0 = chron_order[0][0]  # 使用第一个现金流的时间作为基准时间
            
            # 确保所有金额都是浮点数类型
            return sum(float(cf) / (1 + float(rate)) ** ((t - t0).days / 365.0) for t, cf in chron_order)
        except Exception as e:
            print(f"XNPV计算异常: {str(e)}")
            traceback.print_exc()
            return 0.0
    
    def _secant_method(self, f, x0, x1, max_iterations=100, tolerance=1e-6):
        """使用割线法求解函数f(x)=0
        
        Args:
            f: 目标函数
            x0: 初始猜测值1
            x1: 初始猜测值2
            max_iterations: 最大迭代次数
            tolerance: 收敛容差
            
        Returns:
            float: 函数的根
        """
        try:
            # 确保初始值为浮点数
            x0 = float(x0)
            x1 = float(x1)
            # 确保初始值为浮点数
            x0 = float(x0)
            x1 = float(x1)
            f0 = f(x0)
            
            for i in range(max_iterations):
                f1 = f(x1)
                
                # 检查是否收敛
                if abs(f1) < tolerance:
                    return x1
                    
                # 计算下一个x值
                try:
                    # 防止除以零
                    if f1 - f0 == 0:
                        return x1
                        
                    x_new = x1 - f1 * (x1 - x0) / (f1 - f0)
                    
                    # 如果x_new超出了合理范围，尝试其他值
                    if x_new <= -1 or x_new > 1000:
                        x_new = (x0 + x1) / 2  # 折半
                except:
                    x_new = (x0 + x1) / 2  # 出错时尝试折半
                    
                # 更新值
                x0, f0 = x1, f1
                x1 = x_new
                
            # 如果达到最大迭代次数，返回最后一次的估计值
            return x1
        except Exception as e:
            print(f"割线法求解异常: {str(e)}")
            traceback.print_exc()
            return None
    
    def _xirr(self, cashflows, guess=0.1):
        """计算XIRR (扩展内部收益率)
        
        Args:
            cashflows: 现金流列表，格式为 [(datetime, amount), ...]
            guess: 初始猜测值
            
        Returns:
            float: XIRR值（年化收益率）
        """
        try:
            if not cashflows or len(cashflows) < 2:
                print("现金流不足，至少需要两个现金流")
                return None
            
            # 检查是否所有现金流都是同一个值
            values = [float(cf[1]) for cf in cashflows]
            if all(x == values[0] for x in values):
                print("所有现金流值相同，无法计算XIRR")
                return 0.0
            
            # 检查是否至少有一个正现金流和一个负现金流
            pos = False
            neg = False
            for _, v in cashflows:
                v_float = float(v)
                if v_float > 0:
                    pos = True
                if v_float < 0:
                    neg = True
            if not (pos and neg):
                print("现金流需要同时包含正值和负值")
                return 0.0
                
            # 使用自定义的割线法求解
            f = lambda r: self._xnpv(r, cashflows)
            result = self._secant_method(f, guess, guess + 0.1)
            
            # 限制XIRR的合理范围
            if result is not None:
                # 如果XIRR大于10（即1000%）或小于-0.9（即-90%），视为计算错误
                if result > 10 or result < -0.9:
                    print(f"XIRR计算结果超出合理范围: {result}, 视为计算错误")
                    return None
            
            return result
        except Exception as e:
            # 如果计算失败，返回None
            print(f"XIRR计算异常: {str(e)}")
            traceback.print_exc()
            return None
        
    def calculate_backtest_xirr(self, backtest_id):
        """计算指定回测的XIRR
        
        Args:
            backtest_id: 回测ID
            
        Returns:
            dict: XIRR计算结果，包含以下字段：
                - xirr: XIRR值
                - cashflows: 现金流DataFrame
                - has_incomplete_trades: 是否有未完成的交易
                - backtest_info: 回测基本信息
        """
        print(f"\n======== XIRR计算器 - 计算回测ID: {backtest_id} ========")
        
        try:
            if not self.db_connector:
                print("错误: 数据库连接器未初始化")
                return None
                
            print("获取数据库连接...")
            conn = self.db_connector.get_connection()
            if not conn:
                print("错误: 无法获取数据库连接")
                return None
                
            print("数据库连接成功")
            
            cursor = conn.cursor()
            
            # 获取回测基本信息
            print(f"查询回测基本信息: backtest_id={backtest_id}")
            query = """
                SELECT id, stock_code, start_date, end_date, initial_capital, 
                       final_capital, total_profit, total_profit_rate, 
                       backtest_time, strategy_name
                FROM backtest_results
                WHERE id = %s
            """
            print(f"执行SQL: {query.strip()}")
            cursor.execute(query, (backtest_id,))
            backtest_info = cursor.fetchone()
            
            if not backtest_info:
                print(f"错误: 找不到回测记录, backtest_id={backtest_id}")
                return None
                
            print(f"成功获取回测信息: ID={backtest_info[0]}, 股票={backtest_info[1]}")
                
            # 获取配对交易记录
            print(f"查询配对交易记录: backtest_id={backtest_id}")
            paired_query = """
                SELECT id, level, grid_type, buy_time, buy_price, buy_amount, buy_value,
                       sell_time, sell_price, sell_amount, sell_value, remaining_shares, 
                       band_profit, sell_band_profit_rate, status
                FROM backtest_paired_trades
                WHERE backtest_id = %s
                ORDER BY buy_time
            """
            print(f"执行SQL: {paired_query.strip()}")
            cursor.execute(paired_query, (backtest_id,))
            paired_trades = cursor.fetchall()
            print(f"配对交易记录数: {len(paired_trades)}")
            
            # 获取持仓信息
            print(f"查询持仓信息: backtest_id={backtest_id}")
            position_query = """
                SELECT position_amount, position_cost, last_price, position_value
                FROM backtest_positions
                WHERE backtest_id = %s
            """
            print(f"执行SQL: {position_query.strip()}")
            cursor.execute(position_query, (backtest_id,))
            position = cursor.fetchone()
            if position:
                print(f"持仓信息: 数量={position[0]}, 成本={position[1]}, 最新价={position[2]}, 市值={position[3]}")
            else:
                print("无持仓信息")
            
            # 释放数据库连接
            print("释放数据库连接")
            self.db_connector.release_connection(conn)
            
            # 转换回测信息
            backtest_info_dict = {
                'id': backtest_info[0],
                'stock_code': backtest_info[1],
                'start_date': backtest_info[2],
                'end_date': backtest_info[3],
                'initial_capital': float(backtest_info[4]) if backtest_info[4] is not None else 0.0,
                'final_capital': float(backtest_info[5]) if backtest_info[5] is not None else 0.0,
                'total_profit': float(backtest_info[6]) if backtest_info[6] is not None else 0.0,
                'total_profit_rate': float(backtest_info[7]) if backtest_info[7] is not None else 0.0,
                'backtest_time': backtest_info[8],
                'strategy_name': backtest_info[9]
            }
            
            # 构建现金流（优化版本）
            cashflows = []
            
            # 添加初始资金（作为负现金流）
            # 使用回测初始日期
            start_date = backtest_info_dict['start_date']
            initial_capital = float(backtest_info_dict['initial_capital'])  # 确保是浮点数
            cashflows.append((start_date, -initial_capital))
            
            # 处理配对交易
            has_incomplete_trades = False
            completed_trades_value = 0.0  # 已完成交易的价值
            pending_trades_value = 0.0    # 未完成交易的买入价值
            
            for trade in paired_trades:
                try:
                    buy_time = trade[3]
                    buy_value = float(trade[6]) if trade[6] is not None else 0.0
                    status = trade[14]
                    
                    # 累计买入价值
                    if status == '进行中':
                        pending_trades_value += buy_value
                        has_incomplete_trades = True
                    
                    # 如果有卖出，累计已完成交易价值
                    sell_time = trade[7]
                    if sell_time:
                        sell_value = float(trade[10]) if trade[10] is not None else 0.0
                        completed_trades_value += sell_value
                except Exception as e:
                    print(f"处理交易记录异常: {str(e)}")
                    continue
            
            # 添加最终资金（作为正现金流）
            # 如果有持仓，考虑持仓价值
            end_date = backtest_info_dict['end_date']
            
            # 计算最终现金流 = 最终资金 - 未完成交易的买入值
            final_value = float(backtest_info_dict['final_capital'])  # 确保是浮点数
            
            if position and position[0] and float(position[0]) > 0:
                # 如果有持仓信息，添加持仓价值
                position_value = float(position[3]) if position[3] is not None else 0.0
                final_value = float(backtest_info_dict['final_capital']) - position_value
                has_incomplete_trades = True
                
            cashflows.append((end_date, final_value))
            
            # 创建现金流DataFrame
            df_cashflows = pd.DataFrame(cashflows, columns=['date', 'amount'])
            
            # 计算XIRR
            xirr_value = None
            if len(cashflows) >= 2:  # 至少需要两个现金流才能计算XIRR
                print("开始计算XIRR...")
                xirr_value = self._xirr(cashflows)
                print(f"XIRR计算结果: {xirr_value}")
                if xirr_value is not None:
                    xirr_value = float(xirr_value) * 100  # 转换为百分比
                    print(f"XIRR百分比: {xirr_value:.2f}%")
                else:
                    print("XIRR计算失败，结果为None")
            else:
                print("错误: 现金流数量不足，无法计算XIRR")
            
            result = {
                'xirr': xirr_value,
                'cashflows': df_cashflows,
                'has_incomplete_trades': has_incomplete_trades,
                'backtest_info': backtest_info_dict
            }
            
            print(f"XIRR计算完成: xirr={xirr_value}, 未完成交易={has_incomplete_trades}")
            print("======== XIRR计算器 - 计算结束 ========\n")
            
            return result
                
        except Exception as e:
            error_tb = traceback.format_exc()
            print(f"计算XIRR严重异常: {str(e)}")
            print(f"异常类型: {type(e).__name__}")
            print(f"异常堆栈:\n{error_tb}")
            print("======== XIRR计算器 - 异常退出 ========\n")
            return None
            
    def export_to_excel(self, backtest_id, file_path=None):
        """导出XIRR计算结果到Excel
        
        Args:
            backtest_id: 回测ID
            file_path: Excel文件保存路径，如果为None，则使用默认路径
            
        Returns:
            bool: 是否成功导出
        """
        try:
            # 计算XIRR
            result = self.calculate_backtest_xirr(backtest_id)
            if not result:
                print("无法导出Excel：计算XIRR失败")
                return False
            
            # 如果未指定文件路径，使用默认路径
            if not file_path:
                stock_code = result['backtest_info']['stock_code']
                timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                file_path = f"xirr_{stock_code}_{backtest_id}_{timestamp}.xlsx"
                
            # 创建Excel写入器
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                # 写入回测基本信息
                backtest_info = result['backtest_info']
                df_info = pd.DataFrame([
                    ['回测ID', backtest_info['id']],
                    ['股票代码', backtest_info['stock_code']],
                    ['开始日期', backtest_info['start_date']],
                    ['结束日期', backtest_info['end_date']],
                    ['初始资金', backtest_info['initial_capital']],
                    ['最终资金', backtest_info['final_capital']],
                    ['总收益', backtest_info['total_profit']],
                    ['总收益率', f"{backtest_info['total_profit_rate']}%"],
                    ['XIRR(年化收益率)', f"{result['xirr']:.2f}%" if result['xirr'] is not None else "无法计算"],
                    ['是否有未完成交易', "是" if result['has_incomplete_trades'] else "否"],
                ], columns=['指标', '数值'])
                df_info.to_excel(writer, sheet_name='基本信息', index=False)
                
                # 写入现金流数据
                df_cashflows = result['cashflows']
                df_cashflows['date'] = df_cashflows['date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
                df_cashflows.to_excel(writer, sheet_name='现金流数据', index=False)
                
                # 添加XIRR函数
                sheet = writer.sheets['现金流数据']
                row_count = len(df_cashflows) + 2
                sheet.cell(row=row_count, column=1).value = 'XIRR计算结果:'
                
                # 使用Excel的XIRR函数公式
                formula = f'=XIRR(B2:B{len(df_cashflows)+1},A2:A{len(df_cashflows)+1})'
                sheet.cell(row=row_count, column=2).value = formula
                
            print(f"XIRR计算结果已导出至: {file_path}")
            return True
            
        except Exception as e:
            print(f"导出Excel异常: {str(e)}")
            traceback.print_exc()
            return False


# 使用示例
if __name__ == "__main__":
    # 假设已有数据库连接器
    from backtest_gui.utils.db_connector import DBConnector
    db_connector = DBConnector()
    
    # 创建XIRR计算器
    calculator = XIRRCalculatorSimple(db_connector)
    
    # 计算指定回测的XIRR
    backtest_id = 1  # 示例回测ID
    result = calculator.calculate_backtest_xirr(backtest_id)
    if result and result['xirr'] is not None:
        print(f"XIRR = {result['xirr']:.2f}%")
    else:
        print("无法计算XIRR")
    
    # 导出到Excel
    calculator.export_to_excel(backtest_id) 