#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
回测引擎模块 - 用于执行回测逻辑
"""
import pandas as pd
import numpy as np
from datetime import datetime
import traceback

class BacktestEngine:
    """回测引擎，用于执行回测逻辑"""
    
    def __init__(self):
        """初始化回测引擎"""
        pass
    
    def run_backtest(self, stock_data, strategy, executor, progress_callback=None):
        """执行回测
        
        Args:
            stock_data: 股票数据
            strategy: 策略对象
            executor: 交易执行器
            progress_callback: 进度回调函数，接收当前进度和总进度两个参数
            
        Returns:
            dict: 回测结果
        """
        try:
            # 初始化结果
            result = {
                'stock_data': stock_data,
                'trades': [],
                'positions': [],
                'success': False
            }
            
            # 检查数据
            if stock_data is None or len(stock_data) == 0:
                print("股票数据为空，无法进行回测")
                return result
                
            # 重置执行器
            executor.reset()
            
            # 总数据条数
            total_rows = len(stock_data)
            
            # 遍历每个时间点的数据
            for i, (_, row) in enumerate(stock_data.iterrows()):
                # 获取当前时间和价格
                current_time = row['date']
                current_price = row['close']
                
                # 处理策略
                signals = strategy.process_tick(current_time, current_price)
                
                # 执行交易
                if signals:
                    for signal in signals:
                        executor.execute_trade(
                            time=current_time,
                            price=current_price,
                            trade_type=signal['type'],
                            shares=signal['shares']
                        )
                
                # 更新持仓市值
                executor.update_position_value(current_time, current_price)
                
                # 报告进度 - 更频繁地调用回调函数以提供更平滑的图表更新
                if progress_callback:
                    # 对于较小的数据集（少于1000条），每10条更新一次
                    # 对于较大的数据集，每50条更新一次
                    update_frequency = 10 if total_rows < 1000 else 50
                    
                    if i % update_frequency == 0 or i == total_rows - 1:
                        if not progress_callback(i, total_rows):
                            print("回测被用户取消")
                            return result
            
            # 设置回测成功标志
            result['success'] = True
            
            # 获取交易记录
            result['trades'] = executor.get_trade_history()
            
            # 获取持仓记录
            result['positions'] = executor.get_position_history()
            
            return result
            
        except Exception as e:
            print(f"回测执行错误: {str(e)}")
            traceback.print_exc()
            return {
                'stock_data': stock_data,
                'trades': [],
                'positions': [],
                'success': False,
                'error': str(e)
            } 