#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
交易执行器模块 - 用于执行交易并记录交易历史
"""
import pandas as pd
import numpy as np
from datetime import datetime
import traceback

class TradeExecutor:
    """交易执行器，用于执行交易并记录交易历史"""
    
    def __init__(self, initial_capital=100000.0, commission_rate=0.0003, min_commission=0.1, slippage=0.0):
        """初始化交易执行器
        
        Args:
            initial_capital: 初始资金
            commission_rate: 手续费率
            min_commission: 最低手续费
            slippage: 滑点
        """
        self.initial_capital = initial_capital
        self.commission_rate = commission_rate
        self.min_commission = min_commission
        self.slippage = slippage
        
        # 重置状态
        self.reset()
    
    def reset(self):
        """重置交易执行器状态"""
        # 当前资金
        self.cash = self.initial_capital
        
        # 当前持仓
        self.position = 0
        
        # 当前持仓成本
        self.position_cost = 0.0
        
        # 最大资金占用
        self.max_capital_used = 0.0
        
        # 交易历史
        self.trade_history = []
        
        # 持仓历史
        self.position_history = []
    
    def execute_trade(self, time, price, trade_type, shares):
        """执行交易
        
        Args:
            time: 交易时间
            price: 交易价格
            trade_type: 交易类型，'BUY'或'SELL'
            shares: 交易数量
            
        Returns:
            bool: 交易是否成功
        """
        try:
            # 考虑滑点
            actual_price = price * (1 + self.slippage) if trade_type == 'BUY' else price * (1 - self.slippage)
            
            # 计算交易金额
            amount = actual_price * shares
            
            # 计算手续费
            commission = max(amount * self.commission_rate, self.min_commission)
            
            if trade_type == 'BUY':
                # 检查资金是否足够
                total_cost = amount + commission
                if total_cost > self.cash:
                    print(f"资金不足，无法买入: 需要 {total_cost:.2f}，可用 {self.cash:.2f}")
                    return False
                
                # 更新资金和持仓
                self.cash -= total_cost
                self.position += shares
                
                # 更新持仓成本
                if self.position > 0:
                    self.position_cost = (self.position_cost * (self.position - shares) + amount) / self.position
                else:
                    self.position_cost = 0.0
                    
            elif trade_type == 'SELL':
                # 检查持仓是否足够
                if shares > self.position:
                    print(f"持仓不足，无法卖出: 需要 {shares}，可用 {self.position}")
                    return False
                
                # 更新资金和持仓
                self.cash += amount - commission
                self.position -= shares
                
                # 更新持仓成本
                if self.position > 0:
                    # 持仓成本不变
                    pass
                else:
                    self.position_cost = 0.0
            
            # 记录交易
            self.trade_history.append({
                'time': time,
                'type': trade_type,
                'price': actual_price,
                'shares': shares,
                'amount': amount,
                'commission': commission,
                'cash': self.cash,
                'position': self.position
            })
            
            # 更新最大资金占用
            current_capital_used = self.initial_capital - self.cash
            if current_capital_used > self.max_capital_used:
                self.max_capital_used = current_capital_used
            
            return True
            
        except Exception as e:
            print(f"执行交易错误: {str(e)}")
            traceback.print_exc()
            return False
    
    def update_position_value(self, time, price):
        """更新持仓市值
        
        Args:
            time: 当前时间
            price: 当前价格
        """
        try:
            # 计算持仓市值
            market_value = self.position * price
            
            # 记录持仓历史
            self.position_history.append({
                'time': time,
                'shares': self.position,
                'cost': self.position_cost * self.position,
                'market_value': market_value
            })
            
        except Exception as e:
            print(f"更新持仓市值错误: {str(e)}")
            traceback.print_exc()
    
    def get_total_assets(self):
        """获取当前总资产
        
        Returns:
            float: 当前总资产
        """
        if not self.position_history:
            return self.cash
        
        # 获取最新持仓市值
        latest_position = self.position_history[-1]
        market_value = latest_position['market_value']
        
        return self.cash + market_value
    
    def get_total_profit(self):
        """获取总收益
        
        Returns:
            float: 总收益
        """
        return self.get_total_assets() - self.initial_capital
    
    def get_total_profit_rate(self):
        """获取总收益率
        
        Returns:
            float: 总收益率(%)
        """
        # 使用最大资金占用作为基准计算收益率
        base_capital = max(self.max_capital_used, self.initial_capital * 0.01)
        return (self.get_total_profit() / base_capital) * 100
    
    def get_trade_history(self):
        """获取交易历史
        
        Returns:
            list: 交易历史列表
        """
        return self.trade_history
    
    def get_position_history(self):
        """获取持仓历史
        
        Returns:
            list: 持仓历史列表
        """
        return self.position_history 