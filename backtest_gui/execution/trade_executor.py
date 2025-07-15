#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
交易执行模块 - 执行交易信号并维护账户状态
"""
from datetime import datetime
import pandas as pd


class TradeExecutor:
    """交易执行器，负责执行交易信号并维护账户状态"""
    
    def __init__(self, initial_capital=100000.0):
        """初始化交易执行器
        
        Args:
            initial_capital: 初始资金
        """
        # 账户状态
        self.initial_capital = initial_capital
        self.cash = initial_capital  # 现金
        self.position = 0  # 持仓数量
        self.position_cost = 0.0  # 持仓成本
        
        # 交易记录
        self.trades = []
        
        # 波段收益记录
        self.band_profits = []  # 每个波段的收益
        
        # 最大资金占用
        self.max_capital_used = 0.0
        
    @property
    def current_capital(self):
        """获取当前现金，与 cash 属性同义
        
        Returns:
            float: 当前现金
        """
        return self.cash
        
    def reset(self, initial_capital=None):
        """重置交易执行器
        
        Args:
            initial_capital: 新的初始资金，如果为None则使用之前的初始资金
        """
        if initial_capital is not None:
            self.initial_capital = initial_capital
            
        self.cash = self.initial_capital
        self.position = 0
        self.position_cost = 0.0
        self.trades = []
        self.band_profits = []
        
        # 重置最大资金占用
        self.max_capital_used = 0.0
        
    def execute_signal(self, signal):
        """执行交易信号
        
        Args:
            signal: 交易信号字典，包含 {
                'time': 交易时间,
                'type': 买入/卖出,
                'price': 交易价格,
                'amount': 交易数量,
                'level': 网格级别
            }
            
        Returns:
            dict: 交易记录，包含执行后的结果，如波段收益等
        """
        time = signal['time']
        trade_type = signal['type']
        price = signal['price']
        amount = signal['amount']
        level = signal['level']
        grid_type = signal.get('grid_type', 'NORMAL')
        
        trade_value = price * amount  # 交易金额
        band_profit = None
        band_profit_rate = None
        sell_profit_rate = None
        remaining = 0
        
        if trade_type == '买入':
            # 检查资金是否足够
            if self.cash < trade_value:
                # 资金不足，调整买入数量
                adjusted_amount = int(self.cash / price)
                if adjusted_amount <= 0:
                    return None  # 资金不足，无法买入
                amount = adjusted_amount
                trade_value = price * amount
                
            # 执行买入
            self.cash -= trade_value
            
            # 更新持仓成本
            old_position_value = self.position * self.position_cost
            new_position_value = amount * price
            self.position += amount
            
            if self.position > 0:
                self.position_cost = (old_position_value + new_position_value) / self.position
            else:
                self.position_cost = 0
                
            # 更新最大资金占用
            total_invested = (self.initial_capital - self.cash)
            if total_invested > self.max_capital_used:
                self.max_capital_used = total_invested
                
        elif trade_type == '卖出':
            # 检查持仓是否足够
            if self.position < amount:
                # 持仓不足，调整卖出数量
                amount = self.position
                if amount <= 0:
                    return None  # 没有持仓，无法卖出
                trade_value = price * amount
            
            # 查找相应档位的买入记录
            buy_record = None
            for trade in reversed(self.trades):
                if (trade['level'] == level and 
                    trade['grid_type'] == grid_type and 
                    trade['type'] == '买入'):
                    buy_record = trade
                    break
            
            if buy_record:
                # 计算卖出收益率 = (卖出价格 / 买入价格 - 1) * 100%
                buy_price = buy_record['price']
                sell_profit_rate = (price / buy_price - 1) * 100
                
                # 计算剩余股数
                remaining = buy_record['amount'] - amount
                
                # 计算波段收益
                buy_cost = buy_price * buy_record['amount']  # 买入总成本
                sell_value = price * amount  # 卖出价值
                remaining_value = remaining * price  # 剩余持仓按当前价格计算的价值
                
                # 波段收益 = 卖出价值 + 剩余持仓价值 - 买入成本
                band_profit = sell_value + remaining_value - buy_cost
                
                # 波段收益率 = (卖出价值 + 剩余持仓价值 - 买入成本) / 买入成本 * 100%
                if buy_cost > 0:
                    band_profit_rate = (band_profit / buy_cost) * 100
            else:
                # 找不到对应买入记录，使用平均持仓成本计算
                buy_cost = self.position_cost * amount  # 买入成本
                sell_value = price * amount  # 卖出价值
                band_profit = sell_value - buy_cost  # 波段收益
                
                # 计算波段收益率 = (卖出价值 - 买入成本) / 买入成本 * 100%
                if buy_cost > 0:
                    band_profit_rate = (band_profit / buy_cost) * 100
                    sell_profit_rate = band_profit_rate
            
            # 执行卖出
            self.cash += trade_value
            self.position -= amount
            
            # 记录波段收益
            self.band_profits.append({
                'time': time,
                'level': level,
                'grid_type': grid_type,
                'buy_cost': buy_cost if buy_record else self.position_cost * amount,
                'sell_value': sell_value,
                'profit': band_profit,
                'profit_rate': band_profit_rate,
                'sell_profit_rate': sell_profit_rate,
                'remaining': remaining
            })
            
        # 创建交易记录
        trade = {
            'time': time,
            'type': trade_type,
            'price': price,
            'amount': amount,
            'value': trade_value,
            'band_profit': band_profit,
            'band_profit_rate': band_profit_rate,
            'sell_profit_rate': sell_profit_rate,
            'remaining': remaining,
            'level': level,
            'grid_type': grid_type,
            'cash': self.cash,
            'position': self.position,
            'position_cost': self.position_cost
        }
        
        # 添加到交易记录
        self.trades.append(trade)
        
        return trade
    
    def get_total_assets(self, current_price):
        """获取当前总资产
        
        Args:
            current_price: 当前市场价格
            
        Returns:
            float: 总资产 = 现金 + 持仓市值
        """
        position_value = self.position * current_price
        return self.cash + position_value
        
    def get_account_summary(self, current_price=None):
        """获取账户摘要
        
        Args:
            current_price: 当前市场价格，用于计算持仓市值
            
        Returns:
            dict: 账户摘要
        """
        # 如果未提供当前价格，使用最后一次交易价格或持仓成本
        if current_price is None:
            if len(self.trades) > 0:
                current_price = self.trades[-1]['price']
            else:
                current_price = self.position_cost
                
        # 计算持仓市值
        position_value = self.position * current_price
        
        # 计算总资产
        total_assets = self.cash + position_value
        
        # 计算总收益和收益率
        total_profit = total_assets - self.initial_capital
        
        # 基于初始资金的收益率
        if self.initial_capital > 0:
            profit_rate = (total_profit / self.initial_capital) * 100
        else:
            profit_rate = 0
        
        # 基于最大资金占用的收益率
        if self.max_capital_used > 0:
            max_capital_profit_rate = (total_profit / self.max_capital_used) * 100
        else:
            max_capital_profit_rate = 0
            
        # 统计波段收益
        total_band_profit = sum(band['profit'] for band in self.band_profits) if self.band_profits else 0
        
        return {
            'cash': self.cash,
            'position': self.position,
            'position_cost': self.position_cost,
            'position_value': position_value,
            'total_assets': total_assets,
            'total_profit': total_profit,
            'profit_rate': profit_rate,
            'max_capital_used': self.max_capital_used,
            'max_capital_profit_rate': max_capital_profit_rate,
            'total_band_profit': total_band_profit,
            'trade_count': len(self.trades),
            'buy_count': sum(1 for trade in self.trades if trade['type'] == '买入'),
            'sell_count': sum(1 for trade in self.trades if trade['type'] == '卖出')
        }
        
    def get_trade_history(self):
        """获取交易历史
        
        Returns:
            DataFrame: 交易历史数据框
        """
        if not self.trades:
            return pd.DataFrame()
            
        return pd.DataFrame(self.trades)
        
    def get_band_profits(self):
        """获取波段收益列表
        
        Returns:
            DataFrame: 波段收益数据框
        """
        if not self.band_profits:
            return pd.DataFrame()
            
        return pd.DataFrame(self.band_profits) 