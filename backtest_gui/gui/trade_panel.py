#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
交易面板模块 - 用于显示交易记录和账户状态
"""
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QGroupBox, QTableWidget, QTableWidgetItem, QHeaderView,
    QSplitter, QTabWidget
)
from PyQt5.QtCore import Qt, QDateTime
from PyQt5.QtGui import QColor, QBrush
import traceback

class TradePanel(QWidget):
    """交易面板，显示交易记录和账户状态"""
    
    def __init__(self, parent=None):
        """初始化交易面板
        
        Args:
            parent: 父窗口
        """
        super().__init__(parent)
        
        # 保存交易记录
        self.trades = []
        
        # 保存配对交易记录
        self.paired_trades = {}
        
        # 保存交易执行器引用
        self.executor = None
        
        # 初始化UI
        self.init_ui()
        
    def init_ui(self):
        """初始化用户界面"""
        # 创建主布局
        main_layout = QVBoxLayout(self)
        
        # 创建分隔器
        splitter = QSplitter(Qt.Horizontal)
        main_layout.addWidget(splitter)
        
        # 创建账户状态面板
        account_panel = self._create_account_panel()
        splitter.addWidget(account_panel)
        
        # 创建交易记录标签页
        trade_tabs = QTabWidget()
        splitter.addWidget(trade_tabs)
        
        # 创建原始交易记录表格
        raw_trade_panel = self._create_raw_trade_panel()
        trade_tabs.addTab(raw_trade_panel, "原始交易")
        
        # 创建配对交易记录表格
        paired_trade_panel = self._create_paired_trade_panel()
        trade_tabs.addTab(paired_trade_panel, "配对交易")
        
        # 设置分隔器的初始比例
        splitter.setSizes([300, 700])
        
    def _create_account_panel(self):
        """创建账户状态面板"""
        group_box = QGroupBox("账户状态")
        layout = QVBoxLayout(group_box)
        
        # 最高资金占用
        self.max_capital_label = QLabel(f"最高资金占用: 0.00")
        layout.addWidget(self.max_capital_label)
        
        # 持仓数量
        self.position_label = QLabel(f"持仓数量: 0")
        layout.addWidget(self.position_label)
        
        # 持仓成本
        self.position_cost_label = QLabel(f"持仓成本: 0.0000")
        layout.addWidget(self.position_cost_label)
        
        # 持仓市值
        self.position_value_label = QLabel(f"持仓市值: 0.00")
        layout.addWidget(self.position_value_label)
        
        # 卖出的持仓收益
        self.sold_profit_label = QLabel(f"卖出持仓收益: 0.00")
        layout.addWidget(self.sold_profit_label)
        
        # 当前持仓收益
        self.current_profit_label = QLabel(f"当前持仓收益: 0.00")
        layout.addWidget(self.current_profit_label)
        
        # 总收益
        self.total_profit_label = QLabel(f"总收益: 0.00")
        layout.addWidget(self.total_profit_label)
        
        # 总收益率
        self.total_profit_rate_label = QLabel(f"总收益率: 0.00%")
        layout.addWidget(self.total_profit_rate_label)
        
        layout.addStretch(1)  # 添加弹性空间
        
        return group_box
        
    def _create_raw_trade_panel(self):
        """创建原始交易记录表格"""
        group_box = QGroupBox("交易记录")
        layout = QVBoxLayout(group_box)
        
        # 创建表格
        self.trade_table = QTableWidget()
        self.trade_table.setColumnCount(8)  # 增加档位级别列
        self.trade_table.setHorizontalHeaderLabels(["交易时间", "交易类型", "档位级别", "交易价格", "交易数量", "交易金额", "波段收益", "波段收益率"])
        
        # 设置表格属性
        self.trade_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.trade_table.verticalHeader().setVisible(False)
        self.trade_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.trade_table.setAlternatingRowColors(True)
        
        layout.addWidget(self.trade_table)
        
        return group_box
        
    def _create_paired_trade_panel(self):
        """创建配对交易记录表格"""
        group_box = QGroupBox("配对交易记录")
        layout = QVBoxLayout(group_box)
        
        # 创建表格
        self.paired_trade_table = QTableWidget()
        self.paired_trade_table.setColumnCount(14)
        self.paired_trade_table.setHorizontalHeaderLabels([
            "档位级别", "买入时间", "买入价格", "买入股数", "买入金额", 
            "卖出时间", "卖出价格", "卖出股数", "卖出金额", 
            "剩余股数", "卖出收益率", "波段收益", "波段收益率", "状态"
        ])
        
        # 设置表格属性
        self.paired_trade_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.paired_trade_table.verticalHeader().setVisible(False)
        self.paired_trade_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.paired_trade_table.setAlternatingRowColors(True)
        
        layout.addWidget(self.paired_trade_table)
        
        return group_box
        
    def add_trade(self, trade_time, trade_type, price, amount, trade_value, level=None, grid_type=None, band_profit=None, band_profit_rate=None, sell_profit_rate=None, remaining=None):
        """添加交易记录
        
        Args:
            trade_time: 交易时间
            trade_type: 交易类型（买入/卖出）
            price: 交易价格
            amount: 交易数量
            trade_value: 交易金额
            level: 档位级别（可选）
            grid_type: 网格类型（可选）
            band_profit: 波段收益（可选）
            band_profit_rate: 波段收益率（可选）
            sell_profit_rate: 卖出收益率（可选）
            remaining: 剩余股数（可选）
        """
        # 确保所有数值类型为float
        price = float(price)
        amount = float(amount)
        trade_value = float(trade_value)
        if band_profit is not None:
            band_profit = float(band_profit)
        if band_profit_rate is not None:
            band_profit_rate = float(band_profit_rate)
        if sell_profit_rate is not None:
            sell_profit_rate = float(sell_profit_rate)
        if remaining is not None:
            remaining = float(remaining)
            
        # 更新账户状态
        if trade_type == "买入":
            self.position += amount
            self.current_capital -= trade_value
            self.position_cost = (self.position_cost * (self.position - amount) + price * amount) / self.position if self.position > 0 else 0
            
            # 更新最高资金占用
            used_capital = self.initial_capital - self.current_capital
            if used_capital > self.max_capital_used:
                self.max_capital_used = used_capital
        else:  # 卖出
            self.position -= amount
            self.current_capital += trade_value
            if band_profit is not None:
                self.total_profit += band_profit
                self.sold_position_profit += band_profit
        
        # 添加到交易记录
        trade_record = {
            'time': trade_time,
            'type': trade_type,
            'price': price,
            'amount': amount,
            'value': trade_value,
            'level': level,
            'grid_type': grid_type,
            'band_profit': band_profit,
            'band_profit_rate': band_profit_rate,
            'sell_profit_rate': sell_profit_rate,
            'remaining': remaining
        }
        self.trades.append(trade_record)
        
        # 更新交易记录表格
        row = self.trade_table.rowCount()
        self.trade_table.insertRow(row)
        
        # 设置单元格内容
        self.trade_table.setItem(row, 0, QTableWidgetItem(trade_time.strftime("%Y-%m-%d %H:%M:%S")))
        
        type_item = QTableWidgetItem(trade_type)
        if trade_type == "买入":
            type_item.setForeground(QBrush(QColor("red")))
        else:
            type_item.setForeground(QBrush(QColor("green")))
        self.trade_table.setItem(row, 1, type_item)
        
        # 添加档位级别
        level_text = f"{level}" if level is not None else ""
        if grid_type:
            level_text += f" ({grid_type})"
        self.trade_table.setItem(row, 2, QTableWidgetItem(level_text))
        
        self.trade_table.setItem(row, 3, QTableWidgetItem(f"{price:.4f}"))
        self.trade_table.setItem(row, 4, QTableWidgetItem(f"{amount}"))
        self.trade_table.setItem(row, 5, QTableWidgetItem(f"{trade_value:.2f}"))
        
        if band_profit is not None:
            profit_item = QTableWidgetItem(f"{band_profit:.2f}")
            if band_profit >= 0:
                profit_item.setForeground(QBrush(QColor("red")))
            else:
                profit_item.setForeground(QBrush(QColor("green")))
            self.trade_table.setItem(row, 6, profit_item)
            
            if band_profit_rate is not None:
                rate_item = QTableWidgetItem(f"{band_profit_rate:.2f}%")
                if band_profit_rate >= 0:
                    rate_item.setForeground(QBrush(QColor("red")))
                else:
                    rate_item.setForeground(QBrush(QColor("green")))
                self.trade_table.setItem(row, 7, rate_item)
        
        # 滚动到最新行
        self.trade_table.scrollToItem(self.trade_table.item(row, 0))
        
        # 更新配对交易记录
        if level is not None:
            # 为每个买卖对创建唯一键，使用时间戳作为区分
            if trade_type == "买入":
                key = f"{level}_{grid_type}_{trade_time.timestamp()}"
                self.paired_trades[key] = {"buy": trade_record, "sell": None, "status": "进行中"}
            else:  # 卖出
                # 查找匹配的买入记录
                matched_key = None
                for k, v in self.paired_trades.items():
                    # 检查是否是同一档位且状态为进行中的买入记录
                    if (k.startswith(f"{level}_{grid_type}_") and 
                        v["buy"] is not None and 
                        v["sell"] is None and
                        v["status"] == "进行中"):
                        matched_key = k
                        break
                
                if matched_key:
                    self.paired_trades[matched_key]["sell"] = trade_record
                    self.paired_trades[matched_key]["status"] = "已完成"
            
            self._update_paired_trade_table()
        
        # 更新账户状态显示
        self.update_account_display()
        
    def _update_paired_trade_table(self):
        """更新配对交易表格"""
        # 清空表格
        self.paired_trade_table.setRowCount(0)
        
        # 添加配对交易记录
        for key, pair in self.paired_trades.items():
            row = self.paired_trade_table.rowCount()
            self.paired_trade_table.insertRow(row)
            
            buy_record = pair.get("buy")
            sell_record = pair.get("sell")
            
            # 档位级别
            level_parts = key.split("_")
            level = level_parts[0]
            grid_type = level_parts[1] if len(level_parts) > 1 else ""
            level_text = f"{level}" if level else ""
            if grid_type:
                level_text += f" ({grid_type})"
            self.paired_trade_table.setItem(row, 0, QTableWidgetItem(level_text))
            
            # 买入信息
            if buy_record:
                self.paired_trade_table.setItem(row, 1, QTableWidgetItem(buy_record['time'].strftime("%Y-%m-%d %H:%M:%S")))
                self.paired_trade_table.setItem(row, 2, QTableWidgetItem(f"{buy_record['price']:.4f}"))
                self.paired_trade_table.setItem(row, 3, QTableWidgetItem(f"{buy_record['amount']}"))
                self.paired_trade_table.setItem(row, 4, QTableWidgetItem(f"{buy_record['value']:.2f}"))
            
            # 卖出信息
            if sell_record:
                self.paired_trade_table.setItem(row, 5, QTableWidgetItem(sell_record['time'].strftime("%Y-%m-%d %H:%M:%S")))
                self.paired_trade_table.setItem(row, 6, QTableWidgetItem(f"{sell_record['price']:.4f}"))
                self.paired_trade_table.setItem(row, 7, QTableWidgetItem(f"{sell_record['amount']}"))
                self.paired_trade_table.setItem(row, 8, QTableWidgetItem(f"{sell_record['value']:.2f}"))
                
                # 剩余股数
                remaining = sell_record.get('remaining', buy_record['amount'] - sell_record['amount'] if buy_record else 0)
                self.paired_trade_table.setItem(row, 9, QTableWidgetItem(f"{remaining}"))
                
                # 卖出收益率
                if sell_record.get('sell_profit_rate') is not None:
                    sell_profit_rate_item = QTableWidgetItem(f"{sell_record['sell_profit_rate']:.2f}%")
                    if sell_record['sell_profit_rate'] >= 0:
                        sell_profit_rate_item.setForeground(QBrush(QColor("red")))
                    else:
                        sell_profit_rate_item.setForeground(QBrush(QColor("green")))
                    self.paired_trade_table.setItem(row, 10, sell_profit_rate_item)
                
                # 波段收益和收益率
                if sell_record['band_profit'] is not None:
                    profit_item = QTableWidgetItem(f"{sell_record['band_profit']:.2f}")
                    if sell_record['band_profit'] >= 0:
                        profit_item.setForeground(QBrush(QColor("red")))
                    else:
                        profit_item.setForeground(QBrush(QColor("green")))
                    self.paired_trade_table.setItem(row, 11, profit_item)
                    
                    if sell_record['band_profit_rate'] is not None:
                        rate_item = QTableWidgetItem(f"{sell_record['band_profit_rate']:.2f}%")
                        if sell_record['band_profit_rate'] >= 0:
                            rate_item.setForeground(QBrush(QColor("red")))
                        else:
                            rate_item.setForeground(QBrush(QColor("green")))
                        self.paired_trade_table.setItem(row, 12, rate_item)
            
            # 状态
            status_item = QTableWidgetItem(pair["status"])
            if pair["status"] == "已完成":
                status_item.setForeground(QBrush(QColor("blue")))
            else:
                status_item.setForeground(QBrush(QColor("orange")))
            self.paired_trade_table.setItem(row, 13, status_item)
        
    def update_position_value(self, current_price):
        """更新持仓市值
        
        Args:
            current_price: 当前市场价格
        """
        position_value = self.position * current_price
        self.position_value_label.setText(f"持仓市值: {position_value:.2f}")
        
        # 更新当前持仓收益
        if self.position > 0 and self.position_cost > 0:
            self.current_position_profit = position_value - self.position * self.position_cost
            self.current_profit_label.setText(f"当前持仓收益: {self.current_position_profit:.2f}")
        
        # 更新总资产和总收益率
        total_assets = self.current_capital + position_value
        total_profit = total_assets - self.initial_capital
        self.total_profit_label.setText(f"总收益: {total_profit:.2f}")
        
        # 更新总收益率
        if self.max_capital_used > 0:
            profit_rate = (total_profit / self.max_capital_used) * 100
        else:
            profit_rate = 0.0
        self.total_profit_rate_label.setText(f"总收益率: {profit_rate:.2f}%")
        
    def update_account_display(self):
        """更新账户状态显示"""
        if not self.executor:
            return
            
        # 获取当前价格
        current_price = None
        if self.trades:
            current_price = self.trades[-1].get('price', None)
            
        # 获取账户摘要
        summary = self.executor.get_account_summary(current_price)
        
        # 更新账户状态面板
        self.max_capital_label.setText(f"最高资金占用: {summary['max_capital_used']:.2f}")
        self.position_label.setText(f"持仓数量: {summary['position']:.1f}")
        self.position_cost_label.setText(f"持仓成本: {summary['position_cost']:.4f}")
        self.position_value_label.setText(f"持仓市值: {summary['position_value']:.2f}")
        
        # 计算已实现收益（总收益 - 持仓收益）
        position_profit = summary['position_value'] - summary['position'] * summary['position_cost']
        realized_profit = summary['total_profit'] - position_profit
        
        self.sold_profit_label.setText(f"卖出持仓收益: {realized_profit:.2f}")
        self.current_profit_label.setText(f"当前持仓收益: {position_profit:.2f}")
        self.total_profit_label.setText(f"总收益: {summary['total_profit']:.2f}")
        
        # 使用基于最大资金占用的收益率
        self.total_profit_rate_label.setText(f"总收益率: {summary['max_capital_profit_rate']:.2f}%")
        
    def clear(self):
        """清空交易面板数据"""
        # 清空交易记录表格
        if self.trade_table:
            self.trade_table.setRowCount(0)
        
        if self.paired_trade_table:
            self.paired_trade_table.setRowCount(0)
        
        # 重置账户状态
        self.current_capital = self.initial_capital
        self.total_profit = 0.0
        self.position = 0
        self.position_cost = 0.0
        self.max_capital_used = 0.0
        self.sold_position_profit = 0.0
        self.current_position_profit = 0.0
        self.trades = []
        self.paired_trades = {}
        
        # 更新显示
        self.update_account_display()
        if self.position_value_label:
            self.position_value_label.setText("持仓市值: 0.00")
        if self.total_profit_rate_label:
            self.total_profit_rate_label.setText("总收益率: 0.00%")

    def update_account_status(self, executor):
        """更新账户状态面板
        
        Args:
            executor: 交易执行器实例
        """
        try:
            # 获取账户状态
            max_capital_used = executor.max_capital_used
            total_shares = executor.total_shares
            avg_cost = executor.avg_cost
            current_price = executor.current_price if executor.current_price else 0.0
            total_value = executor.total_value
            
            # 计算总盈亏
            total_profit = executor.total_profit
            
            # 计算持仓盈亏
            position_profit = executor.position_profit
            
            # 计算总收益率 (不使用初始资金，而是使用最大资金占用作为基准)
            if max_capital_used > 0:
                profit_rate = (total_profit / max_capital_used) * 100
            else:
                profit_rate = 0.0
            
            # 更新标签文本
            self.max_capital_label.setText(f"最高资金占用: {max_capital_used:.2f}")
            self.position_label.setText(f"持仓数量: {total_shares:.1f}")
            self.position_cost_label.setText(f"持仓成本: {avg_cost:.4f}")
            self.position_value_label.setText(f"持仓市值: {total_value:.2f}")
            self.sold_profit_label.setText(f"卖出持仓收益: {total_profit - position_profit:.2f}")
            self.current_profit_label.setText(f"当前持仓收益: {position_profit:.2f}")
            self.total_profit_label.setText(f"总收益: {total_profit:.2f}")
            self.total_profit_rate_label.setText(f"总收益率: {profit_rate:.2f}%")
            
        except Exception as e:
            print(f"更新账户状态面板错误: {str(e)}")
            traceback.print_exc() 