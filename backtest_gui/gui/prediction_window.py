#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
预测窗口模块 - 基于已回测的基金预测未来收益
"""
import sys
import pandas as pd
import numpy as np
import traceback
from PyQt5.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QLabel, QComboBox, QPushButton, QDateEdit, 
    QGroupBox, QSplitter, QTableWidget, QTableWidgetItem, 
    QHeaderView, QMessageBox, QDoubleSpinBox, QFormLayout, QStatusBar
)
from PyQt5.QtCore import Qt, QDateTime, QDate
from PyQt5.QtGui import QIcon, QColor, QBrush

from backtest_gui.gui.chart_widget import ChartWidget
from backtest_gui.utils.backtest_data_manager import BacktestDataManager
from backtest_gui.utils.db_connector import DBConnector


class PredictionWindow(QMainWindow):
    """基于已回测基金预测未来收益的窗口"""
    
    def __init__(self, parent=None):
        """初始化预测窗口
        
        Args:
            parent: 父窗口对象
        """
        super().__init__(parent)
        
        # 初始化数据管理器
        self.data_manager = BacktestDataManager()
        
        # 确保数据库连接池已初始化
        if not hasattr(self.data_manager, 'db_connector') or self.data_manager.db_connector is None:
            self.data_manager.db_connector = DBConnector()
            self.data_manager.db_connector.init_connection_pool()
        
        # 存储加载的回测数据
        self.loaded_backtest_data = None
        self.backtest_results = None
        self.stock_code = None
        self.current_price = None
        
        # 初始化UI
        self.init_ui()
        
        # 创建状态栏
        self.statusBar = self.statusBar()
        self.statusBar.showMessage("就绪")
        
        # 加载基金列表
        self.load_stock_list()
        
    def init_ui(self):
        """初始化用户界面"""
        try:
            # 设置窗口基本属性
            self.setWindowTitle("波段交易未来收益预测")
            self.setMinimumSize(1000, 800)
            
            # 创建中央部件
            central_widget = QWidget()
            self.setCentralWidget(central_widget)
            
            # 创建主布局
            main_layout = QVBoxLayout(central_widget)
            
            # 创建顶部控制面板
            control_panel = self.create_control_panel()
            main_layout.addWidget(control_panel)
            
            # 创建分隔器，分隔图表和预测面板
            splitter = QSplitter(Qt.Vertical)
            main_layout.addWidget(splitter, 1)  # 1表示可伸缩
            
            # 添加图表控件
            self.chart_widget = ChartWidget()
            splitter.addWidget(self.chart_widget)
            
            # 添加预测结果表格
            prediction_panel = self.create_prediction_panel()
            splitter.addWidget(prediction_panel)
            
            # 设置分隔器的初始大小比例
            splitter.setSizes([500, 300])
            
            # 创建状态栏
            self.setStatusBar(QStatusBar())
            
        except Exception as e:
            print(f"初始化预测窗口UI错误: {str(e)}")
            traceback.print_exc()
    
    def create_control_panel(self):
        """创建顶部控制面板"""
        panel = QGroupBox("预测控制")
        layout = QHBoxLayout(panel)
        
        # 左侧表单布局 - 基金选择和回测选择
        left_form_layout = QFormLayout()
        
        # 基金选择
        self.fund_combo = QComboBox()
        self.fund_combo.currentTextChanged.connect(self.on_stock_changed)
        left_form_layout.addRow("选择基金:", self.fund_combo)
        
        # 回测结果选择
        self.backtest_combo = QComboBox()
        self.backtest_combo.currentIndexChanged.connect(self.on_backtest_changed)
        left_form_layout.addRow("选择回测:", self.backtest_combo)
        
        # 右侧表单布局 - 预测设置
        right_form_layout = QFormLayout()
        
        # 预测日期
        self.predict_date_edit = QDateEdit(QDateTime.currentDateTime().addMonths(1).date())
        self.predict_date_edit.setCalendarPopup(True)
        right_form_layout.addRow("预测日期:", self.predict_date_edit)
        
        # 预测净值
        self.predict_nav_spin = QDoubleSpinBox()
        self.predict_nav_spin.setRange(0.1, 10.0)
        self.predict_nav_spin.setDecimals(4)
        self.predict_nav_spin.setSingleStep(0.01)
        self.predict_nav_spin.setValue(1.0)
        right_form_layout.addRow("预测净值:", self.predict_nav_spin)
        
        # 添加左右表单布局
        form_widget_left = QWidget()
        form_widget_left.setLayout(left_form_layout)
        layout.addWidget(form_widget_left, 1)
        
        form_widget_right = QWidget()
        form_widget_right.setLayout(right_form_layout)
        layout.addWidget(form_widget_right, 1)
        
        # 预测按钮
        self.predict_button = QPushButton("计算预测")
        self.predict_button.clicked.connect(self.calculate_prediction)
        layout.addWidget(self.predict_button)
        
        return panel
    
    def create_prediction_panel(self):
        """创建预测结果面板"""
        panel = QGroupBox("预测结果")
        layout = QVBoxLayout(panel)
        
        # 创建基本信息显示区
        info_panel = QGroupBox("基金信息")
        info_layout = QHBoxLayout(info_panel)
        
        # 左侧基本信息
        left_info_layout = QFormLayout()
        self.fund_code_label = QLabel("--")
        self.last_price_label = QLabel("--")
        self.position_amount_label = QLabel("--")
        self.position_cost_label = QLabel("--")
        
        left_info_layout.addRow("基金代码:", self.fund_code_label)
        left_info_layout.addRow("最新价格:", self.last_price_label)
        left_info_layout.addRow("持仓数量:", self.position_amount_label)
        left_info_layout.addRow("持仓成本:", self.position_cost_label)
        
        # 右侧预测信息
        right_info_layout = QFormLayout()
        self.predict_price_label = QLabel("--")
        self.predict_profit_label = QLabel("--")
        self.predict_profit_rate_label = QLabel("--")
        self.total_profit_label = QLabel("--")
        
        right_info_layout.addRow("预测价格:", self.predict_price_label)
        right_info_layout.addRow("预测收益:", self.predict_profit_label)
        right_info_layout.addRow("预测收益率:", self.predict_profit_rate_label)
        right_info_layout.addRow("总预测收益:", self.total_profit_label)
        
        left_info_widget = QWidget()
        left_info_widget.setLayout(left_info_layout)
        info_layout.addWidget(left_info_widget)
        
        right_info_widget = QWidget()
        right_info_widget.setLayout(right_info_layout)
        info_layout.addWidget(right_info_widget)
        
        layout.addWidget(info_panel)
        
        # 创建波段收益预测表格
        table_panel = QGroupBox("波段收益预测")
        table_layout = QVBoxLayout(table_panel)
        
        self.prediction_table = QTableWidget()
        self.prediction_table.setColumnCount(16)
        self.prediction_table.setHorizontalHeaderLabels([
            "档位级别", 
            "买入时间", "买入价格", "买入数量", "买入金额",
            "卖出时间", "卖出价格", "卖出数量", "卖出金额",
            "剩余数量", "原波段收益", "原波段收益率", 
            "预测价格", "预测波段收益", "预测波段收益率", "收益变化"
        ])
        
        # 设置表格属性
        self.prediction_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.prediction_table.verticalHeader().setVisible(False)
        self.prediction_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.prediction_table.setAlternatingRowColors(True)
        
        table_layout.addWidget(self.prediction_table)
        
        layout.addWidget(table_panel, 1)  # 1表示可伸缩
        
        return panel
    
    def load_stock_list(self):
        """加载已回测的基金列表"""
        try:
            # 清空基金列表
            self.fund_combo.clear()
            
            # 获取所有已回测的基金代码
            conn = self.data_manager.db_connector.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                """
                SELECT DISTINCT stock_code
                FROM backtest_results
                ORDER BY stock_code
                """
            )
            
            results = cursor.fetchall()
            
            # 添加到下拉列表
            for row in results:
                self.fund_combo.addItem(row[0])
                
            # 释放连接
            self.data_manager.db_connector.release_connection(conn)
            
            # 如果有基金，选择第一个
            if self.fund_combo.count() > 0:
                self.fund_combo.setCurrentIndex(0)
                # 触发基金变更事件
                self.on_stock_changed(self.fund_combo.currentText())
                
        except Exception as e:
            print(f"加载基金列表错误: {str(e)}")
            traceback.print_exc()
            QMessageBox.warning(self, "加载错误", f"加载基金列表失败: {str(e)}")
    
    def on_stock_changed(self, stock_code):
        """基金代码变更处理"""
        if not stock_code:
            return
            
        self.stock_code = stock_code
        self.backtest_results = None
        self.loaded_backtest_data = None
        
        # 加载该基金的回测结果
        try:
            results = self.data_manager.load_backtest_results(stock_code)
            
            # 清空并添加到回测选择下拉框
            self.backtest_combo.clear()
            
            for result in results:
                start_date = result['start_date'].strftime("%Y-%m-%d")
                end_date = result['end_date'].strftime("%Y-%m-%d")
                backtest_time = result['backtest_time'].strftime("%Y-%m-%d %H:%M")
                display_text = f"{start_date} 至 {end_date} (回测时间: {backtest_time})"
                self.backtest_combo.addItem(display_text, result['id'])
                
            # 清空图表和预测表格
            self.chart_widget.clear()
            self.prediction_table.setRowCount(0)
            
            # 清空信息标签
            self.reset_info_labels()
            
            if self.backtest_combo.count() > 0:
                self.backtest_combo.setCurrentIndex(0)
            else:
                QMessageBox.information(self, "无回测数据", f"基金 {stock_code} 没有回测记录。")
                
        except Exception as e:
            print(f"加载回测结果错误: {str(e)}")
            traceback.print_exc()
            QMessageBox.warning(self, "加载错误", f"加载回测结果失败: {str(e)}")
    
    def on_backtest_changed(self, index):
        """回测选择变更处理"""
        if index < 0:
            return
            
        # 获取选中的回测ID
        backtest_id = self.backtest_combo.itemData(index)
        if not backtest_id:
            return
            
        # 加载回测数据
        try:
            self.loaded_backtest_data = self.data_manager.load_backtest_data(backtest_id)
            self.backtest_results = self.loaded_backtest_data['results']
            
            # 获取股票代码和最新价格
            self.stock_code = self.backtest_results['stock_code']
            
            # 获取最后一个价格作为当前价格
            if self.loaded_backtest_data['position'] and self.loaded_backtest_data['position']['last_price']:
                self.current_price = self.loaded_backtest_data['position']['last_price']
            else:
                # 如果没有持仓信息，尝试从交易记录中找到最后价格
                if self.loaded_backtest_data['trades']:
                    self.current_price = self.loaded_backtest_data['trades'][-1]['price']
                else:
                    self.current_price = 0.0
                    
            # 更新基本信息
            self.update_info_labels()
            
            # 更新图表
            if self.loaded_backtest_data['nav_data'] is not None:
                self.chart_widget.update_nav_chart(self.loaded_backtest_data['nav_data'])
                
            # 默认预测值设为当前价格
            if self.current_price > 0:
                self.predict_nav_spin.setValue(self.current_price)
                
            # 默认预测日期设为回测结束日期后一个月
            if self.backtest_results['end_date']:
                end_date = self.backtest_results['end_date']
                predict_date = QDate(end_date.year, end_date.month + 1, end_date.day)
                self.predict_date_edit.setDate(predict_date)
                
            # 自动加载波段回测记录
            if self.loaded_backtest_data and 'paired_trades' in self.loaded_backtest_data:
                self.analyze_band_profits(self.current_price)
                
            # 设置状态
            self.statusBar.showMessage(f"已加载基金 {self.stock_code} 的回测数据")
                
        except Exception as e:
            print(f"加载回测详情错误: {str(e)}")
            traceback.print_exc()
            QMessageBox.warning(self, "加载错误", f"加载回测详情失败: {str(e)}")
    
    def reset_info_labels(self):
        """重置信息标签"""
        self.fund_code_label.setText("--")
        self.last_price_label.setText("--")
        self.position_amount_label.setText("--")
        self.position_cost_label.setText("--")
        
        self.predict_price_label.setText("--")
        self.predict_profit_label.setText("--")
        self.predict_profit_rate_label.setText("--")
        self.total_profit_label.setText("--")
    
    def update_info_labels(self):
        """更新基本信息标签"""
        if not self.loaded_backtest_data:
            self.reset_info_labels()
            return
            
        # 更新基金信息
        self.fund_code_label.setText(self.stock_code)
        self.last_price_label.setText(f"{self.current_price:.4f}")
        
        # 更新持仓信息
        position = self.loaded_backtest_data['position']
        if position:
            self.position_amount_label.setText(f"{position['position_amount']}")
            self.position_cost_label.setText(f"{position['position_cost']:.4f}")
        else:
            self.position_amount_label.setText("0")
            self.position_cost_label.setText("0.0000")
    
    def calculate_prediction(self):
        """计算预测收益"""
        if not self.loaded_backtest_data:
            QMessageBox.warning(self, "无数据", "请先加载回测数据")
            return
            
        # 获取预测参数
        predict_nav = float(self.predict_nav_spin.value())
        
        # 清空预测表格
        self.prediction_table.setRowCount(0)
        
        try:
            # 预测总收益和总收益率
            position = self.loaded_backtest_data.get('position')
            if not position or position.get('position_amount', 0) <= 0:
                QMessageBox.information(self, "无持仓", "当前回测结束时没有持仓，无法进行预测")
                return
                
            # 计算持仓收益
            position_amount = float(position.get('position_amount', 0))
            position_cost = float(position.get('position_cost', 0))
            current_price = float(self.current_price)
            position_value_current = position_amount * current_price
            position_value_predict = position_amount * predict_nav
            
            # 更新预测信息
            self.predict_price_label.setText(f"{predict_nav:.4f}")
            
            predict_profit = position_amount * (predict_nav - current_price)
            self.predict_profit_label.setText(f"{predict_profit:.2f}")
            
            if current_price > 0:
                predict_profit_rate = (predict_nav / current_price - 1) * 100
                self.predict_profit_rate_label.setText(f"{predict_profit_rate:.2f}%")
            else:
                self.predict_profit_rate_label.setText("--")
                
            # 总收益 = 已实现收益 + 预测收益
            total_profit = float(self.backtest_results.get('total_profit', 0)) + predict_profit
            self.total_profit_label.setText(f"{total_profit:.2f}")
            
            # 分析波段收益
            self.analyze_band_profits(predict_nav)
            
        except Exception as e:
            print(f"计算预测收益错误: {str(e)}")
            traceback.print_exc()
            QMessageBox.warning(self, "计算错误", f"计算预测收益失败: {str(e)}")
    
    def analyze_band_profits(self, predict_nav):
        """分析各个波段的收益预测
        
        Args:
            predict_nav: 预测净值
        """
        if not self.loaded_backtest_data or 'paired_trades' not in self.loaded_backtest_data:
            return
            
        paired_trades = self.loaded_backtest_data.get('paired_trades', {})
        if not paired_trades:
            return
            
        # 清空预测表格
        self.prediction_table.setRowCount(0)
            
        # 遍历所有配对交易记录
        row = 0
        for key, pair in paired_trades.items():
            try:
                buy_record = pair.get('buy')
                sell_record = pair.get('sell')
                
                # 只处理有买入记录的配对
                if not buy_record:
                    continue
                    
                # 获取原始数据
                level = buy_record.get('level')
                grid_type = buy_record.get('grid_type')
                buy_time = buy_record.get('time')
                buy_price = float(buy_record.get('price', 0))
                buy_amount = float(buy_record.get('amount', 0))
                buy_value = float(buy_record.get('value', 0))
                
                # 计算剩余数量、原始波段收益和收益率
                remaining = buy_amount
                original_band_profit = 0.0
                original_band_profit_rate = 0.0
                sell_time = None
                sell_price = 0.0
                sell_amount = 0.0
                sell_value = 0.0
                
                if sell_record:
                    sell_time = sell_record.get('time')
                    sell_price = float(sell_record.get('price', 0))
                    sell_amount = float(sell_record.get('amount', 0))
                    sell_value = float(sell_record.get('value', 0))
                    remaining = buy_amount - sell_amount
                    original_band_profit = float(sell_record.get('band_profit', 0.0))
                    original_band_profit_rate = float(sell_record.get('band_profit_rate', 0.0))
                
                # 如果没有剩余，跳过
                if remaining <= 0:
                    continue
                    
                # 确保所有数据都是float类型
                predict_nav = float(predict_nav)
                buy_price = float(buy_price)
                remaining = float(remaining)
                
                # 计算新的波段收益
                # 原收益 + 剩余股数 * (预测价格 - 买入价格)
                predicted_band_profit = original_band_profit + remaining * (predict_nav - buy_price)
                
                # 计算新的波段收益率
                # (原卖出金额 + 剩余股数*预测价格 - 买入金额) / 买入金额 * 100%
                if buy_value > 0:
                    predicted_band_profit_rate = ((sell_value + remaining * predict_nav - buy_value) / buy_value) * 100
                else:
                    predicted_band_profit_rate = 0.0
                
                # 收益变化
                profit_change = predicted_band_profit - original_band_profit
                
                # 添加到表格
                self.prediction_table.insertRow(row)
                
                # 档位级别
                level_text = f"{level}" if level is not None else ""
                if grid_type:
                    level_text += f" ({grid_type})"
                self.prediction_table.setItem(row, 0, QTableWidgetItem(level_text))
                
                # 买入信息
                time_str = buy_time.strftime("%Y-%m-%d %H:%M") if hasattr(buy_time, 'strftime') else str(buy_time)
                self.prediction_table.setItem(row, 1, QTableWidgetItem(time_str))
                self.prediction_table.setItem(row, 2, QTableWidgetItem(f"{buy_price:.4f}"))
                self.prediction_table.setItem(row, 3, QTableWidgetItem(f"{buy_amount}"))
                self.prediction_table.setItem(row, 4, QTableWidgetItem(f"{buy_value:.2f}"))
                
                # 卖出信息
                if sell_record:
                    sell_time_str = sell_time.strftime("%Y-%m-%d %H:%M") if hasattr(sell_time, 'strftime') else str(sell_time)
                    self.prediction_table.setItem(row, 5, QTableWidgetItem(sell_time_str))
                    self.prediction_table.setItem(row, 6, QTableWidgetItem(f"{sell_price:.4f}"))
                    self.prediction_table.setItem(row, 7, QTableWidgetItem(f"{sell_amount}"))
                    self.prediction_table.setItem(row, 8, QTableWidgetItem(f"{sell_value:.2f}"))
                else:
                    self.prediction_table.setItem(row, 5, QTableWidgetItem("--"))
                    self.prediction_table.setItem(row, 6, QTableWidgetItem("--"))
                    self.prediction_table.setItem(row, 7, QTableWidgetItem("--"))
                    self.prediction_table.setItem(row, 8, QTableWidgetItem("--"))
                
                # 剩余数量
                self.prediction_table.setItem(row, 9, QTableWidgetItem(f"{remaining}"))
                
                # 原波段收益
                original_profit_item = QTableWidgetItem(f"{original_band_profit:.2f}")
                if original_band_profit >= 0:
                    original_profit_item.setForeground(QBrush(QColor("red")))
                else:
                    original_profit_item.setForeground(QBrush(QColor("green")))
                self.prediction_table.setItem(row, 10, original_profit_item)
                
                # 原波段收益率
                original_rate_item = QTableWidgetItem(f"{original_band_profit_rate:.2f}%")
                if original_band_profit_rate >= 0:
                    original_rate_item.setForeground(QBrush(QColor("red")))
                else:
                    original_rate_item.setForeground(QBrush(QColor("green")))
                self.prediction_table.setItem(row, 11, original_rate_item)
                
                # 预测价格
                self.prediction_table.setItem(row, 12, QTableWidgetItem(f"{predict_nav:.4f}"))
                
                # 预测波段收益
                predicted_profit_item = QTableWidgetItem(f"{predicted_band_profit:.2f}")
                if predicted_band_profit >= 0:
                    predicted_profit_item.setForeground(QBrush(QColor("red")))
                else:
                    predicted_profit_item.setForeground(QBrush(QColor("green")))
                self.prediction_table.setItem(row, 13, predicted_profit_item)
                
                # 预测波段收益率
                predicted_rate_item = QTableWidgetItem(f"{predicted_band_profit_rate:.2f}%")
                if predicted_band_profit_rate >= 0:
                    predicted_rate_item.setForeground(QBrush(QColor("red")))
                else:
                    predicted_rate_item.setForeground(QBrush(QColor("green")))
                self.prediction_table.setItem(row, 14, predicted_rate_item)
                
                # 收益变化
                change_item = QTableWidgetItem(f"{profit_change:.2f}")
                if profit_change >= 0:
                    change_item.setForeground(QBrush(QColor("red")))
                else:
                    change_item.setForeground(QBrush(QColor("green")))
                self.prediction_table.setItem(row, 15, change_item)
                
                row += 1
            except Exception as e:
                print(f"处理波段数据错误: {str(e)}")
                traceback.print_exc()
                continue
        
        # 设置状态
        self.statusBar.showMessage(f"预测完成，发现 {row} 个有剩余持仓的波段")


if __name__ == "__main__":
    from PyQt5.QtWidgets import QApplication
    import sys
    
    app = QApplication(sys.argv)
    window = PredictionWindow()
    window.show()
    sys.exit(app.exec_()) 