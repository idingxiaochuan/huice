#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
主窗口模块
"""
import os
import sys
import traceback
from datetime import datetime, timedelta
import pandas as pd  # 导入pandas库
import numpy as np
import json
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from PyQt5.QtWidgets import (
    QMainWindow, QVBoxLayout, QHBoxLayout, QWidget, QFrame, QLabel, 
    QPushButton, QComboBox, QDateEdit, QScrollArea, QTabWidget,
    QMessageBox, QLineEdit, QStatusBar, QFileDialog, QAction,
    QMenuBar, QMenu, QGridLayout, QToolBar, QSplitter, 
    QSpacerItem, QSizePolicy, QApplication
)
from PyQt5.QtCore import Qt, QDateTime, QDate, pyqtSignal, pyqtSlot, QTimer, QSize, QThread
from PyQt5.QtGui import QColor, QBrush, QDoubleValidator

from backtest_gui.strategy.band_strategy import BandStrategy
from backtest_gui.execution.trade_executor import TradeExecutor
from backtest_gui.utils.backtest_data_manager import BacktestDataManager
from backtest_gui.gui.chart_widget import ChartWidget
from backtest_gui.gui.trade_panel import TradePanel
from backtest_gui.utils.db_connector import DBConnector
from backtest_gui.utils.backtest_data_manager import BacktestDataManager
from backtest_gui.utils.backtest_engine import BacktestEngine
from backtest_gui.utils.trade_executor import TradeExecutor

class MainWindow(QMainWindow):
    """波段交易回测系统主窗口"""
    
    def __init__(self, config=None):
        """初始化主窗口
        
        Args:
            config: 配置对象
        """
        super().__init__()
        
        # 保存配置
        self.config = config
        
        # 初始化数据库连接器
        try:
            from backtest_gui.utils.db_connector import DBConnector
            self.db_connector = DBConnector()
        except Exception as e:
            print(f"初始化数据库连接器错误: {str(e)}")
            traceback.print_exc()
            self.db_connector = None
        
        # 初始化数据管理器
        self.data_loader = BacktestDataManager(self.db_connector)
        
        # 信号显示状态 - 默认显示
        self.show_signals = True
        
        # 创建状态栏
        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)
        
        try:
            # 初始化UI
            self.init_ui()
            
            # 显示状态信息
            self.statusBar.showMessage("数据库连接成功")
        except Exception as e:
            print(f"初始化错误: {str(e)}")
            traceback.print_exc()
        
    def init_ui(self):
        """初始化用户界面"""
        try:
            # 设置窗口标题
            self.setWindowTitle("行情回测模块")
            self.setMinimumSize(1200, 800)
            
            # 创建中央部件
            central_widget = QWidget()
            self.setCentralWidget(central_widget)
            
            # 创建主布局
            main_layout = QVBoxLayout(central_widget)
            main_layout.setContentsMargins(10, 10, 10, 10)
            main_layout.setSpacing(10)
            
            # 创建顶部控制面板
            control_panel = self.create_control_panel()
            main_layout.addWidget(control_panel)
            
            # 创建策略容器 - 可滚动区域，用于容纳多个回测模块
            self.strategy_scroll_area = QScrollArea()
            self.strategy_scroll_area.setWidgetResizable(True)
            self.strategy_scroll_area.setFrameShape(QFrame.NoFrame)
            
            # 创建内部容器widget
            self.strategy_container = QWidget()
            self.strategy_container_layout = QVBoxLayout(self.strategy_container)
            self.strategy_container_layout.setContentsMargins(0, 0, 0, 0)
            self.strategy_container_layout.setSpacing(10)
            self.strategy_container_layout.addStretch(1)  # 添加弹性空间让内容靠上
            
            # 设置滚动区域的内容
            self.strategy_scroll_area.setWidget(self.strategy_container)
            
            # 添加滚动区域到主布局
            main_layout.addWidget(self.strategy_scroll_area)
            main_layout.setStretchFactor(control_panel, 0)  # 控制面板不伸展
            main_layout.setStretchFactor(self.strategy_scroll_area, 1)  # 内容区域可伸展
            
            # 加载基金列表
            self.load_fund_list()
            
        except Exception as e:
            print(f"初始化界面错误: {str(e)}")
            traceback.print_exc()
            
    def create_control_panel(self):
        """创建顶部控制面板"""
        panel = QWidget()
        panel.setMaximumHeight(50)  # 设置最大高度
        panel.setStyleSheet("""
            QLabel {
                font-size: 12px;
                margin-right: 2px;
            }
            QComboBox, QDateEdit {
                border: 1px solid #CCCCCC;
                border-radius: 3px;
                padding: 2px 5px;
                background: white;
                min-height: 25px;
            }
            QPushButton {
                background-color: #f0f0f0;
                border: 1px solid #CCCCCC;
                border-radius: 3px;
                padding: 5px 10px;
                min-height: 25px;
            }
            QPushButton:hover {
                background-color: #e0e0e0;
            }
            QPushButton#startButton {
                background-color: #4CAF50;
                color: white;
            }
            QPushButton#startButton:hover {
                background-color: #45a049;
            }
        """)
        
        # 创建水平布局
        layout = QHBoxLayout(panel)
        layout.setContentsMargins(10, 5, 10, 5)
        layout.setSpacing(12)  # 设置组件之间的间距
        
        # 1. 基金名称
        fund_label = QLabel("基金名称")
        layout.addWidget(fund_label)
        
        fund_combo = QComboBox()
        fund_combo.addItem("恒生ETF")
        fund_combo.setFixedWidth(180)
        fund_combo.currentTextChanged.connect(self.on_fund_changed)
        layout.addWidget(fund_combo)
        
        # 2. 数据粒度
        data_label = QLabel("数据粒度")
        layout.addWidget(data_label)
        
        data_combo = QComboBox()
        data_combo.addItem("1min")
        data_combo.addItem("5min")
        data_combo.addItem("15min")
        data_combo.addItem("30min")
        data_combo.addItem("60min")
        data_combo.addItem("日线")
        data_combo.setFixedWidth(60)
        data_combo.currentTextChanged.connect(self.on_data_granularity_changed) # 连接数据粒度变化信号
        layout.addWidget(data_combo)
        
        # 3. 波段策略
        strategy_label = QLabel("波段策略")
        layout.addWidget(strategy_label)
        
        strategy_combo = QComboBox()
        strategy_combo.addItem("恒生")
        strategy_combo.setFixedWidth(80)
        strategy_combo.currentTextChanged.connect(self.on_strategy_changed)
        layout.addWidget(strategy_combo)
        
        # 4. 开始日期
        start_label = QLabel("开始日期")
        layout.addWidget(start_label)
        
        start_date = QDateEdit()
        start_date.setDisplayFormat("yyyy-MM-dd")
        start_date.setDate(QDate(2021, 12, 1))
        start_date.setCalendarPopup(True)
        start_date.setFixedWidth(100)
        layout.addWidget(start_date)
        
        # 5. 结束日期
        end_label = QLabel("结束日期")
        layout.addWidget(end_label)
        
        end_date = QDateEdit()
        end_date.setDisplayFormat("yyyy-MM-dd")
        end_date.setDate(QDate(2022, 12, 1))
        end_date.setCalendarPopup(True)
        end_date.setFixedWidth(100)
        layout.addWidget(end_date)
        
        # 添加弹性空间
        layout.addStretch(1)
        
        # 6. 添加策略按钮
        add_strategy_btn = QPushButton("添加策略")
        add_strategy_btn.setFixedWidth(100)
        add_strategy_btn.clicked.connect(self.on_add_strategy)
        layout.addWidget(add_strategy_btn)
        
        # 7. 开始回测按钮
        start_backtest_btn = QPushButton("开始回测")
        start_backtest_btn.setObjectName("startButton")  # 设置对象名以便应用特定样式
        start_backtest_btn.setFixedWidth(100)
        start_backtest_btn.clicked.connect(self.on_start_backtest)
        layout.addWidget(start_backtest_btn)
        
        # 保存引用
        self.fund_combo = fund_combo
        self.data_combo = data_combo
        self.strategy_combo = strategy_combo
        self.start_date_edit = start_date
        self.end_date_edit = end_date
        self.add_strategy_btn = add_strategy_btn
        self.start_backtest_btn = start_backtest_btn
        
        return panel
        
    def on_fund_changed(self, fund_name):
        """基金名称变更事件处理"""
        try:
            self.statusBar.showMessage(f"已选择基金: {fund_name}")
            
            # 加载该基金对应的策略
            self.load_strategies_for_fund(fund_name)
        except Exception as e:
            print(f"基金变更处理错误: {str(e)}")
            traceback.print_exc()
    
    def on_strategy_changed(self, strategy_name):
        """策略选择变更时的回调"""
        try:
            self.statusBar.showMessage(f"已选择策略: {strategy_name}")
        except Exception as e:
            print(f"策略变更处理错误: {str(e)}")
            traceback.print_exc()
    
    def create_subfunc_panel(self):
        """创建子功能面板"""
        try:
            # 创建子功能面板
            subfunc_panel = QWidget()
            subfunc_layout = QVBoxLayout(subfunc_panel)
            subfunc_layout.setContentsMargins(0, 0, 0, 0)
            
            # 添加按钮
            self.predict_button = QPushButton("预测分析")
            self.predict_button.clicked.connect(self.open_prediction_window)
            subfunc_layout.addWidget(self.predict_button)
            
            # 添加交易报告按钮
            self.report_button = QPushButton("交易报告")
            self.report_button.clicked.connect(self.open_trade_report_window)
            subfunc_layout.addWidget(self.report_button)
            
            # 添加更多按钮...
            
            # 添加伸缩项
            subfunc_layout.addStretch()
            
            return subfunc_panel
            
        except Exception as e:
            print(f"创建子功能面板错误: {str(e)}")
            traceback.print_exc()
            return QWidget()
            
    def open_prediction_window(self):
        """打开预测分析窗口"""
        try:
            QMessageBox.information(self, "预测分析", "此功能将在后续版本中实现")
        except Exception as e:
            print(f"打开预测分析窗口错误: {str(e)}")
            traceback.print_exc()
            
    def open_trade_report_window(self):
        """打开交易报告窗口"""
        try:
            from backtest_gui.gui.trade_report_window import TradeReportWindow
            
            # 创建交易报告窗口
            report_window = TradeReportWindow(self.db_connector)
            report_window.show()
            
            # 保存窗口引用，防止被垃圾回收
            self.report_window = report_window
            
        except Exception as e:
            print(f"打开交易报告窗口错误: {str(e)}")
            traceback.print_exc()
            QMessageBox.warning(self, "打开错误", f"打开交易报告窗口失败: {str(e)}")
    
    def load_fund_list(self):
        """加载基金列表"""
        try:
            # 清空基金下拉框
            self.fund_combo.clear()
            
            # 尝试从数据库加载基金列表
            if self.db_connector:
                conn = None
                try:
                    conn = self.db_connector.get_connection()
                    cursor = conn.cursor()
                    
                    # 查询基金列表 - 优先从fund_info表获取
                    cursor.execute("""
                        SELECT DISTINCT fi.fund_code, fi.fund_name 
                        FROM fund_info fi
                        JOIN stock_quotes sq ON fi.fund_code = sq.fund_code
                        ORDER BY fi.fund_code
                    """)
                    
                    funds = cursor.fetchall()
                    
                    # 如果fund_info表没有数据，则从stock_quotes表获取
                    if not funds:
                        cursor.execute("""
                            SELECT DISTINCT fund_code 
                            FROM stock_quotes
                            ORDER BY fund_code
                        """)
                        funds = [(row[0], row[0]) for row in cursor.fetchall()]
                    
                    # 添加基金到下拉框
                    for fund_code, fund_name in funds:
                        # 添加市场后缀
                        if fund_code.startswith('5') or fund_code.startswith('6'):
                            display_code = f"{fund_code}.SH"
                        else:
                            display_code = f"{fund_code}.SZ"
                        
                        # 如果有名称，则显示代码和名称
                        if fund_name and fund_name != fund_code:
                            display_text = f"{display_code} - {fund_name}"
                        else:
                            display_text = display_code
                        
                        # 将原始代码存储为用户数据，以便后续使用
                        self.fund_combo.addItem(display_text, fund_code)
                    
                    print(f"加载了 {self.fund_combo.count()} 个基金")
                    
                    # 如果有基金，选择第一个并加载相关数据
                    if self.fund_combo.count() > 0:
                        self.on_fund_changed(self.fund_combo.currentText())
                        
                finally:
                    if conn:
                        self.db_connector.release_connection(conn)
            
            # 如果数据库加载失败或没有基金，添加示例基金
            if self.fund_combo.count() == 0:
                self.fund_combo.addItem("恒生ETF")
                    
        except Exception as e:
            print(f"加载基金列表错误: {str(e)}")
            traceback.print_exc()
            
    def load_strategies_for_fund(self, fund_name):
        """加载指定基金的策略列表
        
        Args:
            fund_name: 基金名称
        """
        try:
            # 清空策略下拉框
            self.strategy_combo.clear()
            
            # 获取基金代码（去除市场后缀）
            fund_code = self.fund_combo.currentData()
            if not fund_code:
                fund_code = fund_name.split('.')[0]
            
            # 尝试从数据库加载策略列表
            if self.db_connector:
                conn = None
                try:
                    conn = self.db_connector.get_connection()
                    cursor = conn.cursor()
                    
                    # 查询与该基金绑定的策略
                    cursor.execute("""
                        SELECT bs.id, bs.name
                        FROM band_strategies bs
                        JOIN fund_strategy_bindings fsb ON bs.id = fsb.strategy_id
                        WHERE fsb.fund_code = %s
                        ORDER BY fsb.is_default DESC, bs.name
                    """, (fund_code,))
                    
                    strategies = cursor.fetchall()
                    
                    # 如果没有绑定的策略，则加载所有策略
                    if not strategies:
                        cursor.execute("""
                            SELECT id, name
                            FROM band_strategies
                            ORDER BY name
                        """)
                        strategies = cursor.fetchall()
                    
                    # 添加策略到下拉框
                    for strategy_id, strategy_name in strategies:
                        self.strategy_combo.addItem(strategy_name, strategy_id)
                    
                    print(f"加载了 {self.strategy_combo.count()} 个策略")
                    
                finally:
                    if conn:
                        self.db_connector.release_connection(conn)
            
            # 如果数据库加载失败或没有策略，添加示例策略
            if self.strategy_combo.count() == 0:
                self.strategy_combo.addItem("恒生")
            
            # 加载该基金的数据粒度选项
            self.load_data_granularity_for_fund(fund_code)
            
            # 加载该基金的日期范围
            self.load_date_range_for_fund(fund_code)
            
        except Exception as e:
            print(f"加载策略列表错误: {str(e)}")
            traceback.print_exc()
            
    def load_data_granularity_for_fund(self, fund_code):
        """加载指定基金的数据粒度选项
        
        Args:
            fund_code: 基金代码
        """
        try:
            # 清空数据粒度下拉框
            self.data_combo.clear()
            
            # 默认数据粒度选项
            default_granularities = ["1min", "5min", "15min", "30min", "60min", "日线"]
            
            # 尝试从数据库加载数据粒度
            if self.db_connector:
                conn = None
                try:
                    conn = self.db_connector.get_connection()
                    cursor = conn.cursor()
                    
                    # 查询该基金的可用数据粒度
                    cursor.execute("""
                        SELECT DISTINCT data_level
                        FROM stock_quotes
                        WHERE fund_code = %s
                        ORDER BY data_level
                    """, (fund_code,))
                    
                    granularities = [row[0] for row in cursor.fetchall()]
                    
                    # 如果有数据粒度，添加到下拉框
                    if granularities:
                        # 自定义排序顺序
                        ordered_granularities = []
                        for g in ["1min", "5min", "15min", "30min", "60min", "day"]:
                            if g in granularities:
                                ordered_granularities.append(g)
                        
                        # 添加任何其他可能的粒度
                        for g in granularities:
                            if g not in ordered_granularities:
                                ordered_granularities.append(g)
                        
                        # 添加到下拉框
                        for granularity in ordered_granularities:
                            # 将'day'显示为'日线'
                            display_text = "日线" if granularity == "day" else granularity
                            self.data_combo.addItem(display_text, granularity)
                    else:
                        # 如果没有数据粒度，使用默认选项
                        for granularity in default_granularities:
                            self.data_combo.addItem(granularity)
                    
                finally:
                    if conn:
                        self.db_connector.release_connection(conn)
            else:
                # 如果数据库连接失败，使用默认选项
                for granularity in default_granularities:
                    self.data_combo.addItem(granularity)
                    
        except Exception as e:
            print(f"加载数据粒度错误: {str(e)}")
            traceback.print_exc()
            
            # 出错时使用默认选项
            for granularity in ["1min", "5min", "15min", "30min", "60min", "日线"]:
                self.data_combo.addItem(granularity)
                
    def load_date_range_for_fund(self, fund_code):
        """加载指定基金的日期范围
        
        Args:
            fund_code: 基金代码
        """
        try:
            # 获取当前选择的数据粒度
            data_level = self.data_combo.currentData()
            if not data_level:
                data_level = self.data_combo.currentText()
                if data_level == "日线":
                    data_level = "day"
            
            # 尝试从数据库加载日期范围
            if self.db_connector:
                conn = None
                try:
                    conn = self.db_connector.get_connection()
                    cursor = conn.cursor()
                    
                    # 查询该基金在指定数据粒度下的日期范围
                    cursor.execute("""
                        SELECT MIN(date), MAX(date)
                        FROM stock_quotes
                        WHERE fund_code = %s AND data_level = %s
                    """, (fund_code, data_level))
                    
                    result = cursor.fetchone()
                    
                    if result and result[0] and result[1]:
                        min_date, max_date = result
                        
                        # 设置开始日期为最早日期
                        start_date = QDate.fromString(min_date.strftime("%Y-%m-%d"), "yyyy-MM-dd")
                        self.start_date_edit.setDate(start_date)
                        
                        # 设置结束日期为最晚日期
                        end_date = QDate.fromString(max_date.strftime("%Y-%m-%d"), "yyyy-MM-dd")
                        self.end_date_edit.setDate(end_date)
                        
                        print(f"设置日期范围: {min_date.strftime('%Y-%m-%d')} 至 {max_date.strftime('%Y-%m-%d')}")
                    else:
                        # 如果没有数据，使用默认日期范围
                        self.set_default_date_range()
                        
                finally:
                    if conn:
                        self.db_connector.release_connection(conn)
            else:
                # 如果数据库连接失败，使用默认日期范围
                self.set_default_date_range()
                
        except Exception as e:
            print(f"加载日期范围错误: {str(e)}")
            traceback.print_exc()
            
            # 出错时使用默认日期范围
            self.set_default_date_range()
            
    def set_default_date_range(self):
        """设置默认日期范围"""
        # 设置默认开始日期为一年前
        start_date = QDate.currentDate().addYears(-1)
        self.start_date_edit.setDate(start_date)
        
        # 设置默认结束日期为当前日期
        end_date = QDate.currentDate()
        self.end_date_edit.setDate(end_date)
        
    def on_data_granularity_changed(self, data_level):
        """数据粒度变更事件处理"""
        try:
            # 获取当前选择的基金代码
            fund_code = self.fund_combo.currentData()
            if not fund_code:
                fund_name = self.fund_combo.currentText()
                fund_code = fund_name.split('.')[0]
            
            # 更新日期范围
            self.load_date_range_for_fund(fund_code)
            
        except Exception as e:
            print(f"数据粒度变更处理错误: {str(e)}")
            traceback.print_exc()
            
    def closeEvent(self, event):
        """窗口关闭事件处理"""
        try:
            # 关闭数据库连接
            if self.db_connector:
                self.db_connector.close_all()
                
            # 接受关闭事件
            event.accept()
        except Exception as e:
            print(f"窗口关闭事件处理错误: {str(e)}")
            traceback.print_exc()
            event.accept()
    
    def create_backtest_module(self, fund_name, strategy_name, start_date, end_date):
        """创建单个回测模块
        
        Args:
            fund_name: 基金名称
            strategy_name: 策略名称
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            QWidget: 回测模块组件
        """
        # 导入必要的库
        from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
        from matplotlib.figure import Figure
        
        print(f"\n===== 创建回测模块 =====")
        print(f"基金: {fund_name}, 策略: {strategy_name}")
        print(f"日期范围: {start_date} 至 {end_date}")
        
        # 创建整个模块的容器 - 不使用QFrame，改用QWidget以去除所有边框
        module = QWidget()
        module.setStyleSheet("""
            QWidget {
                background-color: white;
                border: none;
            }
        """)
        
        # 创建主布局，增加间距
        module_layout = QVBoxLayout(module)
        module_layout.setContentsMargins(10, 10, 10, 10)
        module_layout.setSpacing(5)
        
        # 创建顶部信息布局
        info_layout = QHBoxLayout()
        
        # 添加基金和策略信息
        info_text = f"基金: {fund_name}    策略: {strategy_name}    时间: {start_date} 至 {end_date}"
        info_label = QLabel(info_text)
        info_label.setStyleSheet("font-weight: bold; color: #333333;")
        info_layout.addWidget(info_label)
        
        # 添加关闭按钮
        close_button = QPushButton("×")
        close_button.setFixedSize(20, 20)
        close_button.setStyleSheet("""
            QPushButton {
                background-color: #f44336;
                color: white;
                border-radius: 10px;
                font-weight: bold;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #d32f2f;
            }
        """)
        close_button.clicked.connect(lambda: self.remove_backtest_module(module))
        info_layout.addWidget(close_button)
        
        # 将信息布局添加到主布局
        module_layout.addLayout(info_layout)
        
        # 创建图表布局
        chart_layout = QVBoxLayout()
        chart_layout.setContentsMargins(0, 0, 0, 0)
        
        # 定义图表尺寸和DPI
        # 注意：我们不再使用固定尺寸，而是让图表自适应容器大小
        figsize = (8.0, 2.66)  # 初始尺寸，但会根据容器大小自动调整
        dpi = 100
        
        # 保存图表尺寸和DPI为模块属性，方便后续使用
        module.chart_figsize = figsize
        module.chart_dpi = dpi
        
        # 创建图表
        fig = Figure(figsize=figsize, dpi=dpi, facecolor='white', constrained_layout=True)
        fig.subplots_adjust(left=0.03, right=0.99, top=0.95, bottom=0.20)
        
        # 创建画布，并设置为可伸缩
        canvas = FigureCanvas(fig)
        canvas.setMinimumHeight(250)  # 设置最小高度
        
        # 设置尺寸策略，使图表可以在两个方向上伸缩
        sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        canvas.setSizePolicy(sizePolicy)
        
        print(f"创建图表: 初始尺寸={figsize}, DPI={dpi}")
        print(f"画布大小: {canvas.size().width()}x{canvas.size().height()}")
        print(f"尺寸策略: 水平={sizePolicy.horizontalPolicy()}, 垂直={sizePolicy.verticalPolicy()}")
        
        # 保存图表控件引用
        module.chart_widget = canvas
        
        # 将图表添加到布局
        chart_layout.addWidget(canvas)
        
        # 将图表布局添加到主布局
        module_layout.addLayout(chart_layout)
        module.chart_layout = chart_layout
        
        # 创建结果文本
        result_text = QLabel("准备开始回测...")
        result_text.setAlignment(Qt.AlignCenter)
        result_text.setStyleSheet("color: #666666;")
        module_layout.addWidget(result_text)
        
        # 保存结果文本引用
        module.result_text = result_text
        
        # 创建按钮布局
        button_layout = QHBoxLayout()
        button_layout.setContentsMargins(0, 5, 0, 0)
        
        # 添加详情按钮
        details_button = QPushButton("查看详情")
        details_button.setStyleSheet("""
            QPushButton {
                background-color: #2196F3;
                color: white;
                border: none;
                padding: 5px 10px;
                border-radius: 3px;
            }
            QPushButton:hover {
                background-color: #1976D2;
            }
            QPushButton:disabled {
                background-color: #BBDEFB;
            }
        """)
        details_button.setEnabled(False)  # 初始禁用
        details_button.clicked.connect(self.show_trade_details)
        button_layout.addWidget(details_button)
        
        # 保存详情按钮引用
        module.details_button = details_button
        
        # 添加信号切换按钮
        signal_toggle_btn = QPushButton("隐藏信号")
        signal_toggle_btn.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                border: none;
                padding: 5px 10px;
                border-radius: 3px;
            }
            QPushButton:hover {
                background-color: #388E3C;
            }
        """)
        signal_toggle_btn.clicked.connect(lambda: self.toggle_signals_visibility(module))
        button_layout.addWidget(signal_toggle_btn)
        
        # 保存信号切换按钮引用
        module.signal_toggle_btn = signal_toggle_btn
        
        # 将按钮布局添加到主布局
        module_layout.addLayout(button_layout)
        
        # 初始化backtest_worker属性为None
        module.backtest_worker = None
        
        # 创建空图表
        self.create_empty_chart(canvas)
        
        # 添加大小变化事件处理
        def on_resize(event):
            print(f"\n===== 模块大小变化 =====")
            print(f"新大小: {module.size().width()}x{module.size().height()}")
            print(f"画布大小: {canvas.size().width()}x{canvas.size().height()}")
            # 在大小变化时，可以在这里添加额外的处理逻辑
        
        # 保存原始的resizeEvent
        original_resize_event = module.resizeEvent
        
        # 重写resizeEvent方法
        def custom_resize_event(event):
            if original_resize_event:
                original_resize_event(event)
            on_resize(event)
        
        # 设置自定义的resizeEvent
        module.resizeEvent = custom_resize_event
        
        print(f"回测模块创建完成: {id(module)}")
        
        return module
    
    def create_empty_chart(self, chart_widget):
        """创建空白图表
        
        Args:
            chart_widget: 图表控件
        """
        try:
            # 导入必要的库
            from matplotlib.figure import Figure
            import matplotlib.pyplot as plt
            
            # 创建新的图表
            fig = Figure(figsize=(8, 2.66), dpi=100, facecolor='white')
            fig.subplots_adjust(left=0.03, right=0.99, top=0.95, bottom=0.20)
            ax = fig.add_subplot(111)
            
            # 设置标题和标签
            ax.set_title("暂无数据")
            ax.set_ylabel("价格")
            
            # 设置垂直方向的边距
            ax.margins(0, 0.15)
            
            # 添加网格线
            ax.grid(True, linestyle='-', color='#E5E5E5', alpha=0.5)
            
            # 设置x轴刻度和标签
            ax.set_xticks([0.0, 0.5, 1.0])
            ax.set_xticklabels(["开始日期", "", "结束日期"], fontsize=12, fontweight='bold')
            
            # 设置标签对齐方式
            for i, label in enumerate(ax.get_xticklabels()):
                if i == 0:
                    label.set_ha('left')  # 左对齐
                elif i == 2:
                    label.set_ha('right')  # 右对齐
                else:
                    label.set_ha('center')  # 居中
            
            # 确保标签可见并设置底部边距
            ax.tick_params(axis='x', which='both', length=0, pad=10)
            
            # 移除所有边框
            for spine in ax.spines.values():
                spine.set_visible(False)
            
            # 移除y轴刻度标记，但保留标签
            ax.tick_params(axis='y', which='both', length=0)
            
            # 更新图表
            chart_widget.figure = fig
            chart_widget.draw()
        except Exception as e:
            print(f"创建空白图表时出错: {str(e)}")
            traceback.print_exc()
    
    def add_backtest_module(self, fund_name, strategy_name, start_date, end_date):
        """添加一个新的回测模块到容器
        
        Args:
            fund_name: 基金名称
            strategy_name: 策略名称
            start_date: 开始日期
            end_date: 结束日期
        """
        try:
            # 创建新模块
            module = self.create_backtest_module(fund_name, strategy_name, start_date, end_date)
            
            # 添加到容器中，确保添加在stretch之前
            self.strategy_container_layout.insertWidget(
                self.strategy_container_layout.count() - 1,  # 在stretch之前插入
                module
            )
            
            # 让模块之间有更大的间距
            if self.strategy_container_layout.count() > 2:  # 如果有多个模块
                self.strategy_container_layout.setSpacing(15)  # 增加模块间距
            
        except Exception as e:
            print(f"添加回测模块错误: {str(e)}")
            traceback.print_exc()
            
    def remove_backtest_module(self, module):
        """移除回测模块
        
        Args:
            module: 要移除的模块
        """
        try:
            # 从布局中移除模块
            self.strategy_container_layout.removeWidget(module)
            
            # 删除模块
            module.deleteLater()
            
            # 如果没有模块了，重置间距
            if self.strategy_container_layout.count() <= 2:  # 只剩下stretch和添加按钮
                self.strategy_container_layout.setSpacing(0)
                
        except Exception as e:
            print(f"移除回测模块错误: {str(e)}")
            traceback.print_exc()
            
    def update_backtest_chart(self, module, stock_data, current_index=None, buy_signals=None, sell_signals=None):
        """更新回测图表
        
        Args:
            module: 回测模块
            stock_data: 股票数据
            current_index: 当前处理的数据索引
            buy_signals: 买入信号列表
            sell_signals: 卖出信号列表
        """
        # 导入必要的库
        from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
        import matplotlib
        matplotlib.use('Agg')  # 使用非交互式后端
        import matplotlib.pyplot as plt
        from matplotlib.figure import Figure
        from matplotlib.patches import Polygon
        import numpy as np
        from datetime import datetime
        import matplotlib.colors as mcolors
        import pandas as pd
        from PyQt5.QtCore import QThread, pyqtSignal

        # 创建后台绘图线程类，防止UI阻塞
        class ChartDrawThread(QThread):
            chart_ready = pyqtSignal(object)  # 图表准备好的信号
            progress = pyqtSignal(str)  # 进度信息信号

            def __init__(self, stock_data, chart_size, buy_signals, sell_signals, show_signals):
                super().__init__()
                self.stock_data = stock_data
                self.chart_size = chart_size
                self.buy_signals = buy_signals
                self.sell_signals = sell_signals
                self.show_signals = show_signals
                
            def run(self):
                try:
                    self.progress.emit("准备数据...")
                    
                    # 使用全部原始数据，不进行任何采样
                    display_data = self.stock_data
                    
                    # 获取日期和收盘价列名
                    date_column = 'date'
                    close_column = 'close'
                    
                    for col in display_data.columns:
                        if 'date' in col.lower() or 'time' in col.lower():
                            date_column = col
                        elif 'close' in col.lower() or 'price' in col.lower():
                            close_column = col
                    
                    # 打印使用全部数据的信息
                    original_length = len(display_data)
                    self.progress.emit(f"使用全部 {original_length} 条数据绘制精细曲线")
                    
                    # 根据图表控件的实际大小调整图表尺寸
                    width_inches = self.chart_size.width() / 100  # 转换为英寸
                    height_inches = self.chart_size.height() / 100  # 转换为英寸
                    
                    # 确保尺寸合理
                    width_inches = max(width_inches, 6.0)  # 最小宽度
                    height_inches = max(height_inches, 2.0)  # 最小高度
                    
                    # 创建新的图表，使用自适应尺寸
                    self.progress.emit(f"创建新图表，尺寸: {width_inches:.2f}x{height_inches:.2f} 英寸")
                    fig = Figure(figsize=(width_inches, height_inches), dpi=100, facecolor='white', constrained_layout=True)
                    
                    # 调整子图边距，增加左边距以确保净值数字完整显示，增加底部边距以容纳日期标签
                    fig.subplots_adjust(left=0.03, right=0.99, top=0.95, bottom=0.20)
                    ax = fig.add_subplot(111)
                    
                    # 绘制价格曲线
                    dates = display_data[date_column].values
                    prices = display_data[close_column].values
                    
                    # 创建x轴数据点
                    x_values = np.arange(len(dates))
                    
                    # 绘制价格曲线 - 使用更细的线宽和更高质量的渲染
                    ax.plot(x_values, prices, 'b-', linewidth=0.8, antialiased=True)
                    
                    # 设置垂直方向的边距，使曲线不会紧贴上下边框
                    ax.margins(0, 0.15)
                    
                    # 设置网格线
                    ax.grid(True, linestyle='-', color='#E5E5E5', alpha=0.5)
                    
                    # 只有在显示信号模式下才绘制买入和卖出信号标记
                    if self.show_signals and (self.buy_signals or self.sell_signals):
                        self.progress.emit("开始绘制信号点...")
                        
                        # 创建日期索引字典，用于快速查找日期对应的索引，大幅提高性能
                        date_index_map = {}
                        for i, date_val in enumerate(dates):
                            date_str = None
                            if hasattr(date_val, 'strftime'):
                                date_str = date_val.strftime('%Y-%m-%d')
                            elif isinstance(date_val, str):
                                if ' ' in date_val:
                                    date_str = date_val.split(' ')[0]
                                elif 'T' in date_val:
                                    date_str = date_val.split('T')[0]
                                else:
                                    date_str = date_val[:10]
                            else:
                                date_str = str(date_val)[:10]
                                
                            # 添加到字典，可能有多个点对应同一天
                            if date_str not in date_index_map:
                                date_index_map[date_str] = []
                            date_index_map[date_str].append(i)
                        
                        # 处理买入信号 - 批量收集后一次性绘制
                        if self.buy_signals:
                            # 过滤测试信号
                            filtered_buy_signals = [signal for signal in self.buy_signals if 'time' in signal and 'price' in signal]
                            
                            # 去重，按日期分组
                            buy_signal_dates = {}
                            for signal in filtered_buy_signals:
                                signal_time = signal['time']
                                date_str = None
                                
                                if hasattr(signal_time, 'strftime'):
                                    date_str = signal_time.strftime('%Y-%m-%d')
                                elif isinstance(signal_time, str):
                                    if ' ' in signal_time:
                                        date_str = signal_time.split(' ')[0]
                                    elif 'T' in signal_time:
                                        date_str = signal_time.split('T')[0]
                                    else:
                                        date_str = signal_time[:10]
                                
                                if date_str and date_str not in buy_signal_dates:
                                    buy_signal_dates[date_str] = signal
                            
                            # 批量收集买入点坐标，一次性绘制
                            buy_x = []
                            buy_y = []
                            for date_str, signal in buy_signal_dates.items():
                                if date_str in date_index_map:
                                    # 优先使用该日期最早的数据点
                                    idx = date_index_map[date_str][0]
                                    actual_price = display_data[close_column].iloc[idx]
                                    buy_x.append(idx)
                                    buy_y.append(actual_price)
                            
                            # 批量绘制所有买入点，而不是逐个绘制
                            if buy_x:
                                self.progress.emit(f"批量绘制 {len(buy_x)} 个买入信号...")
                                ax.scatter(buy_x, buy_y, marker='^', color='green', s=80, alpha=0.8)
                        
                        # 处理卖出信号 - 批量收集后一次性绘制
                        if self.sell_signals:
                            # 过滤测试信号
                            filtered_sell_signals = [signal for signal in self.sell_signals if 'time' in signal and 'price' in signal]
                            
                            # 去重，按日期分组
                            sell_signal_dates = {}
                            for signal in filtered_sell_signals:
                                signal_time = signal['time']
                                date_str = None
                                
                                if hasattr(signal_time, 'strftime'):
                                    date_str = signal_time.strftime('%Y-%m-%d')
                                elif isinstance(signal_time, str):
                                    if ' ' in signal_time:
                                        date_str = signal_time.split(' ')[0]
                                    elif 'T' in signal_time:
                                        date_str = signal_time.split('T')[0]
                                    else:
                                        date_str = signal_time[:10]
                                
                                if date_str and date_str not in sell_signal_dates:
                                    sell_signal_dates[date_str] = signal
                            
                            # 批量收集卖出点坐标，一次性绘制
                            sell_x = []
                            sell_y = []
                            for date_str, signal in sell_signal_dates.items():
                                if date_str in date_index_map:
                                    # 优先使用该日期最早的数据点
                                    idx = date_index_map[date_str][0]
                                    actual_price = display_data[close_column].iloc[idx]
                                    sell_x.append(idx)
                                    sell_y.append(actual_price)
                            
                            # 批量绘制所有卖出点，而不是逐个绘制
                            if sell_x:
                                self.progress.emit(f"批量绘制 {len(sell_x)} 个卖出信号...")
                                ax.scatter(sell_x, sell_y, marker='v', color='red', s=80, alpha=0.8)
                    
                    self.progress.emit("图表绘制完成")
                    self.chart_ready.emit(fig)
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    self.progress.emit(f"图表绘制错误: {str(e)}")
        
        # 主方法继续
        print("\n===== update_backtest_chart方法开始 =====")
        print(f"传入的参数:")
        print(f"  - module: {type(module)}")
        print(f"  - stock_data: {len(stock_data) if stock_data is not None else 'None'} 条记录")
        print(f"  - current_index: {current_index}")
        print(f"  - buy_signals: {len(buy_signals) if buy_signals else 0} 个")
        print(f"  - sell_signals: {len(sell_signals) if sell_signals else 0} 个")
        print(f"  - self.show_signals: {self.show_signals}")
        
        # 保存原始信号数据，只在绘制时根据显示状态决定是否使用
        original_buy_signals = buy_signals
        original_sell_signals = sell_signals
        
        if module is None:
            print("错误: 模块为空，无法更新图表")
            return
            
        # 直接从模块获取图表控件
        if hasattr(module, 'chart_widget'):
            chart_widget = module.chart_widget
            print("从模块属性中找到图表控件")
        else:
            # 如果没有直接属性，尝试从子控件中查找
            chart_widget = None
            for child in module.children():
                if isinstance(child, FigureCanvas):
                    chart_widget = child
                    print("从模块子控件中找到图表控件")
                    break
            
            if not chart_widget:
                print("未找到图表控件，尝试重新创建...")
                from matplotlib.figure import Figure
                
                # 创建图表控件
                fig = Figure(figsize=(8, 2.66), dpi=100, facecolor='white', constrained_layout=True)
                chart_widget = FigureCanvas(fig)
                
                # 设置尺寸策略，使图表可以在两个方向上伸缩
                sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
                chart_widget.setSizePolicy(sizePolicy)
                
                # 添加到模块布局
                if hasattr(module, 'chart_layout'):
                    module.chart_layout.addWidget(chart_widget)
                    print("已重新创建图表控件并添加到布局")
                else:
                    print("模块没有chart_layout属性，无法添加图表控件")
                    return
        
        if stock_data is None or stock_data.empty:
            print("警告: 股票数据为空，创建空图表")
            self.create_empty_chart(chart_widget)
            return
            
        try:
            # 获取模块和图表控件的尺寸
            module_size = module.size()
            chart_size = chart_widget.size()
            print(f"模块大小: {module_size.width()}x{module_size.height()}")
            print(f"图表控件大小: {chart_size.width()}x{chart_size.height()}")
            
            # 显示加载中的状态信息
            self.statusBar.showMessage("正在绘制图表，请稍候...")
            
            # 先显示一个加载中的提示
            if hasattr(chart_widget, 'figure') and chart_widget.figure:
                fig = chart_widget.figure
                for ax in fig.axes:
                    ax.clear()
                ax = fig.axes[0] if fig.axes else fig.add_subplot(111)
                ax.text(0.5, 0.5, '正在绘制图表，请稍候...', 
                        horizontalalignment='center',
                        verticalalignment='center',
                        transform=ax.transAxes,
                        fontsize=14)
                ax.axis('off')
                chart_widget.draw()
            
            # 创建并启动绘图线程
            self.draw_thread = ChartDrawThread(
                stock_data, 
                chart_size, 
                original_buy_signals, 
                original_sell_signals, 
                self.show_signals
            )
            
            # 连接线程信号
            def on_chart_ready(fig):
                # 设置新的图表到画布
                chart_widget.figure = fig
                # 刷新图表
                chart_widget.draw()
                # 设置尺寸策略，确保图表可以在两个方向上伸缩
                sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
                chart_widget.setSizePolicy(sizePolicy)
                # 更新状态栏
                self.statusBar.showMessage(f"图表已更新 - 显示 {len(stock_data)} 条数据记录")
                print("图表绘制完成，已刷新显示")
            
            def on_progress(message):
                # 更新状态栏显示进度
                self.statusBar.showMessage(f"绘制图表中: {message}")
                print(message)
            
            # 连接信号到槽函数
            self.draw_thread.chart_ready.connect(on_chart_ready)
            self.draw_thread.progress.connect(on_progress)
            
            # 启动线程
            self.draw_thread.start()
            
            # 返回更新后的图表控件
            return chart_widget
            
        except Exception as e:
            print(f"更新图表时发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def _on_backtest_progress(self, current, total, message):
        """回测进度回调"""
        # 在状态栏显示进度信息
        progress_percentage = int(current / total * 100) if total > 0 else 0
        self.statusBar.showMessage(f"{message} - {progress_percentage}% ({current}/{total})")
    
    def _on_backtest_chart_update(self, module, data, current_index, buy_signals, sell_signals):
        """回测图表更新回调
        
        在优化后的策略中，回测过程中不再更新图表，只记录更新状态
        """
        # 更新状态栏，但不绘制图表
        self.statusBar.showMessage(f"回测计算中: 已处理 {current_index+1}/{len(data)} 条数据，买入信号:{len(buy_signals)}个，卖出信号:{len(sell_signals)}个")
        
        # 更新进度文本，但不更新图表
        if hasattr(module, 'result_text'):
            module.result_text.setText(f"处理进度: {current_index+1}/{len(data)} ({(current_index+1)/len(data)*100:.1f}%)")
    
    def _on_backtest_completed(self, module, data, buy_signals, sell_signals, total_profit_rate, total_profit, backtest_id):
        """回测完成回调"""
        try:
            # 导入必要的库
            from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
            
            print("\n===== 回测完成回调 =====")
            print(f"参数信息:")
            print(f"  - module: {type(module)}")
            print(f"  - data: {len(data) if data is not None else 'None'} 条记录")
            print(f"  - buy_signals: {len(buy_signals) if buy_signals else 0} 个")
            print(f"  - sell_signals: {len(sell_signals) if sell_signals else 0} 个")
            print(f"  - total_profit_rate: {total_profit_rate}")
            print(f"  - backtest_id: {backtest_id}")
            print(f"  - 模块大小: {module.size().width()}x{module.size().height()}")
            
            # 更新模块显示
            module.result_text.setText(f"总收益率: {total_profit_rate:.2f}%, 总收益: {total_profit:.2f}")
            
            # 保存回测ID到模块
            module.backtest_id = backtest_id
            
            # 启用查看详情按钮
            if hasattr(module, 'details_button'):
                module.details_button.setEnabled(True)
            
            # 隐藏取消按钮
            if hasattr(module, 'cancel_button'):
                module.cancel_button.setVisible(False)
            
            # 保存数据到backtest_worker对象，以便后续可以切换信号显示
            worker = module.backtest_worker
            print("保存数据到backtest_worker对象:")
            worker.stock_data = data
            worker.buy_signals = buy_signals
            worker.sell_signals = sell_signals
            print(f"  - 已保存stock_data: {len(worker.stock_data) if worker.stock_data is not None else 'None'} 条记录")
            print(f"  - 已保存buy_signals: {len(worker.buy_signals) if worker.buy_signals else 0} 个")
            print(f"  - 已保存sell_signals: {len(worker.sell_signals) if worker.sell_signals else 0} 个")
            
            # 验证数据是否正确保存
            print(f"  - 验证stock_data: {len(worker.stock_data) if hasattr(worker, 'stock_data') and worker.stock_data is not None else 'None'} 条记录")
            print(f"  - 验证buy_signals: {len(worker.buy_signals) if hasattr(worker, 'buy_signals') and worker.buy_signals else 0} 个")
            print(f"  - 验证sell_signals: {len(worker.sell_signals) if hasattr(worker, 'sell_signals') and worker.sell_signals else 0} 个")
            
            print(f"回测完成，准备绘制图表: 数据长度={len(data)}, 买入信号={len(buy_signals) if buy_signals else 0}, 卖出信号={len(sell_signals) if sell_signals else 0}")
            
            # 打印买入信号详情
            if buy_signals and len(buy_signals) > 0:
                print("\n===== 买入信号详情 =====")
                for i, signal in enumerate(buy_signals[:5]):  # 只打印前5个信号
                    print(f"买入信号 {i+1}: {signal}")
                if len(buy_signals) > 5:
                    print(f"... 还有 {len(buy_signals) - 5} 个买入信号")
            
            # 打印卖出信号详情
            if sell_signals and len(sell_signals) > 0:
                print("\n===== 卖出信号详情 =====")
                for i, signal in enumerate(sell_signals[:5]):  # 只打印前5个信号
                    print(f"卖出信号 {i+1}: {signal}")
                if len(sell_signals) > 5:
                    print(f"... 还有 {len(sell_signals) - 5} 个卖出信号")
            
            # 获取图表控件
            chart_widget = None
            if hasattr(module, 'chart_widget'):
                chart_widget = module.chart_widget
                print(f"找到图表控件，尺寸: {chart_widget.size().width()}x{chart_widget.size().height()}")
            else:
                # 如果没有直接属性，尝试从子控件中查找
                for child in module.findChildren(FigureCanvas):
                    chart_widget = child
                    module.chart_widget = child  # 保存引用以便后续使用
                    print(f"在子控件中找到图表控件，尺寸: {chart_widget.size().width()}x{chart_widget.size().height()}")
                    break
            
            if not chart_widget:
                print("错误: 未找到图表控件，无法更新图表")
                return
            
            # 获取图表控件的当前尺寸
            chart_size = chart_widget.size()
            print(f"图表控件当前尺寸: {chart_size.width()}x{chart_size.height()}")
            
            # 根据图表控件的实际大小调整图表尺寸
            width_inches = chart_size.width() / 100  # 转换为英寸
            height_inches = chart_size.height() / 100  # 转换为英寸
            
            # 确保尺寸合理
            width_inches = max(width_inches, 6.0)  # 最小宽度
            height_inches = max(height_inches, 2.0)  # 最小高度
            
            # 创建新图表，使用当前容器尺寸
            print(f"创建新图表，尺寸: {width_inches:.2f}x{height_inches:.2f} 英寸")
            from matplotlib.figure import Figure
            fig = Figure(figsize=(width_inches, height_inches), dpi=100, facecolor='white', constrained_layout=True)
            
            # 设置新的图表到画布
            chart_widget.figure = fig
            
            # 设置尺寸策略，确保图表可以在两个方向上伸缩
            from PyQt5.QtWidgets import QSizePolicy
            sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
            chart_widget.setSizePolicy(sizePolicy)
            print(f"设置尺寸策略: 水平={sizePolicy.horizontalPolicy()}, 垂直={sizePolicy.verticalPolicy()}")
            
            # 更新图表
            self.update_backtest_chart(module, data, len(data)-1, buy_signals, sell_signals)
            
            # 更新状态栏
            self.statusBar.showMessage(f"回测完成: {total_profit_rate:.2f}% 收益率")
            
            print("回测工作线程执行完毕")
        except Exception as e:
            print(f"回测完成回调错误: {str(e)}")
            import traceback
            traceback.print_exc()
            self.statusBar.showMessage(f"回测完成处理错误: {str(e)}")
            
            # 尝试更新模块显示
            if hasattr(module, 'result_text'):
                module.result_text.setText(f"回测错误: {str(e)}")
                
            # 尝试隐藏取消按钮
            if hasattr(module, 'cancel_button'):
                module.cancel_button.setVisible(False)
    
    def _on_backtest_error(self, error_message):
        """回测错误回调"""
        QMessageBox.warning(self, "回测错误", error_message)
    
    def _cancel_backtest(self, module):
        """取消回测"""
        if hasattr(module, 'backtest_worker'):
            # 确认是否取消
            reply = QMessageBox.question(
                self, 
                "取消回测", 
                "确定要取消当前回测吗？", 
                QMessageBox.Yes | QMessageBox.No, 
                QMessageBox.No
            )
            
            if reply == QMessageBox.Yes:
                # 取消回测
                module.backtest_worker.cancel()
                self.statusBar.showMessage("正在取消回测...")
    
    def save_backtest_results(self, fund_code, start_date, end_date, initial_capital, final_capital, total_profit, total_profit_rate, strategy_id, strategy_name):
        """保存回测结果到数据库
        
        Args:
            fund_code: 基金代码
            start_date: 开始日期
            end_date: 结束日期
            initial_capital: 初始资金
            final_capital: 最终资金
            total_profit: 总收益
            total_profit_rate: 总收益率
            strategy_id: 策略ID
            strategy_name: 策略名称
            
        Returns:
            int: 回测ID，如果保存失败则返回None
        """
        try:
            if not self.db_connector:
                print("无法保存回测结果：数据库连接器未初始化")
                return None
                
            conn = self.db_connector.get_connection()
            if not conn:
                print("无法获取数据库连接")
                return None
                
            try:
                cursor = conn.cursor()
                
                # 插入回测结果
                cursor.execute("""
                    INSERT INTO backtest_results 
                    (stock_code, start_date, end_date, initial_capital, final_capital, 
                    total_profit, total_profit_rate, backtest_time, strategy_id, strategy_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s)
                    RETURNING id
                """, (
                    fund_code, start_date, end_date, initial_capital, final_capital,
                    total_profit, total_profit_rate, strategy_id, strategy_name
                ))
                
                # 获取新插入的回测ID
                backtest_id = cursor.fetchone()[0]
                
                # 提交事务
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
            print(f"保存回测结果过程中出错: {str(e)}")
            traceback.print_exc()
            return None 

    def _find_closest_date(self, target_date, date_list):
        """找到列表中与目标日期最接近的日期
        
        Args:
            target_date: 目标日期
            date_list: 日期列表
            
        Returns:
            最接近的日期
        """
        try:
            if not date_list:
                return None
                
            # 将target_date确保为datetime对象
            if isinstance(target_date, str):
                try:
                    target_date = pd.to_datetime(target_date)
                except:
                    print(f"无法将目标日期转换为datetime: {target_date}")
                    return None
                
            # 创建一个有效的datetime列表用于比较
            valid_dates = []
            valid_indices = []
            
            for i, d in enumerate(date_list):
                # 跳过非日期类型
                if not hasattr(d, 'strftime') and not isinstance(d, str):
                    continue
                    
                # 将字符串转换为datetime
                if isinstance(d, str):
                    try:
                        dt = pd.to_datetime(d)
                        valid_dates.append(dt)
                        valid_indices.append(i)
                    except:
                        continue
                else:
                    # 已经是datetime类型
                    valid_dates.append(d)
                    valid_indices.append(i)
            
            if not valid_dates:
                print(f"没有找到有效的日期进行比较")
                return None
                
            # 找到时间差最小的日期
            min_diff = float('inf')
            closest_idx = -1
            
            for i, dt in enumerate(valid_dates):
                # 安全地计算时间差（秒）
                try:
                    if hasattr(dt, 'timestamp') and hasattr(target_date, 'timestamp'):
                        diff = abs(dt.timestamp() - target_date.timestamp())
                        if diff < min_diff:
                            min_diff = diff
                            closest_idx = i
                except Exception as e:
                    print(f"计算时间差异时出错: {str(e)}, 类型: {type(dt)} vs {type(target_date)}")
                    continue
            
            if closest_idx >= 0:
                orig_idx = valid_indices[closest_idx]
                print(f"找到最接近日期: 目标={target_date}, 匹配={date_list[orig_idx]}, 差异={min_diff}秒")
                return date_list[orig_idx]
            
            return None
            
        except Exception as e:
            print(f"查找最接近日期出错: {str(e)}")
            return None 

    def _get_relative_position(self, signal_time, date_list):
        """计算信号时间在日期列表中的相对位置（0.0到1.0之间）
        
        Args:
            signal_time: 信号时间
            date_list: 日期列表
            
        Returns:
            相对位置（0.0-1.0之间的浮点数），如果无法计算则返回-1
        """
        try:
            if not date_list or len(date_list) == 0:
                return -1
                
            # 确保信号时间是datetime对象
            if isinstance(signal_time, str):
                try:
                    signal_time = pd.to_datetime(signal_time)
                except:
                    return -1
            
            # 获取列表中第一个和最后一个有效日期
            first_date = None
            last_date = None
            
            for date in date_list:
                if hasattr(date, 'strftime') or isinstance(date, str):
                    try:
                        if isinstance(date, str):
                            dt = pd.to_datetime(date)
                        else:
                            dt = date
                            
                        if first_date is None or dt < first_date:
                            first_date = dt
                        if last_date is None or dt > last_date:
                            last_date = dt
                    except:
                        continue
            
            if first_date is None or last_date is None:
                return -1
                
            # 确保信号时间在日期范围内
            if signal_time < first_date:
                return 0.0  # 如果信号时间早于第一个日期，放在开头
            if signal_time > last_date:
                return 1.0  # 如果信号时间晚于最后一个日期，放在结尾
            
            # 计算相对位置
            total_range = (last_date - first_date).total_seconds()
            if total_range == 0:
                return 0.5  # 避免除以零
                
            signal_position = (signal_time - first_date).total_seconds()
            rel_pos = signal_position / total_range
            
            return min(max(rel_pos, 0.0), 1.0)  # 确保结果在0到1之间
            
        except Exception as e:
            print(f"计算相对位置出错: {str(e)}")
            return -1

    def _compare_date_formats(self, signal_date, chart_date):
        """比较信号日期和图表日期的格式
        
        Args:
            signal_date: 信号日期
            chart_date: 图表日期
            
        Returns:
            bool: 日期是否匹配
        """
        try:
            # 转换为字符串格式比较（只保留日期部分）
            signal_str = None
            chart_str = None
            
            # 处理信号日期
            if hasattr(signal_date, 'strftime'):
                signal_str = signal_date.strftime('%Y-%m-%d')
            elif isinstance(signal_date, str):
                if ' ' in signal_date:
                    signal_str = signal_date.split(' ')[0]
                elif 'T' in signal_date:
                    signal_str = signal_date.split('T')[0]
                else:
                    signal_str = signal_date[:10]
            else:
                signal_str = str(signal_date)[:10]
                
            # 处理图表日期
            if hasattr(chart_date, 'strftime'):
                chart_str = chart_date.strftime('%Y-%m-%d')
            elif isinstance(chart_date, str):
                if ' ' in chart_date:
                    chart_str = chart_date.split(' ')[0]
                elif 'T' in chart_date:
                    chart_str = chart_date.split('T')[0]
                else:
                    chart_str = chart_date[:10]
            else:
                chart_str = str(chart_date)[:10]
                
            # 比较日期字符串
            return signal_str == chart_str
            
        except Exception as e:
            print(f"比较日期格式错误: {str(e)}")
            return False

    def on_add_strategy(self):
        """添加策略按钮点击事件"""
        try:
            # 获取当前参数
            fund_display = self.fund_combo.currentText()
            fund_code = self.fund_combo.currentData()
            if not fund_code:
                fund_code = fund_display.split('.')[0]
                
            strategy_name = self.strategy_combo.currentText()
            strategy_id = self.strategy_combo.currentData()
            
            start_date = self.start_date_edit.date().toString("yyyy-MM-dd")
            end_date = self.end_date_edit.date().toString("yyyy-MM-dd")
            
            # 添加新的回测模块
            self.add_backtest_module(fund_display, strategy_name, start_date, end_date)
            
            # 更新状态栏
            self.statusBar.showMessage(f"已添加 {fund_display} 的回测模块")
        except Exception as e:
            print(f"添加策略处理错误: {str(e)}")
            traceback.print_exc()
            QMessageBox.warning(self, "添加策略错误", f"添加策略过程中发生错误: {str(e)}")

    def on_start_backtest(self):
        """开始回测按钮点击事件"""
        try:
            # 获取参数
            fund_display = self.fund_combo.currentText()
            fund_code = self.fund_combo.currentData()
            if not fund_code:
                fund_code = fund_display.split('.')[0]
                
            data_display = self.data_combo.currentText()
            data_granularity = self.data_combo.currentData()
            if not data_granularity:
                data_granularity = data_display
                if data_display == "日线":
                    data_granularity = "day"
                    
            strategy_name = self.strategy_combo.currentText()
            strategy_id = self.strategy_combo.currentData()
            
            start_date = self.start_date_edit.date().toString("yyyy-MM-dd")
            end_date = self.end_date_edit.date().toString("yyyy-MM-dd")
            
            # 打印参数信息
            print(f"开始加载行情数据:")
            print(f"  基金: {fund_display} (代码: {fund_code})")
            print(f"  数据粒度: {data_granularity} (显示为: {data_display})")
            print(f"  时间范围: {start_date} 至 {end_date}")
            
            # 更新状态栏
            self.statusBar.showMessage(f"正在加载 {fund_display} 的行情数据...")
            
            # 从数据库加载行情数据
            pure_code = fund_code.split('.')[0] if '.' in fund_code else fund_code
            print(f"准备加载数据: 代码={pure_code}, 粒度={data_granularity}, 开始={start_date}, 结束={end_date}")
            
            stock_data = self.data_loader.load_stock_data(
                pure_code, 
                data_granularity, 
                start_date, 
                end_date
            )
            
            # 检查加载的数据
            if stock_data is None:
                print("加载的数据为None")
                QMessageBox.warning(self, "数据错误", f"无法加载 {fund_display} 的行情数据，请确保数据已导入")
                self.statusBar.showMessage(f"回测失败: 无法加载行情数据")
                return
            elif len(stock_data) == 0:
                print("加载的数据为空DataFrame")
                QMessageBox.warning(self, "数据错误", f"加载的 {fund_display} 行情数据为空，请确保选择了正确的时间范围")
                self.statusBar.showMessage(f"回测失败: 行情数据为空")
                return
            
            # 设置数据级别属性
            if not hasattr(stock_data, 'attrs'):
                stock_data.attrs = {}
            stock_data.attrs['data_level'] = data_granularity
            print(f"设置股票数据的数据级别属性: {data_granularity}")
            
            # 打印数据信息
            print(f"成功加载数据: {len(stock_data)} 条记录")
            print(f"数据列: {stock_data.columns.tolist()}")
            print(f"数据前5行:\n{stock_data.head()}")
            
            # 查找是否已经有对应的策略模块
            existing_module = None
            for i in range(self.strategy_container_layout.count() - 1):  # -1是因为最后一个是stretch
                item = self.strategy_container_layout.itemAt(i)
                if item and item.widget():
                    module = item.widget()
                    if hasattr(module, 'result_text'):
                        info_text = module.findChild(QLabel).text()
                        if f"基金: {fund_display}" in info_text and f"策略: {strategy_name}" in info_text:
                            existing_module = module
                            break
            
            # 如果没有找到现有模块，则创建新的
            if existing_module is None:
                module = self.create_backtest_module(fund_display, strategy_name, start_date, end_date)
                # 添加到容器中，确保添加在stretch之前
                self.strategy_container_layout.insertWidget(
                    self.strategy_container_layout.count() - 1,  # 在stretch之前插入
                    module
                )
                
                # 让模块之间有更大的间距
                if self.strategy_container_layout.count() > 2:  # 如果有多个模块
                    self.strategy_container_layout.setSpacing(15)  # 增加模块间距
            else:
                # 使用现有模块
                module = existing_module
                print(f"使用已有的回测模块")
            
            # 初始化波段策略
            from backtest_gui.strategy.band_strategy import BandStrategy
            band_strategy = BandStrategy(fund_code=pure_code, db_connector=self.db_connector)
            
            # 导入回测工作线程
            from backtest_gui.utils.backtest_worker import BacktestWorker
            
            # 创建回测工作线程
            self.backtest_worker = BacktestWorker(
                module=module,
                stock_data=stock_data,
                band_strategy=band_strategy,
                db_connector=self.db_connector,
                pure_code=pure_code,
                start_date=start_date,
                end_date=end_date,
                strategy_id=strategy_id,
                strategy_name=strategy_name
            )
            
            # 连接信号
            self.backtest_worker.progress_signal.connect(self._on_backtest_progress)
            self.backtest_worker.chart_update_signal.connect(self._on_backtest_chart_update)
            self.backtest_worker.completed_signal.connect(self._on_backtest_completed)
            self.backtest_worker.error_signal.connect(self._on_backtest_error)
            self.backtest_worker.status_signal.connect(self.statusBar.showMessage)
            
            # 保存线程引用到模块，以便后续可以取消
            module.backtest_worker = self.backtest_worker
            
            # 打印日志，确认worker已保存到模块
            print(f"已将backtest_worker保存到模块: {id(self.backtest_worker)}")
            
            # 添加取消按钮
            if not hasattr(module, 'cancel_button'):
                from PyQt5.QtWidgets import QPushButton
                cancel_button = QPushButton("取消回测")
                cancel_button.clicked.connect(lambda: self._cancel_backtest(module))
                module.layout().addWidget(cancel_button)
                module.cancel_button = cancel_button
            
            # 更新模块状态
            module.result_text.setText("正在后台回测中...")
            
            # 启动工作线程
            self.backtest_worker.start()
            
            # 更新状态栏
            self.statusBar.showMessage(f"{fund_display} 的回测已在后台启动...")
            
        except Exception as e:
            print(f"开始回测处理错误: {str(e)}")
            traceback.print_exc()
            QMessageBox.warning(self, "回测错误", f"回测过程中发生错误: {str(e)}")

    def show_trade_details(self):
        """显示交易详情"""
        try:
            # 获取当前回测ID
            sender = self.sender()
            if not sender:
                return
                
            # 获取父模块
            parent_module = None
            parent = sender.parent()
            while parent:
                if hasattr(parent, 'backtest_id'):
                    parent_module = parent
                    break
                parent = parent.parent()
            
            if not parent_module or not hasattr(parent_module, 'backtest_id'):
                QMessageBox.warning(self, "交易详情", "无法找到当前回测信息")
                return
                
            backtest_id = parent_module.backtest_id
            
            # 打开交易报告窗口
            from backtest_gui.gui.trade_report_window import TradeReportWindow
            
            # 创建交易报告窗口
            report_window = TradeReportWindow(self.db_connector)
            
            # 直接加载该回测ID的交易记录
            report_window.load_backtest_summary(backtest_id=backtest_id)
            report_window.load_paired_trades(backtest_id=backtest_id)
            
            # 显示窗口
            report_window.show()
            
            # 保存窗口引用，防止被垃圾回收
            self.report_window = report_window
            
        except Exception as e:
            print(f"显示交易详情错误: {str(e)}")
            traceback.print_exc()
            QMessageBox.warning(self, "显示错误", f"显示交易详情失败: {str(e)}")

    def toggle_signals_visibility(self, module):
        """切换买卖信号的显示状态
        
        Args:
            module: 回测模块
        """
        try:
            print("\n===== 开始切换信号显示状态 =====")
            print(f"当前信号显示状态: {'显示' if self.show_signals else '隐藏'}")
            
            # 获取模块中的信号切换按钮
            if not hasattr(module, 'signal_toggle_btn'):
                print("模块没有signal_toggle_btn属性，尝试在子控件中查找按钮...")
                for child in module.findChildren(QPushButton):
                    if child.text() in ["显示信号", "隐藏信号"]:
                        module.signal_toggle_btn = child
                        print(f"找到信号切换按钮: {child.text()}")
                        break
            
            if not hasattr(module, 'signal_toggle_btn'):
                print("错误: 未找到信号切换按钮")
                return
            else:
                print(f"当前按钮文本: {module.signal_toggle_btn.text()}")
                
            # 切换显示状态
            self.show_signals = not self.show_signals
            print(f"切换后的信号显示状态: {'显示' if self.show_signals else '隐藏'}")
            
            # 更新按钮文本
            module.signal_toggle_btn.setText("显示信号" if not self.show_signals else "隐藏信号")
            print(f"已更新按钮文本为: {module.signal_toggle_btn.text()}")
            
            # 查找chart_widget属性
            chart_widget = None
            if hasattr(module, 'chart_widget'):
                chart_widget = module.chart_widget
                print("直接从模块获取到chart_widget属性")
            else:
                # 尝试在子控件中查找FigureCanvas
                print("在模块子控件中查找FigureCanvas...")
                for child in module.findChildren(FigureCanvas):
                    chart_widget = child
                    module.chart_widget = child  # 保存引用以便后续使用
                    print("在子控件中找到FigureCanvas并保存为chart_widget属性")
                    break
            
            if not chart_widget:
                print("错误: 未找到图表控件")
                return
            
            # 查找backtest_worker属性
            backtest_worker = None
            if hasattr(module, 'backtest_worker'):
                backtest_worker = module.backtest_worker
                print(f"直接从模块获取到backtest_worker属性: ID={id(backtest_worker)}")
            else:
                print("模块没有backtest_worker属性")
                return
            
            # 检查worker是否有必要的数据
            if not hasattr(backtest_worker, 'stock_data') or not hasattr(backtest_worker, 'buy_signals') or not hasattr(backtest_worker, 'sell_signals'):
                print("错误: backtest_worker缺少必要的数据属性")
                return
            
            # 获取数据
            stock_data = backtest_worker.stock_data
            buy_signals = backtest_worker.buy_signals
            sell_signals = backtest_worker.sell_signals
            
            print(f"准备重绘图表:")
            print(f"  - stock_data: {len(stock_data) if stock_data is not None else 'None'} 条记录")
            print(f"  - buy_signals: {len(buy_signals) if buy_signals else 0} 个")
            print(f"  - sell_signals: {len(sell_signals) if sell_signals else 0} 个")
            
            # 重新绘制图表
            print("调用update_backtest_chart重绘图表...")
            self.update_backtest_chart(module, stock_data, len(stock_data)-1, buy_signals, sell_signals)
            
            # 更新状态栏
            signal_status = "显示" if self.show_signals else "隐藏"
            self.statusBar.showMessage(f"已{signal_status}买卖信号")
            print(f"已更新状态栏消息: 已{signal_status}买卖信号")
            
            print("===== 信号显示状态切换完成 =====\n")
        except Exception as e:
            print(f"切换信号显示状态错误: {str(e)}")
            traceback.print_exc()

