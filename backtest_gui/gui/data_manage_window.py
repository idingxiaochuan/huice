#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据整理模块 - 管理波段策略和基金绑定关系
"""
import os
import sys
import time
import json
import datetime
import traceback
import copy
from datetime import datetime
from PyQt5.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, 
    QPushButton, QLabel, QLineEdit, QComboBox, QTableWidget, 
    QTableWidgetItem, QHeaderView, QMessageBox, QDialog, 
    QDialogButtonBox, QFormLayout, QDoubleSpinBox, QSpinBox,
    QTabWidget, QGroupBox, QSplitter, QScrollArea, QFrame, QTextEdit,
    QProgressBar, QAbstractItemView, QFileDialog
)
from PyQt5.QtCore import Qt, QDateTime, pyqtSignal, pyqtSlot, QThread, QObject
from PyQt5.QtGui import QColor, QBrush, QFont
from PyQt5.QtWidgets import QApplication
import pandas as pd
import numpy as np

from backtest_gui.utils.db_connector import DBConnector as DatabaseConnectionPool, get_database_connection
from backtest_gui.strategy.band_strategy import GridLevel, BandStrategy
from backtest_gui.gui.components.fund_selector import FundSelectorWidget
from backtest_gui.utils.qmt_path_finder import find_qmt_path

# 导入新的数据获取模块
from backtest_gui.fund_data_fetcher import FundDataFetcher


class BandStrategyEditor(QDialog):
    """波段策略编辑器对话框"""
    
    def __init__(self, parent=None, strategy_data=None, fund_code=None):
        """初始化波段策略编辑器
        
        Args:
            parent: 父窗口
            strategy_data: 策略数据，如果为None则为新建策略
            fund_code: 基金代码
        """
        super().__init__(parent)
        
        self.strategy_data = strategy_data
        self.fund_code = fund_code
        self.grid_levels = []
        
        if strategy_data:
            self.strategy_id = strategy_data.get('strategy_id')
            self.strategy_name = strategy_data.get('strategy_name')
            self.description = strategy_data.get('description')
            self.grid_levels = strategy_data.get('grid_levels', [])
        else:
            self.strategy_id = None
            self.strategy_name = ""
            self.description = ""
            
        # 初始化UI
        self.init_ui()
        
        # 如果是编辑模式，加载数据
        if self.strategy_data:
            self.load_strategy_data()
    
    def init_ui(self):
        """初始化用户界面"""
        self.setWindowTitle("波段策略编辑器")
        self.setMinimumWidth(800)
        self.setMinimumHeight(600)
        
        # 创建主布局
        main_layout = QVBoxLayout(self)
        
        # 创建基本信息表单
        form_group = QGroupBox("基本信息")
        form_layout = QFormLayout(form_group)
        
        # 策略名称
        self.name_edit = QLineEdit(self.strategy_name)
        form_layout.addRow("策略名称:", self.name_edit)
        
        # 基金代码
        self.fund_code_combo = QComboBox()
        if self.fund_code:
            self.fund_code_combo.addItem(self.fund_code)
        form_layout.addRow("基金代码:", self.fund_code_combo)
        
        # 策略描述
        self.description_edit = QLineEdit(self.description)
        form_layout.addRow("策略描述:", self.description_edit)
        
        main_layout.addWidget(form_group)
        
        # 创建网格级别表格
        grid_group = QGroupBox("网格级别配置")
        grid_layout = QVBoxLayout(grid_group)
        
        # 表格
        self.grid_table = QTableWidget()
        self.grid_table.setColumnCount(7)
        self.grid_table.setHorizontalHeaderLabels([
            "级别", "类型", "买入价格", "卖出价格", "买入数量", "卖出数量", "操作"
        ])
        self.grid_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        grid_layout.addWidget(self.grid_table)
        
        # 添加按钮
        button_layout = QHBoxLayout()
        self.add_level_btn = QPushButton("添加级别")
        self.add_level_btn.clicked.connect(self.add_grid_level)
        button_layout.addWidget(self.add_level_btn)
        
        # 添加从Excel导入按钮
        self.import_excel_btn = QPushButton("从Excel导入")
        self.import_excel_btn.clicked.connect(self.import_from_excel)
        button_layout.addWidget(self.import_excel_btn)
        
        # 添加创建Excel模板按钮
        self.create_template_btn = QPushButton("创建Excel模板")
        self.create_template_btn.clicked.connect(self.create_excel_template)
        button_layout.addWidget(self.create_template_btn)
        
        # 添加导出到Excel按钮
        self.export_excel_btn = QPushButton("导出到Excel")
        self.export_excel_btn.clicked.connect(self.export_to_excel)
        button_layout.addWidget(self.export_excel_btn)
        
        grid_layout.addLayout(button_layout)
        
        main_layout.addWidget(grid_group)
        
        # 对话框按钮
        button_box = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        main_layout.addWidget(button_box)
        
        # 加载初始网格级别
        self.update_grid_table()

    def import_from_excel(self):
        """从Excel导入网格配置数据"""
        try:
            import pandas as pd
            from PyQt5.QtWidgets import QFileDialog, QMessageBox
            
            # 打开文件选择对话框
            file_path, _ = QFileDialog.getOpenFileName(
                self, "选择Excel文件", "", "Excel Files (*.xlsx *.xls);;All Files (*)"
            )
            
            if not file_path:
                return  # 用户取消了选择
                
            # 读取Excel文件
            try:
                df = pd.read_excel(file_path)
                
                # 定义列名映射（支持多种常见格式）
                column_mappings = {
                    "级别": ["级别", "level", "等级", "网格级别", "网格等级", "序号"],
                    "买入价": ["买入价", "buy_price", "买入价格", "买价", "买入", "买入点"],
                    "卖出价": ["卖出价", "sell_price", "卖出价格", "卖价", "卖出", "卖出点"],
                    "买入股数": ["买入股数", "buy_shares", "买入数量", "买入份额", "买入份数", "买入股票数"],
                    "卖出股数": ["卖出股数", "sell_shares", "卖出数量", "卖出份额", "卖出份数", "卖出股票数"],
                    "类型": ["类型", "grid_type", "网格类型", "类别", "type"]
                }
                
                # 尝试自动匹配列
                actual_columns = {}
                df_columns = list(df.columns)
                
                for target_col, possible_names in column_mappings.items():
                    # 尝试精确匹配
                    found = False
                    for name in possible_names:
                        if name in df_columns:
                            actual_columns[target_col] = name
                            found = True
                            break
                            
                    # 如果没有精确匹配，尝试部分匹配
                    if not found:
                        for col in df_columns:
                            for name in possible_names:
                                if name in col or col in name:
                                    actual_columns[target_col] = col
                                    found = True
                                    break
                            if found:
                                break
                
                # 检查必要的列是否匹配成功
                required_columns = ["买入价", "卖出价", "买入股数", "卖出股数"]
                missing_columns = [col for col in required_columns if col not in actual_columns]
                
                if missing_columns:
                    message = f"Excel文件缺少必要的列: {', '.join(missing_columns)}\n\n可能的列名包括:\n"
                    for col in missing_columns:
                        message += f"- {col}: {', '.join(column_mappings[col])}\n"
                    
                    QMessageBox.warning(self, "格式错误", message)
                    return
                    
                # 清空现有网格级别
                self.grid_levels = []
                
                # 将Excel数据导入到网格级别
                for idx, row in df.iterrows():
                    try:
                        # 处理级别列
                        if "级别" in actual_columns:
                            level = int(row[actual_columns["级别"]])
                        else:
                            level = idx + 1  # 默认使用行号+1作为级别
                        
                        # 处理买入价和卖出价
                        buy_price = float(row[actual_columns["买入价"]])
                        sell_price = float(row[actual_columns["卖出价"]])
                        
                        # 处理买入和卖出股数
                        buy_shares = float(row[actual_columns["买入股数"]])
                        sell_shares = float(row[actual_columns["卖出股数"]])
                        
                        # 处理类型
                        if "类型" in actual_columns:
                            grid_type = str(row[actual_columns["类型"]])
                        else:
                            grid_type = "NORMAL"  # 默认类型
                            
                        # 验证数据
                        if buy_price <= 0 or sell_price <= 0:
                            print(f"行 {idx+1}: 买入价或卖出价必须为正数")
                            continue
                            
                        if buy_shares <= 0 or sell_shares <= 0:
                            print(f"行 {idx+1}: 买入数量或卖出数量必须为正数")
                            continue
                        
                        # 创建网格级别对象
                        grid_level = GridLevel(
                            level=level,
                            grid_type=grid_type,
                            buy_price=buy_price,
                            sell_price=sell_price,
                            buy_shares=buy_shares,
                            sell_shares=sell_shares
                        )
                        
                        self.grid_levels.append(grid_level)
                        
                    except (ValueError, KeyError) as e:
                        print(f"导入第{idx+1}行时出错: {str(e)}")
                        continue
                
                # 确保级别编号是连续的
                self.grid_levels.sort(key=lambda x: x.buy_price, reverse=True)  # 按买入价格降序排序
                for i, grid in enumerate(self.grid_levels):
                    grid.level = i + 1
                        
                # 更新表格
                self.update_grid_table()
                
                # 显示成功消息
                QMessageBox.information(
                    self, 
                    "导入成功", 
                    f"成功从Excel导入 {len(self.grid_levels)} 个网格级别"
                )
                
            except Exception as e:
                import traceback
                traceback.print_exc()
                QMessageBox.critical(
                    self, 
                    "导入错误", 
                    f"读取Excel文件失败: {str(e)}"
                )
                
        except ImportError:
            QMessageBox.critical(
                self, 
                "导入错误", 
                "缺少必要的库支持，请安装pandas: pip install pandas openpyxl"
            )
    
    def load_strategy_data(self):
        """加载策略数据到界面"""
        if not self.strategy_data:
            return
            
        # 加载基本信息
        self.name_edit.setText(self.strategy_name)
        self.description_edit.setText(self.description)
        
        # 更新网格级别表格
        self.update_grid_table()
    
    def update_grid_table(self):
        """更新网格级别表格"""
        self.grid_table.setRowCount(0)
        
        for level in self.grid_levels:
            row = self.grid_table.rowCount()
            self.grid_table.insertRow(row)
            
            # 级别
            self.grid_table.setItem(row, 0, QTableWidgetItem(str(level.level)))
            
            # 类型
            self.grid_table.setItem(row, 1, QTableWidgetItem(level.grid_type))
            
            # 买入价格
            self.grid_table.setItem(row, 2, QTableWidgetItem(str(level.buy_price)))
            
            # 卖出价格
            self.grid_table.setItem(row, 3, QTableWidgetItem(str(level.sell_price)))
            
            # 买入数量
            self.grid_table.setItem(row, 4, QTableWidgetItem(str(level.buy_shares)))
            
            # 卖出数量
            self.grid_table.setItem(row, 5, QTableWidgetItem(str(level.sell_shares)))
            
            # 操作按钮
            delete_btn = QPushButton("删除")
            delete_btn.clicked.connect(lambda _, r=row: self.remove_grid_level(r))
            self.grid_table.setCellWidget(row, 6, delete_btn)
    
    def add_grid_level(self):
        """添加网格级别"""
        # 获取当前最大级别
        max_level = 1
        if self.grid_levels:
            max_level = max(level.level for level in self.grid_levels) + 1
            
        # 创建新级别
        new_level = GridLevel(
            level=max_level,
            grid_type="NORMAL",
            buy_price=0.5,
            sell_price=0.55,
            buy_shares=1000,
            sell_shares=1000
        )
        
        # 添加到列表
        self.grid_levels.append(new_level)
        
        # 更新表格
        self.update_grid_table()
    
    def remove_grid_level(self, row):
        """删除网格级别
        
        Args:
            row: 表格行索引
        """
        if 0 <= row < len(self.grid_levels):
            del self.grid_levels[row]
            self.update_grid_table()
    
    def accept(self):
        """保存策略数据"""
        # 验证输入
        if not self.validate_inputs():
            return
            
        # 更新策略数据
        self.strategy_name = self.name_edit.text()
        self.description = self.description_edit.text()
        self.fund_code = self.fund_code_combo.currentText()
        
        # 更新网格级别数据
        for row in range(self.grid_table.rowCount()):
            level = int(self.grid_table.item(row, 0).text())
            grid_type = self.grid_table.item(row, 1).text()
            buy_price = float(self.grid_table.item(row, 2).text())
            sell_price = float(self.grid_table.item(row, 3).text())
            buy_shares = float(self.grid_table.item(row, 4).text())  # 改为float
            sell_shares = float(self.grid_table.item(row, 5).text())  # 改为float
            
            self.grid_levels[row] = GridLevel(
                level=level,
                grid_type=grid_type,
                buy_price=buy_price,
                sell_price=sell_price,
                buy_shares=buy_shares,
                sell_shares=sell_shares
            )
        
        # 调用父类方法关闭对话框
        super().accept()
    
    def validate_inputs(self):
        """验证输入数据
        
        Returns:
            bool: 是否验证通过
        """
        # 检查策略名称
        if not self.name_edit.text():
            QMessageBox.warning(self, "输入错误", "请输入策略名称")
            return False
            
        # 检查基金代码
        if not self.fund_code_combo.currentText():
            QMessageBox.warning(self, "输入错误", "请选择基金代码")
            return False
            
        # 检查网格级别
        if self.grid_table.rowCount() == 0:
            QMessageBox.warning(self, "输入错误", "请至少添加一个网格级别")
            return False
            
        # 检查网格级别数据
        for row in range(self.grid_table.rowCount()):
            try:
                level = int(self.grid_table.item(row, 0).text())
                buy_price = float(self.grid_table.item(row, 2).text())
                sell_price = float(self.grid_table.item(row, 3).text())
                buy_shares = float(self.grid_table.item(row, 4).text())  # 改为float
                sell_shares = float(self.grid_table.item(row, 5).text())  # 改为float
                
                # 检查买入价格小于卖出价格
                if buy_price >= sell_price:
                    QMessageBox.warning(self, "输入错误", f"第{row+1}行: 买入价格必须小于卖出价格")
                    return False
                    
                # 检查数量为正数
                if buy_shares <= 0 or sell_shares <= 0:
                    QMessageBox.warning(self, "输入错误", f"第{row+1}行: 买入和卖出数量必须为正数")
                    return False
            except ValueError:
                QMessageBox.warning(self, "输入错误", f"第{row+1}行: 数据格式错误")
                return False
                
        return True

    def create_excel_template(self):
        """创建Excel模板文件"""
        try:
            import pandas as pd
            from PyQt5.QtWidgets import QFileDialog, QMessageBox
            
            # 打开保存文件对话框
            file_path, _ = QFileDialog.getSaveFileName(
                self, "保存Excel模板", "网格策略模板.xlsx", "Excel Files (*.xlsx);;All Files (*)"
            )
            
            if not file_path:
                return  # 用户取消了保存
                
            # 创建示例数据
            data = {
                "级别": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
                "类型": ["NORMAL"] * 11,
                "买入价": [1.462, 1.345, 1.251, 1.176, 1.117, 1.061, 1.008, 0.958, 0.910, 0.864, 0.821],
                "买入金额": [1000.00, 1100.00, 1200.00, 1331.00, 1464.10, 1610.51, 1771.56, 1948.72, 2143.59, 2357.95, 2593.74],
                "买入股数": [700, 800, 1000, 1100, 1300, 1500, 1800, 2000, 2400, 2700, 3200],
                "卖出价": [1.579, 1.453, 1.338, 1.246, 1.173, 1.114, 1.059, 1.006, 0.955, 0.908, 0.862],
                "卖出股数": [600, 700, 800, 1000, 1100, 1300, 1500, 1700, 2000, 2300, 2700],
                "卖出金额": [947.40, 1016.87, 1070.78, 1246.41, 1290.21, 1448.56, 1587.84, 1709.57, 1910.70, 2087.44, 2327.95]
            }
            
            # 创建DataFrame
            df = pd.DataFrame(data)
            
            # 保存到Excel
            df.to_excel(file_path, index=False)
            
            # 显示成功消息
            QMessageBox.information(
                self, 
                "模板创建成功", 
                f"Excel模板已保存到: {file_path}\n\n"
                "请按照模板格式填写您的网格策略数据，然后使用'从Excel导入'功能导入。"
            )
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            QMessageBox.critical(
                self, 
                "创建模板失败", 
                f"创建Excel模板失败: {str(e)}"
            )

    def export_to_excel(self):
        """导出当前网格级别到Excel文件"""
        try:
            import pandas as pd
            from PyQt5.QtWidgets import QFileDialog, QMessageBox
            
            # 如果没有数据，提示用户
            if not self.grid_levels:
                QMessageBox.warning(self, "导出失败", "当前没有网格级别数据可导出")
                return
                
            # 默认文件名
            default_filename = f"{self.name_edit.text() or '网格策略'}.xlsx"
            
            # 打开保存文件对话框
            file_path, _ = QFileDialog.getSaveFileName(
                self, "导出到Excel", default_filename, "Excel Files (*.xlsx);;All Files (*)"
            )
            
            if not file_path:
                return  # 用户取消了保存
                
            # 收集数据
            data = {
                "级别": [],
                "类型": [],
                "买入价": [],
                "卖出价": [],
                "买入股数": [],
                "卖出股数": [],
                "买入金额": [],  # 计算值
                "卖出金额": []   # 计算值
            }
            
            # 填充数据
            for grid in self.grid_levels:
                data["级别"].append(grid.level)
                data["类型"].append(grid.grid_type)
                data["买入价"].append(grid.buy_price)
                data["卖出价"].append(grid.sell_price)
                data["买入股数"].append(grid.buy_shares)
                data["卖出股数"].append(grid.sell_shares)
                # 计算金额
                data["买入金额"].append(round(grid.buy_price * grid.buy_shares, 2))
                data["卖出金额"].append(round(grid.sell_price * grid.sell_shares, 2))
            
            # 创建DataFrame
            df = pd.DataFrame(data)
            
            # 保存到Excel
            df.to_excel(file_path, index=False)
            
            # 显示成功消息
            QMessageBox.information(
                self, 
                "导出成功", 
                f"网格级别数据已成功导出到: {file_path}"
            )
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            QMessageBox.critical(
                self, 
                "导出失败", 
                f"导出到Excel失败: {str(e)}"
            )


class DataManageWindow(QMainWindow):
    """数据整理窗口"""
    
    def __init__(self, config=None):
        """初始化数据管理窗口
        
        Args:
            config: 配置对象
        """
        super().__init__()
        
        # 保存配置
        self.config = config
        
        # 初始化变量
        self.conn = None
        self.funds = []  # 基金列表
        self.strategies = []  # 策略列表
        self.selected_fund = None
        self.data_fetcher = None
        
        # 创建控制台用于日志（避免初始化UI前就调用log）
        self.market_status_text = QTextEdit()
        self.market_status_text.setReadOnly(True)
        
        # 初始化UI（先创建界面元素）
        self.init_ui()
        
        # 初始化数据获取器
        self.data_fetcher = FundDataFetcher()
        
        # 初始化数据库连接器
        try:
            self.conn = get_database_connection()
            if self.conn:
                print("数据库连接成功")  # 使用print而非log，确保不依赖UI
                # 确保数据库表结构存在
                try:
                    cursor = self.conn.cursor()
                    self.ensure_tables_exist(cursor)
                    self.conn.commit()
                    print("数据库表结构已创建")
                except Exception as e:
                    print(f"创建数据库表结构时发生错误: {str(e)}")
                    traceback.print_exc()
                
                # 设置数据获取器的数据库连接
                if self.data_fetcher:
                    self.data_fetcher.set_db_connection(self.conn)
                # 设置基金选择器的数据库连接
                if hasattr(self, 'fund_selector') and self.fund_selector is not None:
                    self.fund_selector.db_connection = self.conn
                
                # 在UI和数据库都初始化后记录日志
                self.log("数据库连接成功")
            else:
                print("数据库连接失败")
                self.log("数据库连接失败")
        except Exception as e:
            print(f"数据库连接异常: {str(e)}")
            traceback.print_exc()
            # 在UI初始化后记录日志
            self.log(f"数据库连接异常: {str(e)}")
        
        # 连接信号
        if self.data_fetcher:
            self.data_fetcher.progress_signal.connect(self.on_progress_update)
            # 根据data_fetcher可能定义的信号名称适配
            if hasattr(self.data_fetcher, 'finished_signal'):
                self.data_fetcher.finished_signal.connect(self.on_data_fetch_finished)
            elif hasattr(self.data_fetcher, 'completed_signal'):
                self.data_fetcher.completed_signal.connect(self.on_data_fetch_completed)
            
            if hasattr(self.data_fetcher, 'error_signal'):
                self.data_fetcher.error_signal.connect(self.on_data_fetch_error)
        
        # 加载数据
        self.load_funds()
        self.load_band_strategies()
    
    def init_ui(self):
        """初始化UI"""
        # 窗口配置
        self.setWindowTitle("数据整理")
        self.resize(1200, 800)
        
        # 主布局
        main_layout = QVBoxLayout()
        main_widget = QWidget()
        main_widget.setLayout(main_layout)
        self.setCentralWidget(main_widget)
        
        # 顶部标题
        title_label = QLabel("波段策略管理")
        title_label.setStyleSheet("font-size: 16pt; font-weight: bold;")
        main_layout.addWidget(title_label)
        
        # 顶部控制区域 - 基金代码、数据级别、获取行情数据和进度条
        top_control_layout = QHBoxLayout()
        main_layout.addLayout(top_control_layout)
        
        # 基金代码输入
        fund_code_label = QLabel("基金代码:")
        top_control_layout.addWidget(fund_code_label)
        
        self.fund_code_input = QLineEdit()
        self.fund_code_input.setPlaceholderText("输入基金代码")
        self.fund_code_input.returnPressed.connect(self.on_get_data_clicked)
        self.fund_code_input.setMaximumWidth(int(self.width() * 0.2))  # 缩小为原来的1/5
        top_control_layout.addWidget(self.fund_code_input)
        
        # 数据级别选择
        level_label = QLabel("数据级别")
        top_control_layout.addWidget(level_label)
        
        self.level_selector = QComboBox()
        # 添加所有可能的数据级别
        all_data_levels = ['1min', '5min', '15min', '30min', '60min', 'day', 'week', 'month']
        self.level_selector.addItems(all_data_levels)
        top_control_layout.addWidget(self.level_selector)
        
        # 获取行情数据按钮
        self.get_data_btn = QPushButton("获取基金行情")
        self.get_data_btn.clicked.connect(self.on_get_data_clicked)
        top_control_layout.addWidget(self.get_data_btn)
        
        # 进度条 - 增加宽度
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        top_control_layout.addWidget(self.progress_bar, 1)  # 设置为可伸缩，占据剩余空间
        
        # 主内容区域
        content_layout = QHBoxLayout()
        main_layout.addLayout(content_layout, 1)  # 设置为可伸缩
        
        # 左侧区域 (2/5宽度) - 行情数据状态和波段策略
        left_layout = QVBoxLayout()
        left_widget = QWidget()
        left_widget.setLayout(left_layout)
        content_layout.addWidget(left_widget, 2)  # 占2/5宽度
        
        # 行情数据状态
        import_group = QGroupBox("行情数据状态")
        import_layout = QVBoxLayout(import_group)
        if not hasattr(self, 'market_status_text'):
            self.market_status_text = QTextEdit()
            self.market_status_text.setReadOnly(True)
        import_layout.addWidget(self.market_status_text)
        left_layout.addWidget(import_group)
        
        # 波段策略区域
        strategy_group = QGroupBox("波段策略")
        strategy_layout = QVBoxLayout(strategy_group)
        left_layout.addWidget(strategy_group)
        
        # 策略表格
        self.strategy_table = QTableWidget()
        self.strategy_table.setColumnCount(3)
        self.strategy_table.setHorizontalHeaderLabels(["名称", "描述", "创建时间"])
        self.strategy_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.strategy_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.strategy_table.horizontalHeader().setStretchLastSection(True)
        strategy_layout.addWidget(self.strategy_table)
        
        # 策略操作按钮
        strategy_btn_layout = QHBoxLayout()
        strategy_layout.addLayout(strategy_btn_layout)
        
        create_strategy_btn = QPushButton("创建策略")
        create_strategy_btn.clicked.connect(self.create_band_strategy)
        strategy_btn_layout.addWidget(create_strategy_btn)
        
        edit_strategy_btn = QPushButton("编辑策略")
        edit_strategy_btn.clicked.connect(self.edit_band_strategy)
        strategy_btn_layout.addWidget(edit_strategy_btn)
        
        delete_strategy_btn = QPushButton("删除策略")
        delete_strategy_btn.clicked.connect(self.delete_band_strategy)
        strategy_btn_layout.addWidget(delete_strategy_btn)
        
        # 右侧区域 (3/5宽度) - 基金选择和网格级别详情
        right_layout = QVBoxLayout()
        right_widget = QWidget()
        right_widget.setLayout(right_layout)
        content_layout.addWidget(right_widget, 3)  # 占3/5宽度
        
        # 基金选择区域
        fund_group = QGroupBox("基金选择")
        fund_layout = QVBoxLayout(fund_group)
        right_layout.addWidget(fund_group)
        
        # 创建基金选择器（不显示基金代码和数据级别输入框）
        self.fund_selector = FundSelectorWidget(parent=self)
        fund_layout.addWidget(self.fund_selector)
        
        # 删除行情数据按钮
        self.delete_data_btn = QPushButton("删除行情数据")
        self.delete_data_btn.clicked.connect(self.on_delete_data_clicked)
        fund_layout.addWidget(self.delete_data_btn)
        
        # 网格详情区域
        grid_group = QGroupBox("网格级别详情")
        grid_layout = QVBoxLayout(grid_group)
        right_layout.addWidget(grid_group)
        
        # 网格表格
        self.grid_table = QTableWidget()
        self.grid_table.setColumnCount(6)
        self.grid_table.setHorizontalHeaderLabels(["级别", "类型", "买入价格", "卖出价格", "买入数量", "卖出数量"])
        self.grid_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.grid_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        grid_layout.addWidget(self.grid_table)
        
        # 初始化信号
        self.fund_selector.fund_selected.connect(self.on_fund_selected)
        self.fund_selector.fund_level_selected.connect(self.on_fund_level_selected)
        # 信号连接将在__init__中完成，因为data_fetcher还没初始化
        
        # 将顶部输入框与fund_selector同步
        self.fund_code_input.textChanged.connect(self.sync_fund_code_to_selector)
        
        # 加载数据
        self.load_funds()
        
    def sync_fund_code_to_selector(self, text):
        """将顶部基金代码输入框的值同步到fund_selector"""
        if hasattr(self, 'fund_selector') and self.fund_selector is not None:
            self.fund_selector.search_input.setText(text)
    
    def load_funds(self):
        """加载基金列表"""
        if not self.conn:
            self.log("数据库未连接，无法加载基金列表")
            return
            
        try:
            # 让fund_selector自己加载基金列表
            if hasattr(self, 'fund_selector') and self.fund_selector is not None:
                self.fund_selector.load_funds()
                self.log("已加载基金列表")
                
        except Exception as e:
            self.log(f"查询基金列表错误: {str(e)}")
            traceback.print_exc()
            QMessageBox.warning(self, "加载错误", f"加载基金列表失败: {str(e)}")
    
    def load_band_strategies(self, fund_code=None):
        """加载波段策略列表
        
        Args:
            fund_code: 基金代码，如果为None则使用当前选中的基金
        """
        if not self.conn:
            return
            
        try:
            # 获取当前选择的基金
            if not fund_code:
                fund_code, _ = self.fund_selector.get_selected_fund()
                
            if not fund_code:
                return
                
            # 处理基金代码，移除.SH或.SZ后缀
            pure_fund_code = fund_code.split('.')[0] if '.' in fund_code else fund_code
                
            self.log(f"加载基金 {fund_code} 的波段策略")
                
            # 清空策略列表
            self.strategies = []
            self.strategy_table.setRowCount(0)
            
            # 清空网格表格
            self.grid_table.setRowCount(0)
            
            # 获取数据库连接
            cursor = self.conn.cursor()
            
            # 查询策略列表 - 只查询绑定到当前基金的策略
            # 使用纯代码查询，同时也尝试查询带后缀的代码
            cursor.execute("""
                SELECT bs.id, bs.name, bs.description, bs.created_at, fs.is_default
                FROM band_strategies bs
                JOIN fund_strategy_bindings fs ON bs.id = fs.strategy_id
                WHERE fs.fund_code = %s OR fs.fund_code = %s
                ORDER BY fs.is_default DESC, bs.name
            """, (pure_fund_code, fund_code))
            
            strategies = cursor.fetchall()
            
            if not strategies:
                self.log(f"未找到基金 {fund_code} 的波段策略")
                return
                
            # 将策略添加到表格
            for i, strategy in enumerate(strategies):
                strategy_id = strategy[0]
                strategy_name = strategy[1]
                strategy_desc = strategy[2]
                created_at = strategy[3]
                is_default = strategy[4]
                
                # 加载网格级别
                cursor.execute("""
                    SELECT level, grid_type, buy_price, sell_price, buy_shares, sell_shares
                    FROM grid_levels
                    WHERE strategy_id = %s
                    ORDER BY level
                """, (strategy_id,))
                
                grid_levels = []
                for grid_row in cursor.fetchall():
                    level, grid_type, buy_price, sell_price, buy_shares, sell_shares = grid_row
                    
                    grid_level = GridLevel(
                        level=level,
                        grid_type=grid_type,
                        buy_price=float(buy_price),
                        sell_price=float(sell_price),
                        buy_shares=float(buy_shares),
                        sell_shares=float(sell_shares)
                    )
                    
                    grid_levels.append(grid_level)
                
                # 添加到策略列表
                self.strategies.append({
                    'strategy_id': strategy_id,
                    'strategy_name': strategy_name,
                    'strategy_desc': strategy_desc,
                    'created_at': created_at,
                    'is_default': is_default,
                    'grid_levels': grid_levels
                })
                
                # 添加到表格
                self.strategy_table.insertRow(i)
                
                # 策略名称
                name_item = QTableWidgetItem(strategy_name)
                self.strategy_table.setItem(i, 0, name_item)
                
                # 策略描述
                desc_item = QTableWidgetItem(strategy_desc if strategy_desc else "")
                self.strategy_table.setItem(i, 1, desc_item)
                
                # 创建时间
                created_item = QTableWidgetItem(created_at.strftime("%Y-%m-%d %H:%M:%S"))
                self.strategy_table.setItem(i, 2, created_item)
            
            # 选择第一个策略
            if self.strategies:
                self.strategy_table.selectRow(0)
                self.load_strategy_details(self.strategies[0]['strategy_id'])
                
            self.log(f"已加载 {len(self.strategies)} 个波段策略")
                
        except Exception as e:
            self.log(f"加载波段策略失败: {str(e)}")
            traceback.print_exc()
    
    def load_strategy_details(self, strategy_id):
        """加载策略详情
        
        Args:
            strategy_id: 策略ID
        """
        try:
            # 清空网格表格
            self.grid_table.setRowCount(0)
            
            # 获取数据库连接
            cursor = self.conn.cursor()
            
            # 查询网格级别
            cursor.execute("""
                SELECT level, grid_type, buy_price, sell_price, buy_shares, sell_shares
                FROM grid_levels
                WHERE strategy_id = %s
                ORDER BY level
            """, (strategy_id,))
            
            grid_levels = cursor.fetchall()
            
            # 先清空策略对象中的网格级别列表，防止重复添加
            for strategy in self.strategies:
                if strategy['strategy_id'] == strategy_id:
                    strategy['grid_levels'] = []
            
            # 添加到表格
            for row in grid_levels:
                level, grid_type, buy_price, sell_price, buy_shares, sell_shares = row
                
                row_idx = self.grid_table.rowCount()
                self.grid_table.insertRow(row_idx)
                self.grid_table.setItem(row_idx, 0, QTableWidgetItem(str(level)))
                self.grid_table.setItem(row_idx, 1, QTableWidgetItem(grid_type))
                self.grid_table.setItem(row_idx, 2, QTableWidgetItem(str(buy_price)))
                self.grid_table.setItem(row_idx, 3, QTableWidgetItem(str(sell_price)))
                self.grid_table.setItem(row_idx, 4, QTableWidgetItem(str(buy_shares)))
                self.grid_table.setItem(row_idx, 5, QTableWidgetItem(str(sell_shares)))
                
                # 保存到策略对象
                for strategy in self.strategies:
                    if strategy['strategy_id'] == strategy_id:
                        strategy['grid_levels'].append(GridLevel(
                            level=level,
                            grid_type=grid_type,
                            buy_price=float(buy_price),
                            sell_price=float(sell_price),
                            buy_shares=float(buy_shares),
                            sell_shares=float(sell_shares)
                        ))
            
        except Exception as e:
            self.log(f"查询网格级别错误: {str(e)}")
            traceback.print_exc()
    
    def on_fund_selected(self, fund_code, fund_name):
        """当选择基金时触发
        
        Args:
            fund_code: 基金代码
            fund_name: 基金名称
        """
        self.log(f"已选择基金: {fund_code} - {fund_name}")
        
        # 更新顶部输入框
        self.fund_code_input.setText(fund_code)
        
        # 加载该基金的波段策略
        self.load_band_strategies(fund_code)
        
        # 检查该基金的可用数据级别并更新级别选择器
        self.update_data_levels_for_fund(fund_code)
    
    def update_data_levels_for_fund(self, fund_code):
        """更新指定基金的可用数据级别
        
        Args:
            fund_code: 基金代码
        """
        if not fund_code or not self.conn:
            return
            
        try:
            # 从fund_code中提取纯代码（去掉.SH或.SZ后缀）
            pure_code = fund_code.split('.')[0] if '.' in fund_code else fund_code
            
            # 查询该基金的可用数据级别
            cursor = self.conn.cursor()
            
            query = """
            SELECT data_level FROM stock_quotes 
            WHERE fund_code = %s
            GROUP BY data_level
            ORDER BY data_level
            """
            
            self.log(f"查询基金 {fund_code} 的数据级别...")
            cursor.execute(query, (pure_code,))
            available_data_levels = [row[0] for row in cursor.fetchall()]
            self.log(f"查询到基金 {fund_code} 的数据级别: {', '.join(available_data_levels) if available_data_levels else '无'}")
            
            # 获取当前选中的级别
            current_level = self.level_selector.currentText()
            
            # 所有可能的数据级别
            all_data_levels = ['1min', '5min', '15min', '30min', '60min', 'day', 'week', 'month']
            
            # 更新数据级别选择器，保持所有选项但突出显示可用的选项
            self.level_selector.blockSignals(True)  # 阻止信号触发
            
            # 为每个数据级别设置不同的样式
            for i in range(self.level_selector.count()):
                level = self.level_selector.itemText(i)
                if level in available_data_levels:
                    # 可用级别显示为正常字体
                    self.level_selector.setItemData(i, None, Qt.ForegroundRole)
                    self.level_selector.setItemData(i, QFont("Arial", 9, QFont.Normal), Qt.FontRole)
                else:
                    # 不可用级别显示为灰色斜体
                    self.level_selector.setItemData(i, QColor(150, 150, 150), Qt.ForegroundRole)
                    font = QFont("Arial", 9, QFont.Light)
                    font.setItalic(True)
                    self.level_selector.setItemData(i, font, Qt.FontRole)
            
            # 尝试选择之前选中的级别（如果可用）
            if current_level in available_data_levels:
                index = self.level_selector.findText(current_level)
                if index >= 0:
                    self.level_selector.setCurrentIndex(index)
            elif available_data_levels:
                # 如果之前选中的级别不可用，选择第一个可用级别
                index = self.level_selector.findText(available_data_levels[0])
                if index >= 0:
                    self.level_selector.setCurrentIndex(index)
                    
            self.log(f"已更新基金 {fund_code} 的数据级别，当前选择: {self.level_selector.currentText()}")
                
            self.level_selector.blockSignals(False)  # 恢复信号
            
        except Exception as e:
            self.log(f"更新数据级别失败: {str(e)}")
            traceback.print_exc()
            # 出错时回滚事务
            if self.conn:
                self.conn.rollback()
    
    def on_fund_level_selected(self, fund_code, fund_name, data_level):
        """当选择基金数据级别时触发
        
        Args:
            fund_code: 基金代码
            fund_name: 基金名称
            data_level: 数据级别
        """
        self.log(f"已选择基金: {fund_code} - {fund_name}, 数据级别: {data_level}")
        
        # 更新顶部输入框
        self.fund_code_input.setText(fund_code)
        
        # 从fund_code中提取纯代码
        pure_code = fund_code.split('.')[0] if '.' in fund_code else fund_code
        
        # 更新数据级别下拉框
        index = self.level_selector.findText(data_level)
        if index >= 0:
            self.level_selector.setCurrentIndex(index)
        
        # 验证该基金是否真的有这个数据级别
        try:
            cursor = self.conn.cursor()
            query = """
            SELECT COUNT(*) FROM stock_quotes 
            WHERE fund_code = %s AND data_level = %s
            """
            cursor.execute(query, (pure_code, data_level))
            count = cursor.fetchone()[0]
            
            if count > 0:
                # 如果确实存在这个数据级别，记录日志
                self.log(f"确认基金 {fund_code} 有 {data_level} 级别的数据，共 {count} 条")
            else:
                # 如果不存在这个数据级别，提示用户
                self.log(f"提示: 基金 {fund_code} 没有 {data_level} 级别的数据，获取行情数据后将自动创建")
            
            # 更新所有数据级别的显示样式
            self.update_data_levels_for_fund(fund_code)
        except Exception as e:
            self.log(f"验证数据级别失败: {str(e)}")
            traceback.print_exc()
            if self.conn:
                self.conn.rollback()
    
    def on_delete_data_clicked(self):
        """删除行情数据"""
        # 直接从输入框获取基金代码
        fund_code = self.get_fund_code_from_input()
        
        # 如果输入框为空，尝试从选择的行获取
        if not fund_code:
            fund_code, _ = self.fund_selector.get_selected_fund()
              
        if not fund_code:
            QMessageBox.warning(self, "选择错误", "请先输入或选择基金")
            return
        
        # 从fund_code中提取纯代码（去掉.SH或.SZ后缀）
        pure_code = fund_code.split('.')[0] if '.' in fund_code else fund_code
        
        # 获取当前选择的数据级别
        data_level = self.level_selector.currentText()
        
        if not data_level:
            QMessageBox.warning(self, "选择错误", "请先选择数据级别")
            return
        
        # 先查询该基金有哪些数据级别可用
        cursor = None
        try:
            # 开始新事务
            if self.conn:
                self.conn.rollback()  # 确保没有正在进行的事务
                
            self.log(f"查询基金 {fund_code} 的数据级别 {data_level} 是否存在...")
            cursor = self.conn.cursor()
            
            # 验证该数据级别是否存在
            validate_query = """
            SELECT COUNT(*) FROM stock_quotes 
            WHERE fund_code = %s AND data_level = %s
            """
            cursor.execute(validate_query, (pure_code, data_level))
            count = cursor.fetchone()[0]
            
            if count == 0:
                QMessageBox.information(self, "提示", f"未找到{fund_code}的{data_level}级别数据")
                return
                
            self.log(f"基金 {fund_code} 的{data_level}级别数据共有{count}条")
                
            # 确认删除
            reply = QMessageBox.question(self, "确认删除", 
                                        f"确定要删除{fund_code}的{data_level}级别数据吗？此操作不可恢复！", 
                                        QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
            
            if reply != QMessageBox.Yes:
                return
                
            # 查询要删除的数据数量
            count_query = """
            SELECT COUNT(*) FROM stock_quotes 
            WHERE fund_code = %s AND data_level = %s
            """
            cursor.execute(count_query, (pure_code, data_level))
            count_result = cursor.fetchone()
            data_count = count_result[0] if count_result else 0
            
            if data_count == 0:
                QMessageBox.information(self, "提示", f"未找到{fund_code}的{data_level}级别行情数据")
                return
                
            # 删除数据
            self.log(f"开始删除{fund_code}的{data_level}级别数据...")
            delete_query = """
            DELETE FROM stock_quotes 
            WHERE fund_code = %s AND data_level = %s
            """
            cursor.execute(delete_query, (pure_code, data_level))
            
            # 删除fund_data_ranges表中的对应记录
            try:
                range_delete_query = """
                DELETE FROM fund_data_ranges 
                WHERE fund_code = %s AND data_level = %s
                """
                cursor.execute(range_delete_query, (pure_code, data_level))
                self.log(f"已删除{fund_code}的{data_level}级别数据范围信息")
            except Exception as e_range:
                self.log(f"删除数据范围信息失败，但不影响主要删除操作: {str(e_range)}")
            
            # 提交事务
            self.conn.commit()
            
            # 更新基金列表
            self.fund_selector.load_funds()
            
            # 更新该基金的可用数据级别
            self.update_data_levels_for_fund(fund_code)
            
            QMessageBox.information(self, "成功", f"{fund_code}的{data_level}级别数据已成功删除，共{data_count}条记录")
            self.log(f"{fund_code}的{data_level}级别数据已成功删除，共{data_count}条记录")
            
        except Exception as e:
            self.log(f"删除数据失败: {str(e)}")
            traceback.print_exc()
            if self.conn:
                self.conn.rollback()
            QMessageBox.critical(self, "错误", f"删除数据失败: {str(e)}")
        finally:
            if cursor:
                cursor.close()

    def get_fund_code_from_input(self):
        """从输入框获取基金代码，并进行格式化
        
        Returns:
            str: 格式化后的基金代码
        """
        code = self.fund_code_input.text().strip().upper()
        
        if not code:
            return None
            
        # 如果没有.SH或.SZ后缀，自动添加
        if '.' not in code:
            # 根据代码前缀判断交易所
            if code.startswith(('6', '5', '7', '0')):  # 上证代码通常以6、5、7开头
                code = f"{code}.SH"
            else:  # 深证代码
                code = f"{code}.SZ"
                
        return code

    def on_get_data_clicked(self):
        """获取行情数据"""
        # 直接从输入框获取基金代码
        fund_code = self.get_fund_code_from_input()
        
        # 如果输入框为空，尝试从选择的行获取
        if not fund_code:
            fund_code, _ = self.fund_selector.get_selected_fund()
            
        # 获取数据级别
        data_level = self.level_selector.currentText()
        
        if not fund_code:
            QMessageBox.warning(self, "选择错误", "请先输入或选择基金")
            return
            
        if not data_level:
            QMessageBox.warning(self, "选择错误", "请先选择数据级别")
            return
            
        # 从fund_code中提取纯代码（去掉.SH或.SZ后缀）
        pure_code = fund_code.split('.')[0] if '.' in fund_code else fund_code
        
        # 检查该基金是否已有该级别的数据
        try:
            cursor = self.conn.cursor()
            query = """
            SELECT COUNT(*) FROM stock_quotes 
            WHERE fund_code = %s AND data_level = %s
            """
            cursor.execute(query, (pure_code, data_level))
            count = cursor.fetchone()[0]
            
            if count > 0:
                # 如果已有数据，询问是否要更新
                reply = QMessageBox.question(
                    self, 
                    "数据已存在", 
                    f"基金 {fund_code} 已有 {count} 条 {data_level} 级别的数据，是否要更新？",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.Yes
                )
                
                if reply == QMessageBox.No:
                    return
        except Exception as e:
            self.log(f"检查数据存在性失败: {str(e)}")
            # 继续执行，不阻止获取数据
        
        # 禁用按钮，避免重复点击
        self.get_data_btn.setEnabled(False)
        self.progress_bar.setValue(0)
        
        # 显示正在获取数据
        self.log(f"开始获取 {fund_code} 的 {data_level} 级别数据...")
        
        # 创建数据获取器
        if not hasattr(self, 'data_fetcher') or self.data_fetcher is None:
            from backtest_gui.fund_data_fetcher import FundDataFetcher
            self.data_fetcher = FundDataFetcher()
            self.data_fetcher.progress_signal.connect(self.on_progress_update)
            self.data_fetcher.error_signal.connect(self.on_data_fetch_error)
            self.data_fetcher.completed_signal.connect(self.on_data_fetch_completed)
        
        # 设置数据库连接
        if self.conn:
            self.data_fetcher.set_db_connection(self.conn)
        
        # 开始获取数据，不需要传入开始和结束日期，自动使用上市日期到昨天
        try:
            # 自动获取从上市日期到昨天的所有数据
            self.data_fetcher.fetch_data(
                symbol=fund_code, 
                data_level=data_level, 
                start_date=None,  # 使用上市日期
                end_date=None,    # 使用昨天
                save_to_db=True
            )
        except Exception as e:
            self.log(f"启动数据获取失败: {str(e)}")
            traceback.print_exc()
            self.get_data_btn.setEnabled(True)
            QMessageBox.critical(self, "错误", f"启动数据获取失败: {str(e)}")

    def on_progress_update(self, current, total, message):
        """更新进度条"""
        if total > 0:
            progress = int(current * 100 / total)
            self.progress_bar.setValue(progress)
        
        if message:
            self.log(message)
    
    def on_data_fetch_finished(self, result_type, result_content):
        """数据获取完成处理
        
        Args:
            result_type: 结果类型，'success'或'error'
            result_content: 结果内容，成功时为获取的数据量，失败时为错误消息
        """
        # 恢复按钮状态
        self.get_data_btn.setEnabled(True)
        self.progress_bar.setValue(100)
        
        if result_type == 'success':
            # 获取成功
            QMessageBox.information(self, "成功", f"成功获取行情数据: {result_content}")
            self.log(f"成功获取行情数据: {result_content}")
            
            # 获取当前基金代码和数据级别
            fund_code = self.get_fund_code_from_input()
            if not fund_code:
                fund_code, _ = self.fund_selector.get_selected_fund()
            data_level = self.level_selector.currentText()
            
            # 更新fund_data_ranges表
            if fund_code and data_level:
                try:
                    # 获取纯代码（去掉.SH或.SZ后缀）
                    pure_code = fund_code.split('.')[0] if '.' in fund_code else fund_code
                    
                    # 查询该基金该级别的数据范围
                    cursor = self.conn.cursor()
                    range_query = """
                    SELECT MIN(date), MAX(date), COUNT(*) 
                    FROM stock_quotes 
                    WHERE fund_code = %s AND data_level = %s
                    """
                    cursor.execute(range_query, (pure_code, data_level))
                    range_result = cursor.fetchone()
                    
                    if range_result and range_result[0] and range_result[1]:
                        start_date = range_result[0]
                        end_date = range_result[1]
                        count = range_result[2]
                        
                        # 更新或插入fund_data_ranges表
                        upsert_query = """
                        INSERT INTO fund_data_ranges (fund_code, data_level, start_date, end_date, record_count, updated_at)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (fund_code, data_level) 
                        DO UPDATE SET 
                            start_date = EXCLUDED.start_date,
                            end_date = EXCLUDED.end_date,
                            record_count = EXCLUDED.record_count,
                            updated_at = NOW()
                        """
                        cursor.execute(upsert_query, (pure_code, data_level, start_date, end_date, count))
                        self.conn.commit()
                        self.log(f"已更新基金 {fund_code} 的 {data_level} 级别数据范围")
                        
                        # 查询基金名称
                        cursor.execute("SELECT fund_name FROM fund_info WHERE fund_code = %s", (pure_code,))
                        fund_info_result = cursor.fetchone()
                        if fund_info_result and fund_info_result[0]:
                            self.log(f"基金名称: {fund_info_result[0]}")
                except Exception as e:
                    self.log(f"更新数据范围失败: {str(e)}")
            
            # 更新基金列表
            self.fund_selector.load_funds()
            
        else:
            # 获取失败
            QMessageBox.warning(self, "失败", f"获取行情数据失败: {result_content}")
            self.log(f"获取行情数据失败: {result_content}")
    
    def on_data_fetch_error(self, error_message):
        """处理数据获取错误事件
        
        Args:
            error_message: 错误消息
        """
        # 记录日志
        self.log(f"数据获取错误: {error_message}")
        
        # 显示错误提示
        QMessageBox.critical(self, "错误", f"获取数据失败: {error_message}")
        
        # 启用按钮
        self.get_data_btn.setEnabled(True)
    
    def on_data_fetch_completed(self, success, message, data):
        """数据获取完成处理 - 处理completed_signal信号
        
        Args:
            success: 是否成功 (Boolean)
            message: 结果消息
            data: 获取的数据
        """
        # 恢复按钮状态
        self.get_data_btn.setEnabled(True)
        self.progress_bar.setValue(100)
        
        if success:
            # 获取成功
            data_count = len(data) if data is not None else 0
            self.log(f"获取行情数据成功: {message}，共 {data_count} 条记录")
            QMessageBox.information(self, "成功", f"获取行情数据成功: {message}")
            
            # 获取当前基金代码和数据级别
            fund_code = self.get_fund_code_from_input()
            if not fund_code:
                fund_code, _ = self.fund_selector.get_selected_fund()
            data_level = self.level_selector.currentText()
            
            # 如果有数据，更新fund_data_ranges表
            if success and data_count > 0 and fund_code and data_level:
                try:
                    # 获取纯代码（去掉.SH或.SZ后缀）
                    pure_code = fund_code.split('.')[0] if '.' in fund_code else fund_code
                    
                    # 查询该基金该级别的数据范围
                    cursor = self.conn.cursor()
                    range_query = """
                    SELECT MIN(date), MAX(date), COUNT(*) 
                    FROM stock_quotes 
                    WHERE fund_code = %s AND data_level = %s
                    """
                    cursor.execute(range_query, (pure_code, data_level))
                    range_result = cursor.fetchone()
                    
                    if range_result and range_result[0] and range_result[1]:
                        start_date = range_result[0]
                        end_date = range_result[1]
                        count = range_result[2]
                        
                        # 更新或插入fund_data_ranges表
                        upsert_query = """
                        INSERT INTO fund_data_ranges (fund_code, data_level, start_date, end_date, record_count, updated_at)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (fund_code, data_level) 
                        DO UPDATE SET 
                            start_date = EXCLUDED.start_date,
                            end_date = EXCLUDED.end_date,
                            record_count = EXCLUDED.record_count,
                            updated_at = NOW()
                        """
                        cursor.execute(upsert_query, (pure_code, data_level, start_date, end_date, count))
                        self.conn.commit()
                        self.log(f"已更新基金 {fund_code} 的 {data_level} 级别数据范围")
                        
                        # 查询基金名称
                        cursor.execute("SELECT fund_name FROM fund_info WHERE fund_code = %s", (pure_code,))
                        fund_info_result = cursor.fetchone()
                        if fund_info_result and fund_info_result[0]:
                            self.log(f"基金名称: {fund_info_result[0]}")
                            
                        # 找到对应的行，更新时间范围显示
                        for row in range(self.fund_selector.fund_table.rowCount()):
                            code_item = self.fund_selector.fund_table.item(row, 0)
                            if code_item and code_item.text() == fund_code:
                                # 更新该行的时间范围
                                self.fund_selector.update_date_range(row, data_level)
                                # 同步更新下拉框选择的数据级别
                                level_combo = self.fund_selector.fund_table.cellWidget(row, 2)
                                if level_combo:
                                    index = level_combo.findText(data_level)
                                    if index >= 0:
                                        level_combo.setCurrentIndex(index)
                                break
                            
                except Exception as e:
                    self.log(f"更新数据范围失败: {str(e)}")
            
            # 更新基金列表
            self.fund_selector.load_funds()
            
        else:
            # 获取失败
            self.log(f"获取行情数据失败: {message}")
            QMessageBox.warning(self, "失败", f"获取行情数据失败: {message}")
    
    def create_band_strategy(self):
        """创建新的波段策略"""
        # 直接从输入框获取基金代码
        fund_code = self.get_fund_code_from_input()
        
        # 如果输入框为空，尝试从选择的行获取
        if not fund_code:
            fund_code, _ = self.fund_selector.get_selected_fund()
            
        if not fund_code:
            QMessageBox.warning(self, "创建错误", "请先输入或选择基金")
            return
            
        # 创建编辑器对话框
        editor = BandStrategyEditor(self, fund_code=fund_code)
        
        # 显示对话框
        if editor.exec_() == QDialog.Accepted:
            # 保存策略
            self.save_band_strategy(
                strategy_id=None,
                strategy_name=editor.strategy_name,
                description=editor.description,
                fund_code=editor.fund_code,
                grid_levels=editor.grid_levels
            )
            
            # 重新加载策略列表
            self.load_band_strategies(fund_code)
    
    def edit_band_strategy(self):
        """编辑选中的波段策略"""
        # 获取当前选择的策略
        index = self.strategy_table.currentIndex().row()
        if index < 0 or index >= len(self.strategies):
            QMessageBox.warning(self, "编辑错误", "请先选择策略")
            return
            
        strategy = self.strategies[index]
        
        # 获取基金代码，优先使用输入框的值
        fund_code = self.get_fund_code_from_input()
        if not fund_code:
            fund_code, _ = self.fund_selector.get_selected_fund()
        
        # 创建编辑器对话框
        editor = BandStrategyEditor(
            self,
            strategy_data={
                'strategy_id': strategy['strategy_id'],
                'strategy_name': strategy['strategy_name'],
                'description': strategy['strategy_desc'], # Changed from strategy['description']
                'grid_levels': copy.deepcopy(strategy['grid_levels']) # Create a deep copy
            },
            fund_code=fund_code
        )
        
        # 显示对话框
        if editor.exec_() == QDialog.Accepted:
            # 保存策略
            self.save_band_strategy(
                strategy_id=strategy['strategy_id'],
                strategy_name=editor.strategy_name,
                description=editor.description,
                fund_code=editor.fund_code,
                grid_levels=editor.grid_levels
            )
            
            # 重新加载策略列表
            self.load_band_strategies()
    
    def delete_band_strategy(self):
        """删除选中的波段策略"""
        # 获取当前选择的策略
        index = self.strategy_table.currentIndex().row()
        if index < 0 or index >= len(self.strategies):
            QMessageBox.warning(self, "删除错误", "请先选择策略")
            return
            
        strategy = self.strategies[index]
        
        # 确认删除
        reply = QMessageBox.question(
            self, 
            "确认删除", 
            f"确定要删除策略 '{strategy['strategy_name']}' 吗？",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply != QMessageBox.Yes:
            return
            
        # 执行删除
        if not self.conn:
            return
            
        try:
            # 开始事务
            cursor = self.conn.cursor()
            
            # 删除网格级别
            cursor.execute("DELETE FROM grid_levels WHERE strategy_id = %s", (strategy['strategy_id'],))
            
            # 删除基金策略绑定
            cursor.execute("DELETE FROM fund_strategy_bindings WHERE strategy_id = %s", (strategy['strategy_id'],))
            
            # 兼容旧表
            cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'fund_strategy_mapping')")
            if cursor.fetchone()[0]:
                cursor.execute("DELETE FROM fund_strategy_mapping WHERE strategy_id = %s", (strategy['strategy_id'],))
            
            # 删除策略
            cursor.execute("DELETE FROM band_strategies WHERE id = %s", (strategy['strategy_id'],))
            
            # 提交事务
            self.conn.commit()
            
            # 重新加载策略列表
            self.load_band_strategies()
            
            # 提示成功
            QMessageBox.information(self, "删除成功", f"策略 '{strategy['strategy_name']}' 已删除")
            
        except Exception as e:
            QMessageBox.warning(self, "删除错误", f"删除策略失败: {str(e)}")
            
            # 回滚事务
            self.conn.rollback()
    
    def save_band_strategy(self, strategy_id, strategy_name, description, fund_code, grid_levels):
        """保存波段策略
        
        Args:
            strategy_id: 策略ID，如果为None则为新建
            strategy_name: 策略名称
            description: 策略描述
            fund_code: 基金代码
            grid_levels: 网格级别列表
        """
        if not self.conn:
            return
            
        try:
            # 开始事务
            cursor = self.conn.cursor()
            
            # 检查表是否存在，不存在则创建
            self.ensure_tables_exist(cursor)
            
            # 处理基金代码，移除.SH或.SZ后缀
            pure_fund_code = fund_code.split('.')[0] if '.' in fund_code else fund_code
            
            # 保存策略
            if strategy_id is None:
                # 新建策略
                cursor.execute(
                    "INSERT INTO band_strategies (name, description, created_at, updated_at) VALUES (%s, %s, NOW(), NOW()) RETURNING id",
                    (strategy_name, description)
                )
                strategy_id = cursor.fetchone()[0]
            else:
                # 更新策略
                cursor.execute(
                    "UPDATE band_strategies SET name = %s, description = %s, updated_at = NOW() WHERE id = %s",
                    (strategy_name, description, strategy_id)
                )
            
            # 删除旧的网格级别
            cursor.execute("DELETE FROM grid_levels WHERE strategy_id = %s", (strategy_id,))
            
            # 保存网格级别
            for level in grid_levels:
                cursor.execute(
                    """
                    INSERT INTO grid_levels 
                    (strategy_id, level, grid_type, buy_price, sell_price, buy_shares, sell_shares)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        strategy_id, 
                        level.level, 
                        level.grid_type, 
                        level.buy_price, 
                        level.sell_price, 
                        level.buy_shares, 
                        level.sell_shares
                    )
                )
            
            # 检查基金策略绑定是否存在（使用纯代码）
            cursor.execute(
                "SELECT COUNT(*) FROM fund_strategy_bindings WHERE fund_code = %s AND strategy_id = %s",
                (pure_fund_code, strategy_id)
            )
            
            if cursor.fetchone()[0] == 0:
                # 创建基金策略绑定
                cursor.execute(
                    "INSERT INTO fund_strategy_bindings (fund_code, strategy_id, is_default, created_at) VALUES (%s, %s, TRUE, NOW())",
                    (pure_fund_code, strategy_id)
                )
                
                # 兼容旧表
                cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'fund_strategy_mapping')")
                if cursor.fetchone()[0]:
                    try:
                        cursor.execute(
                            "INSERT INTO fund_strategy_mapping (fund_code, strategy_id, created_at) VALUES (%s, %s, NOW())",
                            (pure_fund_code, strategy_id)
                        )
                    except Exception as e:
                        self.log(f"插入到旧表失败，这是预期的: {str(e)}")
            
            # 提交事务
            self.conn.commit()
            
            # 提示成功
            QMessageBox.information(self, "保存成功", f"策略 '{strategy_name}' 已保存")
            
            # 记录日志
            self.log(f"成功保存策略 '{strategy_name}' (ID: {strategy_id})")
            
        except Exception as e:
            QMessageBox.warning(self, "保存错误", f"保存策略失败: {str(e)}")
            
            # 回滚事务
            self.conn.rollback()
    
    def ensure_tables_exist(self, cursor):
        """确保数据库表存在
        
        Args:
            cursor: 数据库游标
        """
        try:
            # 检查并创建基金表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS funds (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL UNIQUE,
                name VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # 检查并创建基金信息表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS fund_info (
                id SERIAL PRIMARY KEY,
                fund_code VARCHAR(20) NOT NULL UNIQUE,
                fund_name VARCHAR(100) NOT NULL,
                listing_date DATE,
                fund_type VARCHAR(50),
                manager VARCHAR(100),
                company VARCHAR(100),
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # 检查并创建基金数据时间范围表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS fund_data_ranges (
                id SERIAL PRIMARY KEY,
                fund_code VARCHAR(20) NOT NULL,
                data_level VARCHAR(10) NOT NULL,
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                record_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (fund_code, data_level)
            )
            """)
            
            # 检查并创建策略表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS band_strategies (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                description TEXT,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
            """)
            
            # 检查并创建网格级别表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS grid_levels (
                id SERIAL PRIMARY KEY,
                strategy_id INTEGER NOT NULL,
                level INTEGER NOT NULL,
                grid_type VARCHAR(20) NOT NULL,
                buy_price DECIMAL(10, 4) NOT NULL,
                sell_price DECIMAL(10, 4) NOT NULL,
                buy_shares INTEGER NOT NULL,
                sell_shares INTEGER NOT NULL,
                CONSTRAINT fk_strategy
                    FOREIGN KEY(strategy_id)
                    REFERENCES band_strategies(id)
                    ON DELETE CASCADE
            )
            """)
            
            # 检查并创建基金策略绑定表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS fund_strategy_bindings (
                id SERIAL PRIMARY KEY,
                fund_code VARCHAR(20) NOT NULL,
                strategy_id INTEGER NOT NULL,
                is_default BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP NOT NULL,
                CONSTRAINT fk_strategy_binding
                    FOREIGN KEY(strategy_id)
                    REFERENCES band_strategies(id)
                    ON DELETE CASCADE
            )
            """)
            
            # 向前兼容性：检查旧表并迁移数据
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'fund_strategy_mapping'
            )
            """)
            
            old_table_exists = cursor.fetchone()[0]
            if old_table_exists:
                # 检查新表中是否已有数据
                cursor.execute("SELECT COUNT(*) FROM fund_strategy_bindings")
                new_table_count = cursor.fetchone()[0]
                
                if new_table_count == 0:
                    # 如果新表为空，迁移旧表数据
                    try:
                        cursor.execute("""
                        INSERT INTO fund_strategy_bindings (fund_code, strategy_id, is_default, created_at)
                        SELECT fund_code, strategy_id, FALSE, created_at FROM fund_strategy_mapping
                        """)
                        self.log("成功从旧表迁移数据到新表")
                    except Exception as e:
                        self.log(f"迁移数据失败: {str(e)}")
            
            self.log("数据库表结构已检查并创建")
            
        except Exception as e:
            self.log(f"创建表结构错误: {str(e)}")
            traceback.print_exc()
    
    def log(self, message):
        """添加日志
        
        Args:
            message: 日志消息
        """
        try:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 检查是否已初始化market_status_text
            if hasattr(self, 'market_status_text') and self.market_status_text is not None:
                self.market_status_text.append(f"[{current_time}] {message}")
            
            # 始终在控制台打印
            print(f"[{current_time}] {message}")
            
        except Exception as e:
            # 确保日志方法自身不会引发异常
            print(f"日志记录错误: {str(e)}")
            traceback.print_exc()
    
    def closeEvent(self, event):
        """窗口关闭事件"""
        # 关闭数据获取器
        if hasattr(self, 'data_fetcher'):
            self.data_fetcher.close()
            
        # 关闭数据库连接
        if self.conn:
            try:
                self.conn.close()
                self.log("数据库连接已关闭")
            except Exception as e:
                self.log(f"关闭数据库连接异常: {str(e)}")
                
        # 接受关闭事件
        event.accept() 