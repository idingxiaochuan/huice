#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
基金选择器组件
用于选择基金和查询基金信息
"""

import os
import sys
import traceback
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, 
    QPushButton, QComboBox, QTableWidget, QTableWidgetItem,
    QHeaderView, QMessageBox, QListWidget, QListWidgetItem,
    QSplitter
)
from PyQt5.QtCore import Qt, pyqtSignal, pyqtSlot

class FundSelectorWidget(QWidget):
    """基金选择器组件"""
    
    # 定义信号
    fund_selected = pyqtSignal(str, str)  # 基金代码, 基金名称
    fund_level_selected = pyqtSignal(str, str, str)  # 基金代码, 基金名称, 数据级别
    
    def __init__(self, parent=None, db_connection=None):
        """初始化基金选择器
        
        Args:
            parent: 父窗口
            db_connection: 数据库连接
        """
        super().__init__(parent)
        self.db_connection = db_connection
        self.funds = []  # 基金列表
        self.current_fund_code = None  # 当前选中的基金代码
        self.current_fund_name = None  # 当前选中的基金名称
        
        # 初始化UI
        self.init_ui()
        
        # 加载基金列表
        self.load_funds()
    
    def init_ui(self):
        """初始化用户界面"""
        main_layout = QVBoxLayout(self)
        
        # 隐藏搜索区域，因为已经移到了主窗口顶部
        self.search_input = QLineEdit()
        self.search_input.setVisible(False)  # 隐藏，但保留以兼容现有代码
        self.level_selector = QComboBox()
        self.level_selector.setVisible(False)  # 隐藏，但保留以兼容现有代码
        
        # 基金数据表格
        self.fund_table = QTableWidget(0, 4)  # 0行，4列（基金代码、基金名称、数据级别、时间范围）
        self.fund_table.setHorizontalHeaderLabels(["基金代码", "基金名称", "数据级别", "时间范围"])
        
        # 调整列宽比例：基金代码20%，基金名称25%，数据级别15%，时间范围40%
        self.fund_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Interactive)
        self.fund_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Interactive)
        self.fund_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Interactive)
        self.fund_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)
        
        # 设置初始列宽
        self.fund_table.setColumnWidth(0, 100)  # 基金代码列宽
        self.fund_table.setColumnWidth(1, 150)  # 基金名称列宽
        self.fund_table.setColumnWidth(2, 80)   # 数据级别列宽
        # 时间范围列自动拉伸
        
        self.fund_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.fund_table.setSelectionMode(QTableWidget.SingleSelection)
        self.fund_table.setEditTriggers(QTableWidget.NoEditTriggers)
        # 连接表格单元格点击信号
        self.fund_table.cellClicked.connect(self.on_table_cell_clicked)
        main_layout.addWidget(self.fund_table)
        
        # 设置主布局
        self.setLayout(main_layout)
    
    def load_funds(self):
        """加载基金列表"""
        self.funds = []
        
        if not self.db_connection:
            return
        
        try:
            # 清空表格
            self.fund_table.setRowCount(0)
            
            # 从stock_quotes表中查询基金数据
            query = """
            SELECT DISTINCT fund_code, data_level 
            FROM stock_quotes 
            ORDER BY fund_code, data_level
            """
            cursor = self.db_connection.cursor()
            cursor.execute(query)
            
            # 处理结果
            fund_data_levels = {}  # 用于存储每个基金的数据级别
            fund_data_ranges = {}  # 用于存储每个基金每个级别的数据范围
            fund_names = {}        # 用于存储基金名称
            
            for row in cursor.fetchall():
                fund_code = row[0]
                data_level = row[1]
                
                # 记录基金的数据级别
                if fund_code not in fund_data_levels:
                    fund_data_levels[fund_code] = []
                if data_level not in fund_data_levels[fund_code]:
                    fund_data_levels[fund_code].append(data_level)
                
                # 查询该基金该级别的数据范围
                range_query = """
                SELECT MIN(date), MAX(date), COUNT(*) 
                FROM stock_quotes 
                WHERE fund_code = %s AND data_level = %s
                """
                cursor.execute(range_query, (fund_code, data_level))
                range_result = cursor.fetchone()
                
                if range_result and range_result[0] and range_result[1]:
                    start_date = range_result[0]
                    end_date = range_result[1]
                    count = range_result[2]
                    
                    # 更新或插入fund_data_ranges表
                    try:
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
                        cursor.execute(upsert_query, (fund_code, data_level, start_date, end_date, count))
                        self.db_connection.commit()
                    except Exception as e:
                        print(f"更新数据范围失败: {str(e)}")
                    
                    # 保存到本地字典
                    if fund_code not in fund_data_ranges:
                        fund_data_ranges[fund_code] = {}
                    
                    fund_data_ranges[fund_code][data_level] = {
                        'start_date': start_date,
                        'end_date': end_date,
                        'count': count
                    }
            
            # 查询所有基金的名称
            name_query = """
            SELECT fund_code, fund_name FROM fund_info
            """
            cursor.execute(name_query)
            for row in cursor.fetchall():
                fund_code = row[0]
                fund_name = row[1]
                fund_names[fund_code] = fund_name
            
            # 遍历所有基金代码，添加到表格
            for fund_code in fund_data_levels.keys():
                # 添加市场后缀
                if fund_code.startswith(('5', '6', '7', '0')):
                    display_code = f"{fund_code}.SH"
                else:
                    display_code = f"{fund_code}.SZ"
                
                # 获取基金名称
                fund_name = fund_names.get(fund_code, display_code)
                
                # 如果没有名称，尝试从funds表获取
                if fund_name == display_code:
                    try:
                        name_query = "SELECT name FROM funds WHERE symbol = %s"
                        cursor.execute(name_query, (display_code,))
                        name_result = cursor.fetchone()
                        if name_result and name_result[0]:
                            fund_name = name_result[0]
                            
                            # 更新fund_info表
                            try:
                                insert_query = """
                                INSERT INTO fund_info (fund_code, fund_name) 
                                VALUES (%s, %s)
                                ON CONFLICT (fund_code) DO UPDATE SET fund_name = EXCLUDED.fund_name
                                """
                                cursor.execute(insert_query, (fund_code, fund_name))
                                self.db_connection.commit()
                            except Exception as e:
                                print(f"更新基金信息失败: {str(e)}")
                    except Exception as e:
                        print(f"查询基金名称失败: {str(e)}")
                
                # 添加到基金列表
                    self.funds.append({
                        'code': display_code,
                        'name': fund_name,
                    'data_levels': fund_data_levels[fund_code],
                    'data_ranges': fund_data_ranges.get(fund_code, {})
                    })
            
                # 添加到表格
                row = self.fund_table.rowCount()
                self.fund_table.insertRow(row)
            
                # 基金代码列
                code_item = QTableWidgetItem(display_code)
                self.fund_table.setItem(row, 0, code_item)
                
                # 基金名称列
                name_item = QTableWidgetItem(fund_name)
                self.fund_table.setItem(row, 1, name_item)
                
                # 数据级别列 - 添加下拉框
                level_combo = QComboBox()
                level_combo.setProperty("fund_code", display_code)
                level_combo.setProperty("fund_name", fund_name)
                
                # 添加数据级别选项
                for level in fund_data_levels[fund_code]:
                    level_combo.addItem(level)
                
                level_combo.currentTextChanged.connect(self.on_table_level_selected)
                self.fund_table.setCellWidget(row, 2, level_combo)
                
                # 时间范围列 - 如果有数据级别，显示第一个级别的时间范围
                if fund_data_levels[fund_code] and fund_code in fund_data_ranges:
                    first_level = fund_data_levels[fund_code][0]
                    if first_level in fund_data_ranges[fund_code]:
                        range_data = fund_data_ranges[fund_code][first_level]
                        start_date = range_data['start_date'].strftime('%Y-%m-%d') if range_data['start_date'] else 'N/A'
                        end_date = range_data['end_date'].strftime('%Y-%m-%d') if range_data['end_date'] else 'N/A'
                        count = range_data['count']
                        range_text = f"{start_date} 至 {end_date} ({count}条)"
                        self.fund_table.setItem(row, 3, QTableWidgetItem(range_text))
                    else:
                        self.fund_table.setItem(row, 3, QTableWidgetItem("无数据"))
                else:
                    self.fund_table.setItem(row, 3, QTableWidgetItem("无数据"))
            
            # 如果有基金，选中第一个
            if self.fund_table.rowCount() > 0:
                self.fund_table.selectRow(0)
                first_fund = self.funds[0]
                self.select_fund(first_fund['code'], first_fund['name'])
            
        except Exception as e:
            print(f"加载基金列表失败: {str(e)}")
            traceback.print_exc()
    
    def update_date_range(self, row, level):
        """更新指定行的时间范围
        
        Args:
            row: 行索引
            level: 数据级别
        """
        if row < 0 or row >= self.fund_table.rowCount():
            return
            
        # 获取基金代码
        code_item = self.fund_table.item(row, 0)
        if not code_item:
            return
            
        fund_code = code_item.text()
        pure_code = fund_code.split('.')[0] if '.' in fund_code else fund_code
        
        # 查找对应的基金
        fund = None
        for f in self.funds:
            if f['code'] == fund_code:
                fund = f
                break
                
        if not fund:
            return
            
        # 获取该级别的时间范围
        date_range = ""
        if level in fund.get('data_ranges', {}):
            range_data = fund['data_ranges'][level]
            start_date = range_data['start_date'].strftime("%Y-%m-%d") if range_data.get('start_date') else "未知"
            end_date = range_data['end_date'].strftime("%Y-%m-%d") if range_data.get('end_date') else "未知"
            count = range_data.get('count', 0)
            date_range = f"{start_date} 至 {end_date} ({count}条)"
        else:
            # 如果内存中没有，尝试从数据库查询
            try:
                if self.db_connection:
                    cursor = self.db_connection.cursor()
                    query = """
                    SELECT start_date, end_date, record_count 
                    FROM fund_data_ranges 
                    WHERE fund_code = %s AND data_level = %s
                    """
                    cursor.execute(query, (pure_code, level))
                    result = cursor.fetchone()
                    
                    if result and result[0] and result[1]:
                        start_date = result[0].strftime("%Y-%m-%d")
                        end_date = result[1].strftime("%Y-%m-%d")
                        count = result[2]
                        date_range = f"{start_date} 至 {end_date} ({count}条)"
                    else:
                        # 如果fund_data_ranges表中没有，直接查询stock_quotes表
                        query = """
                        SELECT MIN(date), MAX(date), COUNT(*) 
                        FROM stock_quotes 
                        WHERE fund_code = %s AND data_level = %s
                        """
                        cursor.execute(query, (pure_code, level))
                        result = cursor.fetchone()
                        
                        if result and result[0] and result[1]:
                            start_date = result[0].strftime("%Y-%m-%d")
                            end_date = result[1].strftime("%Y-%m-%d")
                            count = result[2]
                            date_range = f"{start_date} 至 {end_date} ({count}条)"
                            
                            # 更新fund_data_ranges表
                            try:
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
                                cursor.execute(upsert_query, (pure_code, level, result[0], result[1], count))
                                self.db_connection.commit()
                            except Exception as e:
                                print(f"更新数据范围失败: {str(e)}")
            except Exception as e:
                print(f"查询时间范围失败: {str(e)}")
                
        # 更新表格
        range_item = QTableWidgetItem(date_range)
        self.fund_table.setItem(row, 3, range_item)
    
    def on_code_changed(self, text):
        """当用户输入代码时自动格式化"""
        # 移除非数字和字母的字符
        clean_text = ''.join(c for c in text if c.isdigit() or c.isalpha() or c == '.')
        
        # 如果格式不同，则更新文本（避免死循环）
        if clean_text != text:
            self.search_input.setText(clean_text)
    
    def on_code_entered(self):
        """处理基金代码输入完成事件"""
        # 获取输入的基金代码
        code = self.search_input.text().strip().upper()
        if not code:
            return
            
        # 检查基金代码格式
        if '.' not in code:
            # 自动添加市场后缀
            if code.startswith(('5', '6', '7', '0')):
                code = f"{code}.SH"
            else:
                code = f"{code}.SZ"
        
        # 检查是否已存在
        for fund in self.funds:
            if fund['code'] == code:
                # 已存在，选中该基金
                for row in range(self.fund_table.rowCount()):
                    if self.fund_table.item(row, 0).text() == code:
                        self.fund_table.selectRow(row)
                        self.select_fund(code, fund['name'])
                return
                
        # 如果是新基金，自动添加
        if self.db_connection:
            try:
                # 尝试获取基金名称
                fund_name = code
                pure_code = code.split('.')[0] if '.' in code else code
                
                # 先尝试从fund_info表查询
                cursor = self.db_connection.cursor()
                cursor.execute("SELECT fund_name FROM fund_info WHERE fund_code = %s", (pure_code,))
                name_result = cursor.fetchone()
                
                if name_result and name_result[0]:
                    fund_name = name_result[0]
                else:
                    # 如果fund_info表中没有，尝试从网络获取
                    try:
                        from backtest_gui.fund_data_fetcher import FundDataWorker
                        worker = FundDataWorker(code, db_connection=self.db_connection)
                        web_fund_name = worker._get_fund_name_from_web(code)
                        if web_fund_name:
                            fund_name = web_fund_name
                            print(f"从网络获取到基金名称: {fund_name}")
                            
                            # 保存到fund_info表
                            insert_query = """
                            INSERT INTO fund_info (fund_code, fund_name) 
                            VALUES (%s, %s)
                            ON CONFLICT (fund_code) DO UPDATE SET fund_name = EXCLUDED.fund_name
                            """
                            cursor.execute(insert_query, (pure_code, fund_name))
                            self.db_connection.commit()
                    except Exception as e:
                        print(f"获取基金名称失败: {str(e)}")
                
                # 添加到数据库
                query = "INSERT INTO funds (symbol, name) VALUES (%s, %s) ON CONFLICT (symbol) DO NOTHING"
                cursor.execute(query, (code, fund_name))
                self.db_connection.commit()
                
                # 添加到列表
                self.funds.append({
                    'code': code,
                    'name': fund_name,
                    'data_levels': [],
                    'data_ranges': {} # Initialize data_ranges for new fund
                })
                
                # 添加到表格
                row = self.fund_table.rowCount()
                self.fund_table.insertRow(row)
                
                # 基金代码列
                code_item = QTableWidgetItem(code)
                self.fund_table.setItem(row, 0, code_item)
                
                # 基金名称列
                name_item = QTableWidgetItem(fund_name)
                self.fund_table.setItem(row, 1, name_item)
                
                # 数据级别列 - 添加下拉框
                level_combo = QComboBox()
                level_combo.setProperty("fund_code", code)
                level_combo.setProperty("fund_name", fund_name)
                level_combo.currentTextChanged.connect(self.on_table_level_selected)
                self.fund_table.setCellWidget(row, 2, level_combo)
                
                # 时间范围列 - 初始为空
                self.fund_table.setItem(row, 3, QTableWidgetItem(""))
                
                # 选中新基金
                self.fund_table.selectRow(row)
                self.select_fund(code, fund_name)
                
                # 获取当前选择的数据级别
                selected_level = self.level_selector.currentText()
                
                # 发送信号
                self.fund_selected.emit(code, fund_name)
                
            except Exception as e:
                QMessageBox.warning(self, "添加基金失败", f"无法添加基金: {str(e)}")
                traceback.print_exc()
    
    def on_table_cell_clicked(self, row, column):
        """当表格单元格被点击时触发"""
        # 获取选中行的基金代码
        code_item = self.fund_table.item(row, 0)
        name_item = self.fund_table.item(row, 1)
        if not code_item:
            return
        
        # 获取基金代码和名称
        fund_code = code_item.text()
        fund_name = name_item.text() if name_item else fund_code
        
        # 获取对应的下拉框
        combo = self.fund_table.cellWidget(row, 2)
        if not combo:
            return
        
        # 选中基金
        self.select_fund(fund_code, fund_name)
        
        # 如果下拉框有数据级别，自动选择当前级别并同步到顶部的级别选择器
        if combo.count() > 0:
            level = combo.currentText()
            
            # 同步到顶部的级别选择器
            index = self.level_selector.findText(level)
            if index >= 0:
                self.level_selector.setCurrentIndex(index)
            
            # 更新时间范围
            self.update_date_range(row, level)
            
            # 发出级别选中信号
            self.fund_level_selected.emit(fund_code, fund_name, level)
    
    def on_table_level_selected(self, level):
        """表格中数据级别选择改变时触发
        
        Args:
            level: 选择的数据级别
        """
        if not level:
            return
            
        # 获取发送信号的下拉框
        combo = self.sender()
        if not combo:
            return
            
        # 获取下拉框所在的行
        for row in range(self.fund_table.rowCount()):
            if self.fund_table.cellWidget(row, 2) == combo:
                # 获取基金代码和名称
                fund_code = combo.property("fund_code")
                fund_name = combo.property("fund_name")
                
                # 更新时间范围
                self.update_date_range(row, level)
                
                # 如果这是当前选中的行，发出级别选中信号
                if self.fund_table.currentRow() == row:
                    self.current_fund_code = fund_code
                    self.current_fund_name = fund_name
                    self.fund_level_selected.emit(fund_code, fund_name, level)
                    
                    # 同步更新主窗口的级别选择器
                    if hasattr(self, 'level_selector') and self.level_selector:
                        # 检查级别是否在选择器中
                        found = False
                        for i in range(self.level_selector.count()):
                            if self.level_selector.itemText(i) == level:
                                self.level_selector.setCurrentText(level)
                                found = True
                                break
                        
                        # 如果不在，添加它
                        if not found:
                            self.level_selector.addItem(level)
                            self.level_selector.setCurrentText(level)
                
                break
    
    def on_level_selected(self, level):
        """当选择数据级别时触发（兼容旧接口）"""
        if self.current_fund_code and level:
            # 发出选中信号
            self.fund_level_selected.emit(self.current_fund_code, self.current_fund_name, level)
    
    def select_fund(self, code, name):
        """选择基金并触发信号"""
        self.search_input.setText(code)
        self.current_fund_code = code
        self.current_fund_name = name
        
        # 发出选中信号
        self.fund_selected.emit(code, name)
    
    def get_selected_fund(self):
        """获取当前选中的基金
        
        Returns:
            tuple: (基金代码, 基金名称)
        """
        return self.current_fund_code, self.current_fund_name
    
    def get_selected_level(self):
        """获取当前选中的数据级别
        
        Returns:
            str: 数据级别
        """
        # 查找选中行
        selected_rows = self.fund_table.selectionModel().selectedRows()
        if selected_rows:
            row = selected_rows[0].row()
            combo = self.fund_table.cellWidget(row, 2) # Changed from 1 to 2 for level_combo
            if combo and combo.count() > 0:
                return combo.currentText()
        
        # 如果表格中没有选中行或选中行没有数据级别，则返回下拉框中的选择
        return self.level_selector.currentText()
    
    def set_selected_fund(self, fund_code):
        """设置选中的基金
        
        Args:
            fund_code: 基金代码
        """
        if not fund_code:
            self.search_input.clear()
            self.current_fund_code = None
            self.current_fund_name = None
            return
            
        self.search_input.setText(fund_code)
        # 触发code_entered以确保正确设置
        self.on_code_entered() 