#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试基金选择器
"""

from PyQt5.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QWidget
import sys
import traceback

from backtest_gui.gui.components.fund_selector import FundSelectorWidget
from backtest_gui.db.database import Database

class TestWindow(QMainWindow):
    """测试窗口"""
    
    def __init__(self):
        super().__init__()
        
        # 设置窗口
        self.setWindowTitle("基金选择器测试")
        self.resize(600, 600)
        
        # 主布局
        main_layout = QVBoxLayout()
        main_widget = QWidget()
        main_widget.setLayout(main_layout)
        self.setCentralWidget(main_widget)
        
        # 连接数据库
        self.db = Database()
        if not self.db.connect():
            print("数据库连接失败")
            return
        
        # 创建基金选择器
        self.fund_selector = FundSelectorWidget(parent=self, db_connection=self.db.get_connection())
        main_layout.addWidget(self.fund_selector)
        
        # 连接信号
        self.fund_selector.fund_selected.connect(self.on_fund_selected)
        self.fund_selector.fund_level_selected.connect(self.on_fund_level_selected)
        
        # 打印加载的基金数量
        print(f"加载了 {len(self.fund_selector.funds)} 个基金")
        for fund in self.fund_selector.funds:
            print(f"基金: {fund['code']}, 数据级别: {fund['data_levels']}")
    
    def on_fund_selected(self, fund_code, fund_name):
        """基金选择事件处理"""
        print(f"选择基金: {fund_code} - {fund_name}")
    
    def on_fund_level_selected(self, fund_code, fund_name, level):
        """基金和数据级别选择事件处理"""
        print(f"选择基金和级别: {fund_code} - {fund_name}, 级别: {level}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = TestWindow()
    window.show()
    sys.exit(app.exec_()) 