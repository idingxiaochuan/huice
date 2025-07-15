#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
波段交易回测系统主程序
"""
import sys
import os

# 添加父级目录到系统路径，以便能够找到backtest_gui包
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QStatusBar, QStackedWidget, QFrame
)
from PyQt5.QtCore import QCoreApplication, QTranslator, Qt
from PyQt5.QtGui import QFont, QIcon

from backtest_gui.gui.main_window import MainWindow
from backtest_gui.gui.prediction_window import PredictionWindow


class Config:
    """配置类"""
    
    def __init__(self):
        self.database_config = {
            'host': '127.0.0.1',
            'port': 5432,
            'dbname': 'huice',
            'user': 'postgres',
            'password': 'postgres'
        }
        
        self.backtest = {
            'initial_capital': 100000.0,
            'default_stock': '515170.SH',
            'batch_size': 500
        }
        
    def get(self, key, default=None):
        """获取配置项
        
        Args:
            key: 配置项键名，使用点号分隔层级
            default: 默认值
        """
        parts = key.split('.')
        value = self.__dict__
        try:
            for part in parts:
                value = value[part]
            return value
        except (KeyError, TypeError):
            return default


class MainApplicationWindow(QMainWindow):
    """主应用窗口"""
    
    def __init__(self):
        super().__init__()
        
        # 保存配置
        self.config = Config()
        
        # 初始化UI
        self.init_ui()
        
        # 实例化子窗口（懒加载）
        self.main_window = None
        self.prediction_window = None
        
        # 默认显示行情回测模块
        self.show_backtest_module()
    
    def init_ui(self):
        """初始化用户界面"""
        # 设置窗口基本属性
        self.setWindowTitle("波段交易系统")
        self.setMinimumSize(1200, 800)
        
        # 创建中央部件
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # 创建主布局
        main_layout = QHBoxLayout(central_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        
        # 创建左侧导航栏
        nav_frame = QFrame()
        nav_frame.setFrameShape(QFrame.StyledPanel)
        nav_frame.setMaximumWidth(180)
        nav_frame.setMinimumWidth(180)
        nav_layout = QVBoxLayout(nav_frame)
        nav_layout.setContentsMargins(0, 0, 0, 0)
        
        # 添加标题
        title_label = QLabel("交易系统")
        title_font = QFont()
        title_font.setPointSize(16)
        title_font.setBold(True)
        title_label.setFont(title_font)
        title_label.setAlignment(Qt.AlignCenter)
        title_label.setStyleSheet("padding: 20px 0; background-color: #f0f0f0;")
        nav_layout.addWidget(title_label)
        
        # 创建模块按钮
        btn_font = QFont()
        btn_font.setPointSize(12)
        
        # 真实交易模块
        self.real_trade_btn = QPushButton("我的交易")
        self.real_trade_btn.setFont(btn_font)
        self.real_trade_btn.setMinimumHeight(60)
        self.real_trade_btn.setStyleSheet(
            "QPushButton { text-align: left; padding-left: 20px; border: none; }"
            "QPushButton:hover { background-color: #e0e0e0; }"
            "QPushButton:checked { background-color: #d0d0d0; font-weight: bold; }"
        )
        self.real_trade_btn.setCheckable(True)
        self.real_trade_btn.clicked.connect(self.show_real_trade_module)
        nav_layout.addWidget(self.real_trade_btn)
        
        # 行情回测模块
        self.backtest_btn = QPushButton("行情回测")
        self.backtest_btn.setFont(btn_font)
        self.backtest_btn.setMinimumHeight(60)
        self.backtest_btn.setStyleSheet(
            "QPushButton { text-align: left; padding-left: 20px; border: none; }"
            "QPushButton:hover { background-color: #e0e0e0; }"
            "QPushButton:checked { background-color: #d0d0d0; font-weight: bold; }"
        )
        self.backtest_btn.setCheckable(True)
        self.backtest_btn.clicked.connect(self.show_backtest_module)
        nav_layout.addWidget(self.backtest_btn)
        
        # 数据分析模块
        self.data_module_btn = QPushButton("数据分析")
        self.data_module_btn.setFont(btn_font)
        self.data_module_btn.setMinimumHeight(60)
        self.data_module_btn.setStyleSheet(
            "QPushButton { text-align: left; padding-left: 20px; border: none; }"
            "QPushButton:hover { background-color: #e0e0e0; }"
            "QPushButton:checked { background-color: #d0d0d0; font-weight: bold; }"
        )
        self.data_module_btn.setCheckable(True)
        self.data_module_btn.clicked.connect(self.show_data_module)
        nav_layout.addWidget(self.data_module_btn)
        
        # 数据整理模块
        self.data_manage_btn = QPushButton("数据整理")
        self.data_manage_btn.setFont(btn_font)
        self.data_manage_btn.setMinimumHeight(60)
        self.data_manage_btn.setStyleSheet(
            "QPushButton { text-align: left; padding-left: 20px; border: none; }"
            "QPushButton:hover { background-color: #e0e0e0; }"
            "QPushButton:checked { background-color: #d0d0d0; font-weight: bold; }"
        )
        self.data_manage_btn.setCheckable(True)
        self.data_manage_btn.clicked.connect(self.show_data_manage_module)
        nav_layout.addWidget(self.data_manage_btn)
        
        # 添加底部空白区域
        nav_layout.addStretch()
        
        # 创建右侧内容区域
        self.content_stack = QStackedWidget()
        
        # 创建各模块的占位符页面
        self.real_trade_page = QWidget()
        self.backtest_page = QWidget()
        self.data_analysis_page = QWidget()
        self.data_manage_page = QWidget()
        
        # 添加页面到堆栈
        self.content_stack.addWidget(self.real_trade_page)
        self.content_stack.addWidget(self.backtest_page)
        self.content_stack.addWidget(self.data_analysis_page)
        self.content_stack.addWidget(self.data_manage_page)
        
        # 添加左侧导航和右侧内容到主布局
        main_layout.addWidget(nav_frame)
        main_layout.addWidget(self.content_stack)
        
        # 设置状态栏
        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)
        self.statusBar.showMessage("就绪")
    
    def show_real_trade_module(self):
        """显示真实交易记录模块"""
        self.real_trade_btn.setChecked(True)
        self.backtest_btn.setChecked(False)
        self.data_module_btn.setChecked(False)
        self.data_manage_btn.setChecked(False)
        self.content_stack.setCurrentWidget(self.real_trade_page)
        self.statusBar.showMessage("真实交易记录模块尚未实现")
    
    def show_backtest_module(self):
        """显示行情回测模块"""
        self.real_trade_btn.setChecked(False)
        self.backtest_btn.setChecked(True)
        self.data_module_btn.setChecked(False)
        self.data_manage_btn.setChecked(False)
        
        # 延迟加载回测窗口
        if not self.main_window:
            self.main_window = MainWindow(self.config)
            self.prediction_window = PredictionWindow()
            self.main_window.prediction_window = self.prediction_window
            
            # 设置回测窗口为backtest_page的内容
            backtest_layout = QVBoxLayout(self.backtest_page)
            backtest_layout.setContentsMargins(0, 0, 0, 0)
            backtest_layout.addWidget(self.main_window)
        
        self.content_stack.setCurrentWidget(self.backtest_page)
        self.statusBar.showMessage("行情回测模块")
    
    def show_data_module(self):
        """显示数据分析模块"""
        self.real_trade_btn.setChecked(False)
        self.backtest_btn.setChecked(False)
        self.data_module_btn.setChecked(True)
        self.data_manage_btn.setChecked(False)
        self.content_stack.setCurrentWidget(self.data_analysis_page)
        self.statusBar.showMessage("数据分析模块尚未实现")
    
    def show_data_manage_module(self):
        """显示数据整理模块"""
        self.real_trade_btn.setChecked(False)
        self.backtest_btn.setChecked(False)
        self.data_module_btn.setChecked(False)
        self.data_manage_btn.setChecked(True)
        
        # 延迟加载数据整理窗口
        if not hasattr(self, 'data_manage_window') or self.data_manage_window is None:
            from backtest_gui.gui.data_manage_window import DataManageWindow
            self.data_manage_window = DataManageWindow(self.config)
            
            # 设置数据整理窗口为data_manage_page的内容
            data_manage_layout = QVBoxLayout(self.data_manage_page)
            data_manage_layout.setContentsMargins(0, 0, 0, 0)
            data_manage_layout.addWidget(self.data_manage_window)
        
        self.content_stack.setCurrentWidget(self.data_manage_page)
        self.statusBar.showMessage("数据整理模块")


def main():
    """主函数，程序入口点"""
    # 创建应用程序
    app = QApplication(sys.argv)
    
    # 创建并显示主应用窗口
    main_app = MainApplicationWindow()
    main_app.show()
    
    # 运行应用程序
    sys.exit(app.exec_())


if __name__ == '__main__':
    main() 