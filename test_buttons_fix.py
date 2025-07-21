#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试脚本 - 修复交易报告窗口中的按钮无法点击的问题
"""
import sys
from PyQt5.QtWidgets import QApplication
from backtest_gui.gui.trade_report_window import TradeReportWindow
from backtest_gui.db.database import Database

def main():
    """主函数"""
    # 创建应用程序
    app = QApplication(sys.argv)
    
    # 创建数据库连接
    db = Database()
    db.connect()
    
    # 创建交易报告窗口
    window = TradeReportWindow(db)
    
    # 强制启用按钮
    window.xirr_button.setEnabled(True)
    window.export_excel_button.setEnabled(True)
    
    # 显示窗口
    window.show()
    
    # 运行应用程序
    sys.exit(app.exec_())

if __name__ == "__main__":
    main() 