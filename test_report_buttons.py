#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试脚本 - 测试交易报告窗口中的按钮是否可点击
"""
import sys
import traceback
from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import QTimer
from backtest_gui.gui.trade_report_window import TradeReportWindow
from backtest_gui.db.database import Database

def check_buttons(window):
    """检查按钮状态"""
    print("\n===== 按钮状态检查 =====")
    print(f"XIRR按钮可用: {window.xirr_button.isEnabled()}")
    print(f"导出Excel按钮可用: {window.export_excel_button.isEnabled()}")
    
    # 强制启用按钮
    window.xirr_button.setEnabled(True)
    window.export_excel_button.setEnabled(True)
    
    print("\n===== 强制启用后 =====")
    print(f"XIRR按钮可用: {window.xirr_button.isEnabled()}")
    print(f"导出Excel按钮可用: {window.export_excel_button.isEnabled()}")
    print("======================\n")

def main():
    """主函数"""
    try:
        # 创建应用程序
        app = QApplication(sys.argv)
        
        # 创建数据库连接
        print("正在连接数据库...")
        db = Database()
        db.connect()
        print("数据库连接成功")
        
        # 创建交易报告窗口
        print("创建交易报告窗口...")
        window = TradeReportWindow(db)
        
        # 设置定时器检查按钮状态
        QTimer.singleShot(1000, lambda: check_buttons(window))
        
        # 显示窗口
        window.show()
        print("窗口显示成功")
        
        # 运行应用程序
        sys.exit(app.exec_())
        
    except Exception as e:
        print(f"测试过程中出错: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main() 