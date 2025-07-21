#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
修复脚本 - 通过猴子补丁(monkey patch)修复交易报告窗口中的按钮状态问题
"""
import sys
import traceback
from PyQt5.QtWidgets import QApplication
from backtest_gui.gui.trade_report_window import TradeReportWindow
from backtest_gui.db.database import Database

# 保存原始方法
original_init_ui = TradeReportWindow.init_ui

# 定义补丁方法
def patched_init_ui(self):
    """补丁方法 - 确保按钮始终可点击"""
    # 调用原始方法
    original_init_ui(self)
    
    # 强制启用按钮
    self.xirr_button.setEnabled(True)
    self.export_excel_button.setEnabled(True)
    
    print("\n===== 按钮补丁已应用 =====")
    print(f"XIRR按钮可用: {self.xirr_button.isEnabled()}")
    print(f"导出Excel按钮可用: {self.export_excel_button.isEnabled()}")
    print("=========================\n")

def main():
    """主函数"""
    try:
        # 应用猴子补丁
        TradeReportWindow.init_ui = patched_init_ui
        print("已应用按钮状态修复补丁")
        
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
        
        # 显示窗口
        window.show()
        print("窗口显示成功")
        
        # 运行应用程序
        sys.exit(app.exec_())
        
    except Exception as e:
        print(f"修复过程中出错: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main() 