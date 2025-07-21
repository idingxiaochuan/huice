#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
最终修复脚本 - 整合所有修改，确保按钮可点击
"""
import sys
import traceback
from PyQt5.QtWidgets import QApplication, QMessageBox
from backtest_gui.gui.trade_report_window import TradeReportWindow
from backtest_gui.db.database import Database

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
        
        # 强制启用按钮
        window.xirr_button.setEnabled(True)
        window.export_excel_button.setEnabled(True)
        print("已强制启用按钮")
        
        # 显示窗口
        window.show()
        print("窗口显示成功")
        
        # 显示使用说明
        QMessageBox.information(window, "使用说明", 
            "按钮已成功启用，现在您可以：\n\n"
            "1. 点击'查询'按钮加载回测记录\n"
            "2. 在回测汇总表格中选择一条记录\n"
            "3. 点击'计算XIRR年化收益率'按钮\n"
            "4. 点击'导出到Excel'按钮\n\n"
            "如果遇到问题，请查看控制台输出的错误信息"
        )
        
        # 运行应用程序
        sys.exit(app.exec_())
        
    except Exception as e:
        print(f"启动过程中出错: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main() 