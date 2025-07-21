#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
直接修复脚本 - 修改on_xirr_clicked方法，使其即使没有选择回测记录也能执行
"""
import sys
import traceback
from PyQt5.QtWidgets import QApplication, QMessageBox
from PyQt5.QtCore import Qt
from backtest_gui.gui.trade_report_window import TradeReportWindow
from backtest_gui.db.database import Database

# 保存原始方法
original_on_xirr_clicked = TradeReportWindow.on_xirr_clicked

# 定义补丁方法
def patched_on_xirr_clicked(self):
    """补丁方法 - 确保即使没有选择回测记录也能执行"""
    print("\n=============== XIRR计算调试(补丁版) ===============")
    
    # 如果没有选择回测记录，尝试从表格中获取第一条记录
    if not self.current_backtest_id:
        print("未选中回测记录，尝试获取第一条记录...")
        
        # 检查表格是否有数据
        if self.summary_table.rowCount() > 0:
            # 获取第一行的回测ID
            backtest_id = int(self.summary_table.item(0, 0).text())
            print(f"自动选择第一条回测记录: ID={backtest_id}")
            
            # 设置当前回测ID
            self.current_backtest_id = backtest_id
            
            # 加载该回测的配对交易
            self.load_paired_trades(backtest_id=backtest_id)
            
            # 选中第一行
            self.summary_table.selectRow(0)
        else:
            print("表格中没有回测记录")
            QMessageBox.warning(self, "警告", "没有可用的回测记录，请先进行查询")
            return
    
    # 调用原始方法
    try:
        original_on_xirr_clicked(self)
    except Exception as e:
        # 恢复鼠标光标
        QApplication.restoreOverrideCursor()
        
        # 显示错误信息
        error_msg = str(e)
        error_tb = traceback.format_exc()
        
        print(f"计算XIRR异常(补丁处理): {error_msg}")
        print(f"异常类型: {type(e).__name__}")
        print(f"调用堆栈:\n{error_tb}")
        
        # 显示详细错误对话框
        detailed_msg = f"错误类型: {type(e).__name__}\n\n详细信息: {error_msg}\n\n调用堆栈:\n{error_tb}"
        QMessageBox.critical(self, "XIRR计算错误(补丁处理)", detailed_msg)
    
    print("============= XIRR计算调试(补丁版)结束 =============\n")

def main():
    """主函数"""
    try:
        # 应用猴子补丁
        TradeReportWindow.on_xirr_clicked = patched_on_xirr_clicked
        print("已应用XIRR计算方法修复补丁")
        
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
        
        # 运行应用程序
        sys.exit(app.exec_())
        
    except Exception as e:
        print(f"修复过程中出错: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main() 