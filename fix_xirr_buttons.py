#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
修复XIRR计算按钮状态的脚本 - 确保按钮始终可点击
"""
import os
import sys
import traceback

def fix_trade_report_window():
    """修复TradeReportWindow中按钮状态的问题"""
    print("\n===== 修复交易报告窗口按钮状态 =====")
    
    file_path = "backtest_gui/gui/trade_report_window.py"
    backup_path = "backtest_gui/gui/trade_report_window.py.bak"
    
    try:
        # 检查文件是否存在
        if not os.path.exists(file_path):
            print(f"错误: 文件不存在 {file_path}")
            return False
            
        # 创建备份
        import shutil
        print(f"创建备份: {backup_path}")
        shutil.copy2(file_path, backup_path)
        
        # 读取文件内容
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 1. 修复按钮初始化状态
        print("修复按钮初始化状态...")
        
        # 确保按钮默认启用
        content = content.replace(
            """        self.xirr_button = QPushButton("计算XIRR年化收益率")
        self.xirr_button.setEnabled(True)  # 修改：默认启用""",
            
            """        self.xirr_button = QPushButton("计算XIRR年化收益率")
        self.xirr_button.setEnabled(True)  # 默认启用，不需要选择回测"""
        )
        
        content = content.replace(
            """        self.trades_only_xirr_button = QPushButton("计算交易专用XIRR")
        self.trades_only_xirr_button.setEnabled(True)  # 默认启用""",
            
            """        self.trades_only_xirr_button = QPushButton("计算交易专用XIRR")
        self.trades_only_xirr_button.setEnabled(True)  # 默认启用，不需要选择回测"""
        )
        
        # 2. 修复on_xirr_clicked方法，确保即使未选择回测也能执行
        content = content.replace(
            """    def on_xirr_clicked(self):
        \"\"\"点击计算XIRR按钮事件\"\"\"
        
        if not self.current_backtest_id:
            QMessageBox.warning(self, "警告", "请先选择一个回测记录")
            return""",
            
            """    def on_xirr_clicked(self):
        \"\"\"点击计算XIRR按钮事件\"\"\"
        print("\\n=============== XIRR计算按钮点击 ===============")
        
        # 强制启用按钮
        self.xirr_button.setEnabled(True)
        
        # 如果未选择回测记录，尝试从表格中获取第一条记录
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
                print("表格中没有回测记录，请先查询")
                QMessageBox.warning(self, "警告", "没有可用的回测记录，请先进行查询")
                return"""
        )
        
        # 3. 修复on_trades_only_xirr_clicked方法，确保即使未选择回测也能执行
        content = content.replace(
            """    def on_trades_only_xirr_clicked(self):
        \"\"\"点击计算交易专用XIRR按钮事件\"\"\"
        print("\\n=============== 交易专用XIRR计算调试 ===============")
        print(f"当前选中的回测ID: {self.current_backtest_id}")
        
        # 强制启用按钮
        self.trades_only_xirr_button.setEnabled(True)
        
        if not self.current_backtest_id:
            print("错误: 未选中回测记录")
            QMessageBox.warning(self, "警告", "请先选择一个回测记录")
            return""",
            
            """    def on_trades_only_xirr_clicked(self):
        \"\"\"点击计算交易专用XIRR按钮事件\"\"\"
        print("\\n=============== 交易专用XIRR计算调试 ===============")
        print(f"当前选中的回测ID: {self.current_backtest_id}")
        
        # 强制启用按钮
        self.trades_only_xirr_button.setEnabled(True)
        
        # 如果未选择回测记录，尝试从表格中获取第一条记录
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
                print("表格中没有回测记录，请先查询")
                QMessageBox.warning(self, "警告", "没有可用的回测记录，请先进行查询")
                return"""
        )
        
        # 4. 修复on_export_excel_clicked方法，确保即使未选择回测也能执行
        content = content.replace(
            """    def on_export_excel_clicked(self):
        \"\"\"点击导出Excel按钮事件\"\"\"
        # 强制启用按钮
        self.xirr_button.setEnabled(True)
        self.export_excel_button.setEnabled(True)
        
        if not self.current_backtest_id:
            QMessageBox.warning(self, "警告", "请先选择一个回测记录")
            return""",
            
            """    def on_export_excel_clicked(self):
        \"\"\"点击导出Excel按钮事件\"\"\"
        print("\\n=============== 导出Excel按钮点击 ===============")
        
        # 强制启用按钮
        self.xirr_button.setEnabled(True)
        self.export_excel_button.setEnabled(True)
        
        # 如果未选择回测记录，尝试从表格中获取第一条记录
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
                print("表格中没有回测记录，请先查询")
                QMessageBox.warning(self, "警告", "没有可用的回测记录，请先进行查询")
                return"""
        )
        
        # 写入修改后的内容
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
            
        print("交易报告窗口按钮状态修复完成")
        return True
        
    except Exception as e:
        print(f"修复按钮状态失败: {str(e)}")
        traceback.print_exc()
        return False

def test_buttons_fix():
    """测试按钮修复是否成功"""
    try:
        # 导入必要的模块
        from PyQt5.QtWidgets import QApplication, QMessageBox
        from backtest_gui.db.database import Database
        from backtest_gui.gui.trade_report_window import TradeReportWindow
        
        print("初始化QApplication...")
        app = QApplication(sys.argv)
        
        print("连接数据库...")
        db = Database()
        db.connect()
        
        print("创建交易报告窗口...")
        window = TradeReportWindow(db)
        
        print("检查按钮状态...")
        if window.xirr_button.isEnabled() and window.trades_only_xirr_button.isEnabled():
            print("按钮状态正常：已启用")
            return True
        else:
            print("按钮状态异常：未启用")
            return False
            
    except Exception as e:
        print(f"测试按钮修复失败: {str(e)}")
        traceback.print_exc()
        return False

def main():
    """主函数"""
    print("===== 开始修复XIRR按钮状态 =====")
    
    # 修复按钮状态
    if fix_trade_report_window():
        # 测试按钮修复是否成功
        test_buttons_fix()
    else:
        print("修复失败")
        
    print("按钮修复完成，现在可以关闭此窗口并运行交易专用XIRR计算器")

if __name__ == "__main__":
    main() 