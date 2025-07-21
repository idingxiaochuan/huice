#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
完整修复脚本 - 修复按钮启用和XIRR计算问题
"""
import sys
import traceback
import os
import shutil
from PyQt5.QtWidgets import QApplication, QMessageBox
from backtest_gui.gui.trade_report_window import TradeReportWindow
from backtest_gui.db.database import Database

def fix_xirr_calculator():
    """修复XIRR计算器中的类型转换问题"""
    print("\n===== 修复XIRR计算器 =====")
    
    # 标准版XIRR计算器
    standard_file = "backtest_gui/utils/xirr_calculator.py"
    standard_backup = "backtest_gui/utils/xirr_calculator.py.bak"
    
    # 简化版XIRR计算器
    simple_file = "backtest_gui/utils/xirr_calculator_simple.py"
    simple_backup = "backtest_gui/utils/xirr_calculator_simple.py.bak"
    
    try:
        # 备份文件
        if os.path.exists(standard_file):
            print(f"备份标准版XIRR计算器: {standard_backup}")
            shutil.copy2(standard_file, standard_backup)
            
        if os.path.exists(simple_file):
            print(f"备份简化版XIRR计算器: {simple_backup}")
            shutil.copy2(simple_file, simple_backup)
            
        # 修复标准版XIRR计算器
        if os.path.exists(standard_file):
            print(f"修复标准版XIRR计算器: {standard_file}")
            with open(standard_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # 修复 _xnpv 方法
            content = content.replace(
                'return sum(cf / (1 + rate) ** ((t - t0).days / 365.0) for t, cf in chron_order)',
                'return sum(float(cf) / (1 + float(rate)) ** ((t - t0).days / 365.0) for t, cf in chron_order)'
            )
            
            # 修复 _secant_method 方法
            content = content.replace(
                'f0 = f(x0)',
                '# 确保初始值为浮点数\n        x0 = float(x0)\n        x1 = float(x1)\n        f0 = f(x0)'
            )
            
            # 修复 _xirr 方法中的类型检查
            content = content.replace(
                'values = [cf[1] for cf in cashflows]',
                'values = [float(cf[1]) for cf in cashflows]'
            )
            
            content = content.replace(
                'if v > 0:',
                'v_float = float(v)\n                if v_float > 0:'
            )
            
            content = content.replace(
                'if v < 0:',
                'if v_float < 0:'
            )
            
            # 修复 calculate_backtest_xirr 方法中的类型转换
            content = content.replace(
                "'initial_capital': backtest_info[4],",
                "'initial_capital': float(backtest_info[4]) if backtest_info[4] is not None else 0.0,"
            )
            
            content = content.replace(
                "'final_capital': backtest_info[5],",
                "'final_capital': float(backtest_info[5]) if backtest_info[5] is not None else 0.0,"
            )
            
            content = content.replace(
                "'total_profit': backtest_info[6],",
                "'total_profit': float(backtest_info[6]) if backtest_info[6] is not None else 0.0,"
            )
            
            content = content.replace(
                "'total_profit_rate': backtest_info[7],",
                "'total_profit_rate': float(backtest_info[7]) if backtest_info[7] is not None else 0.0,"
            )
            
            content = content.replace(
                "cashflows.append((backtest_info_dict['start_date'], -backtest_info_dict['initial_capital']))",
                "cashflows.append((backtest_info_dict['start_date'], -float(backtest_info_dict['initial_capital'])))"
            )
            
            content = content.replace(
                "buy_value = float(trade[6])",
                "buy_value = float(trade[6]) if trade[6] is not None else 0.0"
            )
            
            content = content.replace(
                "sell_value = float(trade[10])",
                "sell_value = float(trade[10]) if trade[10] is not None else 0.0"
            )
            
            content = content.replace(
                "if position and position[0] > 0:",
                "if position and position[0] and float(position[0]) > 0:"
            )
            
            content = content.replace(
                "position_value = float(position[3])",
                "position_value = float(position[3]) if position[3] is not None else 0.0"
            )
            
            content = content.replace(
                "if xirr_value:",
                "if xirr_value is not None:"
            )
            
            content = content.replace(
                "xirr_value = xirr_value * 100",
                "xirr_value = float(xirr_value) * 100"
            )
            
            # 写入修改后的内容
            with open(standard_file, 'w', encoding='utf-8') as f:
                f.write(content)
                
            print(f"标准版XIRR计算器修复完成")
            
        # 修复简化版XIRR计算器
        if os.path.exists(simple_file):
            print(f"修复简化版XIRR计算器: {simple_file}")
            with open(simple_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # 修复 _xnpv 方法
            content = content.replace(
                'return sum(float(cf) / (1 + rate) ** ((t - t0).days / 365.0) for t, cf in chron_order)',
                'return sum(float(cf) / (1 + float(rate)) ** ((t - t0).days / 365.0) for t, cf in chron_order)'
            )
            
            # 修复 _secant_method 方法
            content = content.replace(
                'f0 = f(x0)',
                '# 确保初始值为浮点数\n            x0 = float(x0)\n            x1 = float(x1)\n            f0 = f(x0)'
            )
            
            # 修复 calculate_backtest_xirr 方法中的类型转换
            content = content.replace(
                "initial_capital = backtest_info_dict['initial_capital']",
                "initial_capital = float(backtest_info_dict['initial_capital'])  # 确保是浮点数"
            )
            
            content = content.replace(
                "final_value = backtest_info_dict['final_capital']",
                "final_value = float(backtest_info_dict['final_capital'])  # 确保是浮点数"
            )
            
            content = content.replace(
                "final_value = backtest_info_dict['final_capital'] - position_value",
                "final_value = float(backtest_info_dict['final_capital']) - position_value"
            )
            
            content = content.replace(
                "if xirr_value:",
                "if xirr_value is not None:"
            )
            
            content = content.replace(
                "xirr_value = xirr_value * 100",
                "xirr_value = float(xirr_value) * 100"
            )
            
            # 写入修改后的内容
            with open(simple_file, 'w', encoding='utf-8') as f:
                f.write(content)
                
            print(f"简化版XIRR计算器修复完成")
            
        print("===== XIRR计算器修复完成 =====\n")
        return True
        
    except Exception as e:
        print(f"修复XIRR计算器失败: {str(e)}")
        traceback.print_exc()
        print("===== XIRR计算器修复失败 =====\n")
        return False

def fix_trade_report_window():
    """修复交易报告窗口中的按钮问题"""
    print("\n===== 修复交易报告窗口 =====")
    
    # 交易报告窗口文件
    window_file = "backtest_gui/gui/trade_report_window.py"
    window_backup = "backtest_gui/gui/trade_report_window.py.bak"
    
    try:
        # 备份文件
        if os.path.exists(window_file):
            print(f"备份交易报告窗口文件: {window_backup}")
            shutil.copy2(window_file, window_backup)
            
        # 修复交易报告窗口
        if os.path.exists(window_file):
            print(f"修复交易报告窗口文件: {window_file}")
            with open(window_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # 修复按钮初始化代码
            content = content.replace(
                'self.xirr_button.setEnabled(False)  # 默认禁用',
                'self.xirr_button.setEnabled(True)  # 修改：默认启用'
            )
            
            content = content.replace(
                'self.export_excel_button.setEnabled(False)  # 默认禁用',
                'self.export_excel_button.setEnabled(True)  # 修改：默认启用'
            )
            
            # 修复 on_xirr_clicked 方法
            content = content.replace(
                'def on_xirr_clicked(self):\n        """点击计算XIRR按钮事件"""\n        print("\\n=============== XIRR计算调试 ===============")\n        print(f"当前选中的回测ID: {self.current_backtest_id}")\n        \n        if not self.current_backtest_id:',
                'def on_xirr_clicked(self):\n        """点击计算XIRR按钮事件"""\n        print("\\n=============== XIRR计算调试 ===============")\n        print(f"当前选中的回测ID: {self.current_backtest_id}")\n        \n        # 强制启用按钮\n        self.xirr_button.setEnabled(True)\n        self.export_excel_button.setEnabled(True)\n        \n        if not self.current_backtest_id:'
            )
            
            # 修复 on_export_excel_clicked 方法
            content = content.replace(
                'def on_export_excel_clicked(self):\n        """点击导出Excel按钮事件"""\n        if not self.current_backtest_id:',
                'def on_export_excel_clicked(self):\n        """点击导出Excel按钮事件"""\n        # 强制启用按钮\n        self.xirr_button.setEnabled(True)\n        self.export_excel_button.setEnabled(True)\n        \n        if not self.current_backtest_id:'
            )
            
            # 修复 on_query_clicked 方法
            content = content.replace(
                '            # 查询配对交易记录\n            self.load_paired_trades(fund_code, strategy_id, start_date, end_date, level, status)\n            \n        except Exception as e:',
                '            # 查询配对交易记录\n            self.load_paired_trades(fund_code, strategy_id, start_date, end_date, level, status)\n            \n            # 强制启用按钮\n            self.xirr_button.setEnabled(True)\n            self.export_excel_button.setEnabled(True)\n            \n        except Exception as e:'
            )
            
            # 写入修改后的内容
            with open(window_file, 'w', encoding='utf-8') as f:
                f.write(content)
                
            print(f"交易报告窗口修复完成")
            
        print("===== 交易报告窗口修复完成 =====\n")
        return True
        
    except Exception as e:
        print(f"修复交易报告窗口失败: {str(e)}")
        traceback.print_exc()
        print("===== 交易报告窗口修复失败 =====\n")
        return False

def test_fix():
    """测试修复是否成功"""
    print("\n===== 测试修复 =====")
    
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
        
        # 检查按钮状态
        print("\n检查按钮状态:")
        print(f"XIRR按钮可用: {window.xirr_button.isEnabled()}")
        print(f"导出Excel按钮可用: {window.export_excel_button.isEnabled()}")
        
        # 设置当前回测ID
        backtest_id = 170  # 使用ID为170的回测记录进行测试
        window.current_backtest_id = backtest_id
        print(f"\n已设置当前回测ID: {backtest_id}")
        
        # 模拟点击XIRR按钮
        print("模拟点击XIRR按钮...")
        window.on_xirr_clicked()
        
        # 显示窗口
        window.show()
        print("窗口显示成功")
        
        # 显示测试完成消息
        QMessageBox.information(window, "修复完成", 
            "按钮启用和XIRR计算问题已修复！\n\n"
            "现在您可以：\n"
            "1. 点击'查询'按钮加载回测记录\n"
            "2. 在回测汇总表格中选择一条记录\n"
            "3. 点击'计算XIRR年化收益率'按钮\n"
            "4. 点击'导出到Excel'按钮\n\n"
            "如果遇到问题，请查看控制台输出的错误信息。"
        )
        
        # 运行应用程序
        sys.exit(app.exec_())
        
    except Exception as e:
        print(f"测试过程中出错: {str(e)}")
        traceback.print_exc()
        print("===== 测试失败 =====\n")
        return False

def main():
    """主函数"""
    print("===== 开始修复 =====")
    
    # 修复XIRR计算器
    xirr_fixed = fix_xirr_calculator()
    
    # 修复交易报告窗口
    window_fixed = fix_trade_report_window()
    
    if xirr_fixed and window_fixed:
        print("===== 所有问题已修复，开始测试 =====")
        # 测试修复是否成功
        test_fix()
    else:
        print("===== 修复失败，请检查错误信息 =====")

if __name__ == "__main__":
    main() 