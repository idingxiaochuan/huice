#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试脚本 - 测试XIRR计算是否已修复
"""
import sys
import traceback
from PyQt5.QtWidgets import QApplication, QMessageBox
from PyQt5.QtCore import Qt
from backtest_gui.gui.trade_report_window import TradeReportWindow
from backtest_gui.db.database import Database
from backtest_gui.utils.xirr_calculator_simple import XIRRCalculatorSimple

def test_xirr_calculation(backtest_id=170):
    """测试XIRR计算"""
    print(f"\n===== 测试XIRR计算 (回测ID: {backtest_id}) =====")
    
    try:
        # 创建数据库连接
        print("正在连接数据库...")
        db = Database()
        db.connect()
        print("数据库连接成功")
        
        # 创建XIRR计算器
        print("创建XIRR计算器...")
        calculator = XIRRCalculatorSimple(db)
        
        # 计算XIRR
        print("开始计算XIRR...")
        result = calculator.calculate_backtest_xirr(backtest_id)
        
        # 检查结果
        if result:
            print("\n===== XIRR计算结果 =====")
            print(f"回测ID: {result['backtest_info']['id']}")
            print(f"基金代码: {result['backtest_info']['stock_code']}")
            print(f"回测期间: {result['backtest_info']['start_date']} 至 {result['backtest_info']['end_date']}")
            print(f"初始资金: {result['backtest_info']['initial_capital']:,.2f}")
            print(f"最终资金: {result['backtest_info']['final_capital']:,.2f}")
            print(f"总收益: {result['backtest_info']['total_profit']:,.2f}")
            print(f"总收益率: {result['backtest_info']['total_profit_rate']:.2f}%")
            
            if result['xirr'] is not None:
                print(f"XIRR(年化收益率): {result['xirr']:.2f}%")
            else:
                print("XIRR(年化收益率): 无法计算")
                
            if result['has_incomplete_trades']:
                print("注意: 存在未完成交易，XIRR计算结果包含当前持仓价值")
                
            print("===== 测试成功 =====\n")
            return True
        else:
            print("错误: XIRR计算失败，结果为None")
            print("===== 测试失败 =====\n")
            return False
            
    except Exception as e:
        error_msg = str(e)
        error_tb = traceback.format_exc()
        
        print(f"测试过程中出错: {error_msg}")
        print(f"异常类型: {type(e).__name__}")
        print(f"调用堆栈:\n{error_tb}")
        print("===== 测试失败 =====\n")
        return False

def test_xirr_in_gui(backtest_id=170):
    """在GUI中测试XIRR计算"""
    print(f"\n===== 在GUI中测试XIRR计算 (回测ID: {backtest_id}) =====")
    
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
        
        # 设置当前回测ID
        window.current_backtest_id = backtest_id
        print(f"已设置当前回测ID: {backtest_id}")
        
        # 模拟点击XIRR按钮
        print("模拟点击XIRR按钮...")
        window.on_xirr_clicked()
        
        # 显示窗口
        window.show()
        print("窗口显示成功")
        
        # 显示测试完成消息
        QMessageBox.information(window, "测试完成", 
            f"XIRR计算测试已完成，回测ID: {backtest_id}\n\n"
            "请检查控制台输出是否有错误信息。\n"
            "如果没有错误信息，则表示修复成功。"
        )
        
        # 运行应用程序
        sys.exit(app.exec_())
        
    except Exception as e:
        print(f"GUI测试过程中出错: {str(e)}")
        traceback.print_exc()
        return False

def main():
    """主函数"""
    # 首先测试直接计算
    success = test_xirr_calculation()
    
    if success:
        # 如果直接计算成功，再测试GUI
        test_xirr_in_gui()
    else:
        print("直接计算失败，跳过GUI测试")

if __name__ == "__main__":
    main() 