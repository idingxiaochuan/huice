#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
交易专用XIRR计算器完整修复脚本 - 一次性修复所有问题
"""
import os
import sys
import traceback
import shutil
from datetime import datetime
import pandas as pd

def fix_trades_only_xirr_calculator():
    """修复交易专用XIRR计算器"""
    print("\n===== 修复交易专用XIRR计算器 =====")
    
    # 交易专用XIRR计算器文件路径
    file_path = "backtest_gui/utils/xirr_calculator_trades_only.py"
    backup_path = "backtest_gui/utils/xirr_calculator_trades_only.py.bak"
    
    try:
        # 检查文件是否存在
        if not os.path.exists(file_path):
            print(f"错误: 文件不存在 {file_path}")
            return False
            
        # 创建备份
        print(f"创建备份: {backup_path}")
        shutil.copy2(file_path, backup_path)
        
        # 读取文件内容
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # 修复可能的问题
        
        # 1. 修复重复的空值判断逻辑
        content = content.replace(
            'buy_value = float(trade[6]) if trade[6] is not None else 0.0 if trade[6] is not None else 0.0',
            'buy_value = float(trade[6]) if trade[6] is not None else 0.0'
        )
        
        content = content.replace(
            'sell_value = float(trade[10]) if trade[10] is not None else 0.0 if trade[10] is not None else 0.0',
            'sell_value = float(trade[10]) if trade[10] is not None else 0.0'
        )
        
        content = content.replace(
            'remaining_value = float(position[3]) if position[3] is not None else 0.0 if position[3] is not None else 0.0',
            'remaining_value = float(position[3]) if position[3] is not None else 0.0'
        )
        
        # 2. 修复可能的现金流计算问题
        # 确保现金流的金额是浮点数
        content = content.replace(
            'cashflows.append((buy_time, -buy_value))',
            'cashflows.append((buy_time, -float(buy_value)))'
        )
        
        content = content.replace(
            'cashflows.append((sell_time, sell_value))',
            'cashflows.append((sell_time, float(sell_value)))'
        )
        
        content = content.replace(
            'cashflows.append((end_date, remaining_value))',
            'cashflows.append((end_date, float(remaining_value)))'
        )
        
        # 3. 改进现金流有效性检查
        content = content.replace(
            """            # 检查是否所有现金流都是同一个值
            values = [float(cf[1]) for cf in cashflows]
            if all(x == values[0] for x in values):
                print("所有现金流值相同，无法计算XIRR")
                return 0.0
            
            # 检查是否至少有一个正现金流和一个负现金流
            pos = False
            neg = False
            for _, v in cashflows:
                v_float = float(v)
                if v_float > 0:
                    pos = True
                if v_float < 0:
                    neg = True
            if not (pos and neg):
                print("现金流需要同时包含正值和负值")
                return 0.0""",
                
            """            # 检查是否有足够有效的现金流进行计算
            # 转换现金流金额为浮点数并检查是否有有效值
            values = []
            for _, v in cashflows:
                try:
                    v_float = float(v)
                    if v_float != 0:  # 排除0值现金流
                        values.append(v_float)
                except (ValueError, TypeError):
                    continue
                    
            if len(values) < 2:
                print("没有足够的有效现金流进行计算(至少需要2个非零值)")
                return None
                
            # 检查是否所有现金流都是同一个符号
            if all(v > 0 for v in values) or all(v < 0 for v in values):
                print("现金流需要同时包含正值和负值")
                return None
                
            # 检查是否所有现金流都是同一个值
            unique_values = set(abs(v) for v in values)
            if len(unique_values) < 2:
                print("所有现金流绝对值相同，无法计算合理的XIRR")
                return None"""
        )
        
        # 4. 增强错误处理和日志记录
        content = content.replace(
            'print(f"割线法迭代异常: {str(e)}")',
            'print(f"割线法迭代异常: {str(e)}")\ntraceback.print_exc()'
        )
        
        content = content.replace(
            'print(f"XIRR计算异常: {str(e)}")',
            'print(f"XIRR计算异常: {str(e)}")\ntraceback.print_exc()'
        )
        
        # 5. 修复计算过程中可能的浮点数转换问题
        content = content.replace(
            'return sum(float(cf) / (1 + float(rate)) ** ((t - t0).days / 365.0) for t, cf in chron_order)',
            '''# 确保所有值都是浮点数，并处理可能的异常
            result = 0.0
            for t, cf in chron_order:
                try:
                    # 明确转换为浮点数
                    cf_float = float(cf)
                    days = (t - t0).days
                    # 处理日期可能相同的情况
                    if days < 0:
                        print(f"警告: 日期顺序错误 {t} < {t0}")
                        days = 0
                    exponent = days / 365.0
                    denominator = (1 + float(rate)) ** exponent
                    # 避免除以0
                    if denominator == 0:
                        print(f"警告: 除数为0 (rate={rate}, days={days})")
                        denominator = 1e-10
                    result += cf_float / denominator
                except Exception as calc_e:
                    print(f"计算单个现金流XNPV异常: {str(calc_e)}")
            return result'''
        )
        
        # 6. 优化数据库连接释放
        content = content.replace(
            """            # 释放数据库连接
            self.db_connector.release_connection(conn)""",
            
            """            try:
                # 确保释放数据库连接
                if conn:
                    self.db_connector.release_connection(conn)
            except Exception as conn_e:
                print(f"释放数据库连接异常: {str(conn_e)}")"""
        )
        
        # 写入修改后的内容
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
            
        print(f"交易专用XIRR计算器修复完成: {file_path}")
        return True
        
    except Exception as e:
        print(f"修复交易专用XIRR计算器失败: {str(e)}")
        traceback.print_exc()
        return False

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

def test_calculator():
    """测试计算器修复效果"""
    print("\n===== 测试XIRR计算器 =====")
    
    try:
        # 导入必要的模块
        from backtest_gui.db.database import Database
        from backtest_gui.utils.xirr_calculator_trades_only import XIRRCalculatorTradesOnly
        
        # 创建数据库连接
        print("连接数据库...")
        db = Database()
        success = db.connect()
        if not success:
            print("数据库连接失败")
            return False
        
        print("数据库连接成功")
        
        # 创建交易专用XIRR计算器
        print("创建交易专用XIRR计算器...")
        calculator = XIRRCalculatorTradesOnly(db)
        
        # 查询最新的回测ID
        print("查询最新的回测ID...")
        conn = db.get_connection()
        if not conn:
            print("获取数据库连接失败")
            return False
            
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM backtest_results ORDER BY id DESC LIMIT 1")
        result = cursor.fetchone()
        
        if not result:
            print("未找到任何回测记录")
            db.release_connection(conn)
            return False
            
        backtest_id = result[0]
        print(f"找到最新回测ID: {backtest_id}")
        db.release_connection(conn)
        
        # 计算XIRR
        print(f"计算交易专用XIRR (回测ID={backtest_id})...")
        result = calculator.calculate_backtest_xirr(backtest_id)
        
        # 检查结果
        if not result:
            print("计算失败，结果为None")
            return False
            
        print("\n===== 计算结果 =====")
        print(f"回测ID: {result['backtest_info']['id']}")
        print(f"股票代码: {result['backtest_info']['stock_code']}")
        print(f"回测期间: {result['backtest_info']['start_date']} 至 {result['backtest_info']['end_date']}")
        print(f"总收益率: {result['backtest_info']['total_profit_rate']:.2f}%")
        
        if result['xirr'] is not None:
            print(f"交易专用XIRR: {result['xirr']:.2f}%")
            return True
        else:
            print("交易专用XIRR: 无法计算")
            return False
            
    except Exception as e:
        print(f"测试失败: {str(e)}")
        traceback.print_exc()
        return False

def launch_gui():
    """启动GUI界面"""
    print("\n===== 启动交易报告窗口 =====")
    
    try:
        from PyQt5.QtWidgets import QApplication
        from backtest_gui.db.database import Database
        from backtest_gui.gui.trade_report_window import TradeReportWindow
        
        # 创建应用程序
        app = QApplication(sys.argv)
        
        # 创建数据库连接
        print("连接数据库...")
        db = Database()
        db.connect()
        
        # 创建交易报告窗口
        print("创建交易报告窗口...")
        window = TradeReportWindow(db)
        
        # 强制启用按钮
        window.xirr_button.setEnabled(True)
        window.trades_only_xirr_button.setEnabled(True)
        window.export_excel_button.setEnabled(True)
        
        # 显示窗口
        window.show()
        print("窗口显示成功")
        
        # 运行应用程序
        sys.exit(app.exec_())
        
    except Exception as e:
        print(f"启动GUI失败: {str(e)}")
        traceback.print_exc()
        return False

def main():
    """主函数"""
    print("===== 交易专用XIRR计算器修复程序 =====")
    print("该程序将修复以下问题:")
    print("1. 交易专用XIRR计算器的计算问题")
    print("2. 交易报告窗口中按钮无法点击的问题")
    
    # 修复交易专用XIRR计算器
    xirr_fixed = fix_trades_only_xirr_calculator()
    
    # 修复按钮状态
    button_fixed = fix_trade_report_window()
    
    # 测试修复效果
    if xirr_fixed and button_fixed:
        print("\n所有修复已完成，开始测试...")
        test_success = test_calculator()
        
        if test_success:
            print("\n===== 修复成功 =====")
            print("交易专用XIRR计算器现在可以正常工作")
            
            # 询问是否启动GUI
            while True:
                answer = input("\n是否立即启动交易报告窗口? (y/n): ").lower()
                if answer == 'y':
                    launch_gui()
                    break
                elif answer == 'n':
                    print("\n您可以稍后通过运行'交易专用XIRR计算器.bat'启动")
                    break
                else:
                    print("请输入y或n")
        else:
            print("\n===== 修复不完全 =====")
            print("交易专用XIRR计算器修复完成，但测试未通过")
            print("请尝试运行'交易专用XIRR计算器.bat'并查看详细日志")
    else:
        print("\n===== 修复失败 =====")
        if not xirr_fixed:
            print("交易专用XIRR计算器修复失败")
        if not button_fixed:
            print("按钮状态修复失败")

if __name__ == "__main__":
    main() 