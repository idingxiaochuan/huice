#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据库测试工具 - 测试XIRR功能
"""
import sys
import traceback
from PyQt5.QtWidgets import QApplication, QMainWindow, QPushButton, QVBoxLayout, QWidget, QLabel, QMessageBox, QTextEdit, QInputDialog, QHBoxLayout
from PyQt5.QtCore import Qt

from backtest_gui.db.database import Database
from backtest_gui.utils.xirr_calculator_simple import XIRRCalculatorSimple

class DbTester(QMainWindow):
    """数据库测试工具，用于测试XIRR功能"""
    
    def __init__(self):
        super().__init__()
        
        # 初始化数据库连接
        self.db = Database()
        self.db.connect()
        print("数据库连接成功")
        
        # 初始化XIRR计算器
        self.xirr_calculator = XIRRCalculatorSimple(self.db)
        print("XIRR计算器初始化成功")
        
        self.init_ui()
        
    def init_ui(self):
        """初始化UI"""
        self.setWindowTitle("XIRR数据库测试工具")
        self.setGeometry(200, 200, 800, 600)
        
        # 创建中央部件
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # 创建主布局
        layout = QVBoxLayout(central_widget)
        
        # 添加说明
        info_label = QLabel("XIRR数据库测试工具 - 用于测试XIRR计算功能")
        layout.addWidget(info_label)
        
        # 创建按钮布局
        button_layout = QHBoxLayout()
        
        # 创建查询按钮
        self.query_button = QPushButton("查询回测记录")
        self.query_button.clicked.connect(self.query_backtest_records)
        button_layout.addWidget(self.query_button)
        
        # 创建计算按钮
        self.calc_button = QPushButton("计算XIRR")
        self.calc_button.clicked.connect(self.calculate_xirr)
        button_layout.addWidget(self.calc_button)
        
        # 创建导出按钮
        self.export_button = QPushButton("导出到Excel")
        self.export_button.clicked.connect(self.export_to_excel)
        button_layout.addWidget(self.export_button)
        
        # 添加按钮布局
        layout.addLayout(button_layout)
        
        # 创建日志输出区域
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        layout.addWidget(self.log_text)
        
        # 初始化
        self.current_backtest_id = None
        self.calc_button.setEnabled(False)
        self.export_button.setEnabled(False)
        
    def log(self, message):
        """添加日志"""
        self.log_text.append(str(message))
        print(str(message))
        
    def query_backtest_records(self):
        """查询回测记录"""
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            # 查询回测记录
            cursor.execute("""
                SELECT id, stock_code, start_date, end_date, initial_capital, 
                       final_capital, total_profit, total_profit_rate
                FROM backtest_results
                ORDER BY id DESC
                LIMIT 10
            """)
            
            records = cursor.fetchall()
            self.db.release_connection(conn)
            
            if not records:
                self.log("未找到回测记录")
                return
                
            # 显示回测记录
            self.log("\n===== 最近10条回测记录 =====")
            for record in records:
                self.log(f"回测ID: {record[0]}, 股票代码: {record[1]}, 期间: {record[2]} 至 {record[3]}")
                self.log(f"初始资金: {record[4]}, 最终资金: {record[5]}, 总收益: {record[6]}, 收益率: {record[7]}%")
                self.log("-" * 40)
            
            # 提示用户选择回测ID
            backtest_id, ok = QInputDialog.getInt(self, "选择回测ID", "请输入要测试的回测ID:", 
                                               records[0][0], 1, 9999, 1)
            if ok:
                self.current_backtest_id = backtest_id
                self.calc_button.setEnabled(True)
                self.export_button.setEnabled(True)
                self.log(f"\n已选择回测ID: {backtest_id}")
                
                # 查询该回测的配对交易记录数量
                conn = self.db.get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) FROM backtest_paired_trades WHERE backtest_id = %s
                """, (backtest_id,))
                count = cursor.fetchone()[0]
                self.db.release_connection(conn)
                
                self.log(f"该回测共有 {count} 条配对交易记录")
                
                # 查询几条样本数据
                if count > 0:
                    conn = self.db.get_connection()
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT id, level, grid_type, buy_time, buy_price, sell_time, sell_price, band_profit, sell_band_profit_rate, status
                        FROM backtest_paired_trades 
                        WHERE backtest_id = %s
                        LIMIT 5
                    """, (backtest_id,))
                    samples = cursor.fetchall()
                    self.db.release_connection(conn)
                    
                    self.log("\n===== 配对交易样本 =====")
                    for sample in samples:
                        self.log(f"ID: {sample[0]}, 级别: {sample[1]}, 类型: {sample[2]}")
                        self.log(f"买入: 时间={sample[3]}, 价格={sample[4]}")
                        self.log(f"卖出: 时间={sample[5]}, 价格={sample[6]}")
                        self.log(f"收益: {sample[7]}, 收益率: {sample[8]}%, 状态: {sample[9]}")
                        self.log("-" * 40)
                
        except Exception as e:
            self.log(f"查询回测记录出错: {str(e)}")
            traceback.print_exc()
            
    def calculate_xirr(self):
        """计算XIRR"""
        if not self.current_backtest_id:
            QMessageBox.warning(self, "警告", "请先选择一个回测记录")
            return
            
        try:
            self.log(f"\n===== 开始计算XIRR，回测ID: {self.current_backtest_id} =====")
            
            # 计算XIRR
            result = self.xirr_calculator.calculate_backtest_xirr(self.current_backtest_id)
            
            if not result:
                self.log("计算XIRR失败，结果为None")
                return
                
            # 显示计算结果
            self.log("\n===== XIRR计算结果 =====")
            self.log(f"回测ID: {result['backtest_info']['id']}")
            self.log(f"股票代码: {result['backtest_info']['stock_code']}")
            self.log(f"回测期间: {result['backtest_info']['start_date']} 至 {result['backtest_info']['end_date']}")
            self.log(f"初始资金: {result['backtest_info']['initial_capital']}")
            self.log(f"最终资金: {result['backtest_info']['final_capital']}")
            self.log(f"总收益: {result['backtest_info']['total_profit']}")
            self.log(f"总收益率: {result['backtest_info']['total_profit_rate']}%")
            
            if result['xirr'] is not None:
                self.log(f"XIRR(年化收益率): {result['xirr']:.2f}%")
            else:
                self.log("XIRR(年化收益率): 无法计算")
                
            if result['has_incomplete_trades']:
                self.log("注意: 存在未完成交易，XIRR计算结果包含当前持仓价值")
                
            # 显示现金流详情
            self.log("\n===== 现金流详情 =====")
            for i, (date, amount) in enumerate(result['cashflows'].values):
                self.log(f"现金流 {i+1}: 日期={date}, 金额={amount}")
            
        except Exception as e:
            self.log(f"计算XIRR异常: {str(e)}")
            self.log(f"异常堆栈:\n{traceback.format_exc()}")
            
    def export_to_excel(self):
        """导出到Excel"""
        if not self.current_backtest_id:
            QMessageBox.warning(self, "警告", "请先选择一个回测记录")
            return
            
        try:
            self.log(f"\n===== 开始导出到Excel，回测ID: {self.current_backtest_id} =====")
            
            # 导出到Excel
            file_path = f"xirr_report_{self.current_backtest_id}.xlsx"
            success = self.xirr_calculator.export_to_excel(self.current_backtest_id, file_path)
            
            if success:
                self.log(f"XIRR计算结果已导出至: {file_path}")
            else:
                self.log("导出Excel失败")
                
        except Exception as e:
            self.log(f"导出Excel异常: {str(e)}")
            self.log(f"异常堆栈:\n{traceback.format_exc()}")
            
    def closeEvent(self, event):
        """关闭窗口事件"""
        # 断开数据库连接
        if hasattr(self, 'db') and self.db:
            print("数据库连接已关闭")
        event.accept()
        
def main():
    """主函数"""
    app = QApplication(sys.argv)
    window = DbTester()
    window.show()
    sys.exit(app.exec_())
    
if __name__ == "__main__":
    main() 