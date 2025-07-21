#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试导出功能
"""
import os
from backtest_gui.utils.db_connector import DBConnector
from backtest_gui.utils.xirr_calculator_trades_only import XIRRCalculatorTradesOnly

def test_export_to_excel(backtest_id):
    """测试导出功能"""
    print(f"测试导出回测ID: {backtest_id} 的XIRR计算结果到Excel")
    
    # 初始化数据库连接器
    db_connector = DBConnector()
    
    # 初始化XIRR计算器
    xirr_calculator = XIRRCalculatorTradesOnly(db_connector)
    
    # 创建导出路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, f"xirr_export_{backtest_id}.xlsx")
    
    # 导出到Excel
    result = xirr_calculator.export_to_excel(backtest_id, file_path)
    
    if result:
        print(f"导出成功: {file_path}")
    else:
        print("导出失败")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        backtest_id = int(sys.argv[1])
    else:
        backtest_id = 197  # 默认测试回测ID
    
    test_export_to_excel(backtest_id) 