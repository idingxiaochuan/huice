#!/usr/bin/env python
# -*- coding: utf-8 -*-

from backtest_gui.utils.db_connector import DBConnector
from backtest_gui.utils.xirr_calculator_trades_only import XIRRCalculatorTradesOnly

def main():
    print("开始分析XIRR计算问题...")
    db = DBConnector()
    calculator = XIRRCalculatorTradesOnly(db)
    
    # 计算XIRR
    backtest_id = 184
    result = calculator.calculate_backtest_xirr(backtest_id)
    
    if result and 'xirr' in result:
        print(f"计算结果: {result['xirr']*100:.2f}%")
    else:
        print("计算失败，无结果")
    
    print("分析完成")

if __name__ == "__main__":
    main() 