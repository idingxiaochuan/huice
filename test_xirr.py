#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试指定回测ID的XIRR计算
"""
from backtest_gui.utils.db_connector import DBConnector
from backtest_gui.utils.xirr_calculator_trades_only import XIRRCalculatorTradesOnly

def test_xirr_calculation(backtest_id):
    """测试指定回测ID的XIRR计算"""
    print(f"测试回测ID: {backtest_id} 的XIRR计算")
    
    # 初始化数据库连接器
    db_connector = DBConnector()
    
    # 初始化XIRR计算器
    xirr_calculator = XIRRCalculatorTradesOnly(db_connector)
    
    # 计算XIRR
    result = xirr_calculator.calculate_backtest_xirr(backtest_id)
    
    if result:
        print("\nXIRR计算结果:")
        print(f"XIRR值: {result['xirr'] * 100:.4f}%")
        print(f"总买入金额: {result['total_buy_value']:.2f}")
        print(f"总卖出金额: {result['total_sell_value']:.2f}")
        print(f"剩余股数: {result['remaining_shares']}")
        print(f"剩余股票估值: {result['remaining_value']:.2f}")
        print(f"总现金流: {result['total_cash_flow']:.2f}")
    else:
        print("XIRR计算失败")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        backtest_id = int(sys.argv[1])
    else:
        backtest_id = 197  # 默认测试回测ID
    
    test_xirr_calculation(backtest_id) 