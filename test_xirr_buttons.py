#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试XIRR计算按钮问题的脚本
"""
import sys
import traceback

from backtest_gui.db.database import Database
from backtest_gui.utils.xirr_calculator_simple import XIRRCalculatorSimple

def test_xirr_calculator():
    """测试XIRR计算器功能"""
    try:
        # 连接数据库
        db = Database()
        if not db.connect():
            print("数据库连接失败")
            return False
        
        print("数据库连接成功")
        
        # 创建XIRR计算器
        calculator = XIRRCalculatorSimple(db)
        
        # 获取一个回测ID用于测试
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM backtest_results ORDER BY id DESC LIMIT 1")
        result = cursor.fetchone()
        db.release_connection(conn)
        
        if not result:
            print("找不到回测记录")
            return False
        
        backtest_id = result[0]
        print(f"使用回测ID: {backtest_id} 进行测试")
        
        # 查询该回测的配对交易记录数量
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM backtest_paired_trades WHERE backtest_id = %s
        """, (backtest_id,))
        count = cursor.fetchone()[0]
        db.release_connection(conn)
        
        if count == 0:
            print(f"错误: 该回测没有配对交易记录")
            return False
            
        print(f"该回测有 {count} 条配对交易记录")
        
        # 计算XIRR
        print("\n===== 计算XIRR =====")
        result = calculator.calculate_backtest_xirr(backtest_id)
        
        if not result:
            print("计算XIRR失败，结果为None")
            return False
            
        # 显示XIRR计算结果
        print("\n===== XIRR计算结果 =====")
        print(f"回测ID: {result['backtest_info']['id']}")
        print(f"股票代码: {result['backtest_info']['stock_code']}")
        print(f"回测期间: {result['backtest_info']['start_date']} 至 {result['backtest_info']['end_date']}")
        print(f"初始资金: {result['backtest_info']['initial_capital']}")
        print(f"最终资金: {result['backtest_info']['final_capital']}")
        print(f"总收益: {result['backtest_info']['total_profit']}")
        print(f"总收益率: {result['backtest_info']['total_profit_rate']}%")
        
        if result['xirr'] is not None:
            print(f"XIRR(年化收益率): {result['xirr']:.2f}%")
        else:
            print("XIRR(年化收益率): 无法计算")
            
        if result['has_incomplete_trades']:
            print("注意: 存在未完成交易，XIRR计算结果包含当前持仓价值")
        
        # 测试导出到Excel
        print("\n===== 导出到Excel =====")
        file_path = f"xirr_test_report_{backtest_id}.xlsx"
        success = calculator.export_to_excel(backtest_id, file_path)
        
        if success:
            print(f"XIRR计算结果已导出至: {file_path}")
        else:
            print("导出Excel失败")
            
        return True
        
    except Exception as e:
        print(f"测试异常: {str(e)}")
        traceback.print_exc()
        return False
        
if __name__ == "__main__":
    success = test_xirr_calculator()
    print(f"\n测试结果: {'成功' if success else '失败'}")
    sys.exit(0 if success else 1) 