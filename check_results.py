#!/usr/bin/env python
# -*- coding: utf-8 -*-

from backtest_gui.utils.db_connector import DBConnector

def check_results():
    """检查回测结果"""
    backtest_id = 184
    
    # 获取数据库连接
    db = DBConnector()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    try:
        # 查询回测结果
        cursor.execute("""
            SELECT id, initial_capital, final_capital, total_profit, total_profit_rate
            FROM backtest_results
            WHERE id = %s
        """, (backtest_id,))
        
        result = cursor.fetchone()
        if result:
            print("回测结果:")
            print(f"ID: {result[0]}")
            print(f"初始资金: {float(result[1]):.2f}")
            print(f"最终资金: {float(result[2]):.2f}")
            print(f"总收益: {float(result[3]):.2f}")
            print(f"收益率: {float(result[4]):.2f}%")
        else:
            print(f"未找到回测ID: {backtest_id}的结果")
        
        # 查询XIRR结果
        cursor.execute("""
            SELECT id, xirr, xirr_value, total_buy_value, total_sell_value, 
                   remaining_shares, remaining_value, total_cash_flow
            FROM backtest_xirr
            WHERE backtest_id = %s
        """, (backtest_id,))
        
        xirr_result = cursor.fetchone()
        if xirr_result:
            print("\nXIRR计算结果:")
            print(f"ID: {xirr_result[0]}")
            print(f"XIRR: {float(xirr_result[1]):.4f}")
            print(f"XIRR百分比: {float(xirr_result[2]):.2f}%")
            print(f"总买入金额: {float(xirr_result[3]):.2f}")
            print(f"总卖出金额: {float(xirr_result[4]):.2f}")
            print(f"剩余股数: {int(xirr_result[5])}")
            print(f"剩余股票估值: {float(xirr_result[6]):.2f}")
            print(f"总现金流: {float(xirr_result[7]):.2f}")
        else:
            print(f"未找到回测ID: {backtest_id}的XIRR计算结果")
            
    except Exception as e:
        print(f"错误: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    check_results() 