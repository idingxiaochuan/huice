#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
XIRR回测交易分析工具 - 用于计算和导出回测交易的年化收益率(XIRR)
"""
import sys
import os
import argparse
from datetime import datetime

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入XIRR计算器
from backtest_gui.utils.xirr_calculator import XIRRCalculator
from backtest_gui.utils.db_connector import DBConnector


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='计算回测交易的XIRR(年化收益率)')
    
    # 必选参数：回测ID
    parser.add_argument('backtest_id', type=int, help='回测ID')
    
    # 可选参数：输出Excel文件路径
    parser.add_argument('-o', '--output', type=str, help='输出Excel文件路径')
    
    # 可选参数：不导出Excel
    parser.add_argument('--no-excel', action='store_true', help='不导出Excel文件')
    
    # 可选参数：列出所有回测
    parser.add_argument('-l', '--list', action='store_true', help='列出所有回测')
    
    return parser.parse_args()


def list_backtests(db_connector):
    """列出所有回测"""
    try:
        conn = db_connector.get_connection()
        if not conn:
            print("无法获取数据库连接")
            return False
            
        cursor = conn.cursor()
        
        # 查询所有回测
        cursor.execute(
            """
            SELECT br.id, br.stock_code, br.start_date, br.end_date, 
                   br.initial_capital, br.final_capital, br.total_profit, br.total_profit_rate,
                   br.backtest_time, br.strategy_name, fi.fund_name
            FROM backtest_results br
            LEFT JOIN fund_info fi ON br.stock_code = fi.fund_code
            ORDER BY br.backtest_time DESC
            """
        )
        
        backtests = cursor.fetchall()
        
        # 释放数据库连接
        db_connector.release_connection(conn)
        
        if not backtests:
            print("没有找到回测记录")
            return False
            
        # 打印回测列表
        print("\n所有回测记录：")
        print("-" * 120)
        print(f"{'ID':<5} {'基金代码':<12} {'基金名称':<25} {'开始日期':<20} {'结束日期':<20} {'初始资金':<12} {'总收益率':<8} {'策略名称':<15} {'回测时间'}")
        print("-" * 120)
        
        for bt in backtests:
            backtest_id = bt[0]
            stock_code = bt[1]
            start_date = bt[2].strftime('%Y-%m-%d %H:%M:%S') if bt[2] else ''
            end_date = bt[3].strftime('%Y-%m-%d %H:%M:%S') if bt[3] else ''
            initial_capital = f"{bt[4]:,.2f}" if bt[4] else ''
            profit_rate = f"{bt[7]:.2f}%" if bt[7] is not None else ''
            strategy_name = bt[9] or ''
            backtest_time = bt[8].strftime('%Y-%m-%d %H:%M:%S') if bt[8] else ''
            fund_name = bt[10] or ''
            
            print(f"{backtest_id:<5} {stock_code:<12} {fund_name[:25]:<25} {start_date:<20} {end_date:<20} {initial_capital:<12} {profit_rate:<8} {strategy_name[:15]:<15} {backtest_time}")
            
        print("-" * 120)
        return True
        
    except Exception as e:
        print(f"列出回测异常: {str(e)}")
        return False


def main():
    """主函数"""
    # 解析命令行参数
    args = parse_arguments()
    
    # 创建数据库连接器
    db_connector = DBConnector()
    
    # 如果指定了列出所有回测，则显示回测列表
    if args.list:
        list_backtests(db_connector)
        return
        
    # 创建XIRR计算器
    calculator = XIRRCalculator(db_connector)
    
    # 计算XIRR
    result = calculator.calculate_backtest_xirr(args.backtest_id)
    
    if not result:
        print(f"无法计算回测ID={args.backtest_id}的XIRR")
        return
        
    # 显示基本信息
    print("\n回测基本信息：")
    print(f"回测ID：{result['backtest_info']['id']}")
    print(f"股票代码：{result['backtest_info']['stock_code']}")
    print(f"开始日期：{result['backtest_info']['start_date']}")
    print(f"结束日期：{result['backtest_info']['end_date']}")
    print(f"初始资金：{result['backtest_info']['initial_capital']:,.2f}")
    print(f"最终资金：{result['backtest_info']['final_capital']:,.2f}")
    print(f"总收益：{result['backtest_info']['total_profit']:,.2f}")
    print(f"总收益率：{result['backtest_info']['total_profit_rate']:.2f}%")
    
    # 显示XIRR结果
    if result['xirr'] is not None:
        print(f"\nXIRR(年化收益率)：{result['xirr']:.2f}%")
    else:
        print("\nXIRR(年化收益率)：无法计算")
        
    # 显示是否有未完成交易
    if result['has_incomplete_trades']:
        print("\n注意：存在未完成交易，XIRR计算结果包含当前持仓价值")
    
    # 导出到Excel
    if not args.no_excel:
        output_path = args.output
        calculator.export_to_excel(args.backtest_id, output_path)


if __name__ == "__main__":
    main() 