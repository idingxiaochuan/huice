#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
修复交易专用XIRR计算器的脚本
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

def test_trades_only_xirr_calculator():
    """测试修复后的交易专用XIRR计算器"""
    print("\n===== 测试交易专用XIRR计算器 =====")
    
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
        else:
            print("交易专用XIRR: 无法计算")
            
        # 输出现金流信息
        print("\n现金流信息:")
        for i, (date, amount) in enumerate(zip(result['cashflows']['date'], result['cashflows']['amount'])):
            print(f"{i+1}. {date} - {amount:,.2f}")
        
        # 尝试导出到Excel
        print("\n测试导出到Excel...")
        file_path = f"trades_only_xirr_test_{backtest_id}.xlsx"
        success = calculator.export_to_excel(backtest_id, file_path)
        
        if success:
            print(f"导出成功: {file_path}")
            return True
        else:
            print("导出失败")
            return False
            
    except Exception as e:
        print(f"测试失败: {str(e)}")
        traceback.print_exc()
        return False

def main():
    """主函数"""
    print("===== 开始修复交易专用XIRR计算器 =====")
    
    # 修复交易专用XIRR计算器
    if fix_trades_only_xirr_calculator():
        # 测试修复后的计算器
        test_trades_only_xirr_calculator()
    else:
        print("修复失败")

if __name__ == "__main__":
    main() 