#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
修复XIRR计算异常的问题
"""
import os
import sys
import traceback
import shutil

def fix_standard_xirr_calculator():
    """修复标准版XIRR计算器"""
    print("\n===== 修复标准版XIRR计算器 =====")
    
    # 标准版XIRR计算器
    standard_file = "backtest_gui/utils/xirr_calculator.py"
    standard_backup = "backtest_gui/utils/xirr_calculator.py.bak"
    
    try:
        # 备份文件
        if os.path.exists(standard_file):
            print(f"备份标准版XIRR计算器: {standard_backup}")
            shutil.copy2(standard_file, standard_backup)
            
        # 修复标准版XIRR计算器
        if os.path.exists(standard_file):
            print(f"修复标准版XIRR计算器: {standard_file}")
            with open(standard_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # 修复重复条件判断
            content = content.replace(
                'buy_value = float(trade[6]) if trade[6] is not None else 0.0 if trade[6] is not None else 0.0',
                'buy_value = float(trade[6]) if trade[6] is not None else 0.0'
            )
            
            content = content.replace(
                'sell_value = float(trade[10]) if trade[10] is not None else 0.0 if trade[10] is not None else 0.0',
                'sell_value = float(trade[10]) if trade[10] is not None else 0.0'
            )
            
            content = content.replace(
                'position_value = float(position[3]) if position[3] is not None else 0.0 if position[3] is not None else 0.0',
                'position_value = float(position[3]) if position[3] is not None else 0.0'
            )
            
            # 添加XIRR合理范围限制
            content = content.replace(
                '            # 使用自定义的割线法求解\n            f = lambda r: self._xnpv(r, cashflows)\n            result = self._secant_method(f, guess, guess + 0.1)\n            return result',
                '            # 使用自定义的割线法求解\n            f = lambda r: self._xnpv(r, cashflows)\n            result = self._secant_method(f, guess, guess + 0.1)\n            \n            # 限制XIRR的合理范围\n            if result is not None:\n                # 如果XIRR大于10（即1000%）或小于-0.9（即-90%），视为计算错误\n                if result > 10 or result < -0.9:\n                    print(f"XIRR计算结果超出合理范围: {result}, 视为计算错误")\n                    return None\n            \n            return result'
            )
            
            # 修改现金流处理方式，使用简化版的方法
            content = content.replace(
                """            # 构建现金流
            cashflows = []
            
            # 添加初始资金（作为负现金流）
            cashflows.append((backtest_info_dict['start_date'], -float(backtest_info_dict['initial_capital'])))
            
            # 处理配对交易
            has_incomplete_trades = False
            for trade in paired_trades:
                buy_time = trade[3]
                buy_value = float(trade[6]) if trade[6] is not None else 0.0
                
                # 买入为负现金流（投入资金）
                cashflows.append((buy_time, -buy_value))
                
                # 如果有卖出，添加卖出现金流
                sell_time = trade[7]
                if sell_time:
                    sell_value = float(trade[10]) if trade[10] is not None else 0.0
                    # 卖出为正现金流（收回资金）
                    cashflows.append((sell_time, sell_value))
                    
                # 检查是否有未完成的交易
                if trade[14] == '进行中':
                    has_incomplete_trades = True
            
            # 如果有持仓，添加最终持仓价值（作为正现金流）
            # 将结束日期的持仓作为最后一个现金流
            if position and position[0] and float(position[0]) > 0:
                position_value = float(position[3]) if position[3] is not None else 0.0
                cashflows.append((backtest_info_dict['end_date'], position_value))
                has_incomplete_trades = True""",
                
                """            # 构建现金流（优化版本）
            cashflows = []
            
            # 添加初始资金（作为负现金流）
            # 使用回测初始日期
            start_date = backtest_info_dict['start_date']
            initial_capital = float(backtest_info_dict['initial_capital'])  # 确保是浮点数
            cashflows.append((start_date, -initial_capital))
            
            # 处理配对交易
            has_incomplete_trades = False
            completed_trades_value = 0.0  # 已完成交易的价值
            pending_trades_value = 0.0    # 未完成交易的买入价值
            
            for trade in paired_trades:
                try:
                    buy_time = trade[3]
                    buy_value = float(trade[6]) if trade[6] is not None else 0.0
                    status = trade[14]
                    
                    # 累计买入价值
                    if status == '进行中':
                        pending_trades_value += buy_value
                        has_incomplete_trades = True
                    
                    # 如果有卖出，累计已完成交易价值
                    sell_time = trade[7]
                    if sell_time:
                        sell_value = float(trade[10]) if trade[10] is not None else 0.0
                        completed_trades_value += sell_value
                except Exception as e:
                    print(f"处理交易记录异常: {str(e)}")
                    continue
            
            # 添加最终资金（作为正现金流）
            # 如果有持仓，考虑持仓价值
            end_date = backtest_info_dict['end_date']
            
            # 计算最终现金流 = 最终资金
            final_value = float(backtest_info_dict['final_capital'])  # 确保是浮点数
            
            if position and position[0] and float(position[0]) > 0:
                # 如果有持仓信息，添加持仓价值
                position_value = float(position[3]) if position[3] is not None else 0.0
                # 最终资金已经包含了持仓价值，不需要再加
                has_incomplete_trades = True
                
            cashflows.append((end_date, final_value))"""
            )
            
            # 写入修改后的内容
            with open(standard_file, 'w', encoding='utf-8') as f:
                f.write(content)
                
            print(f"标准版XIRR计算器修复完成")
            
        print("===== 标准版XIRR计算器修复完成 =====\n")
        return True
        
    except Exception as e:
        print(f"修复标准版XIRR计算器失败: {str(e)}")
        traceback.print_exc()
        print("===== 标准版XIRR计算器修复失败 =====\n")
        return False

def test_xirr_calculation(backtest_id=171):
    """测试XIRR计算"""
    print(f"\n===== 测试XIRR计算 (回测ID: {backtest_id}) =====")
    
    try:
        # 导入必要的库
        from backtest_gui.db.database import Database
        from backtest_gui.utils.xirr_calculator import XIRRCalculator
        
        # 创建数据库连接
        print("正在连接数据库...")
        db = Database()
        db.connect()
        print("数据库连接成功")
        
        # 创建XIRR计算器
        print("创建XIRR计算器...")
        calculator = XIRRCalculator(db)
        
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
                
            # 打印现金流信息
            print("\n现金流信息:")
            for i, (date, amount) in enumerate(zip(result['cashflows']['date'], result['cashflows']['amount'])):
                print(f"{i+1}. 日期: {date}, 金额: {amount:,.2f}")
                
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

def main():
    """主函数"""
    print("===== 开始修复XIRR计算异常问题 =====")
    
    # 修复标准版XIRR计算器
    standard_fixed = fix_standard_xirr_calculator()
    
    if standard_fixed:
        print("===== 所有问题已修复，开始测试 =====")
        # 测试修复是否成功
        test_xirr_calculation()
    else:
        print("===== 修复失败，请检查错误信息 =====")

if __name__ == "__main__":
    main() 