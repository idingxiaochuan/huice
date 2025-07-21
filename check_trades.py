#!/usr/bin/env python
# -*- coding: utf-8 -*-

from backtest_gui.utils.db_connector import DBConnector

def analyze_trades(backtest_id):
    """分析指定回测ID的交易数据"""
    print(f"分析回测ID: {backtest_id} 的交易数据")
    
    # 获取数据库连接
    db = DBConnector()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    # 获取回测基本信息
    cursor.execute("""
        SELECT id, stock_code, start_date, end_date, initial_capital, final_capital, total_profit, total_profit_rate
        FROM backtest_results
        WHERE id = %s
    """, (backtest_id,))
    
    backtest_info = cursor.fetchone()
    if not backtest_info:
        print(f"未找到回测ID: {backtest_id}的信息")
        return
    
    # 转换为字典
    backtest_info_dict = {
        'id': backtest_info[0],
        'stock_code': backtest_info[1],
        'start_date': backtest_info[2],
        'end_date': backtest_info[3],
        'initial_capital': float(backtest_info[4]),
        'final_capital': float(backtest_info[5]),
        'total_profit': float(backtest_info[6]),
        'total_profit_rate': float(backtest_info[7])
    }
    
    print(f"回测基本信息: {backtest_info_dict}")
    
    # 获取交易记录总数
    cursor.execute("SELECT COUNT(*) FROM backtest_paired_trades WHERE backtest_id = %s", (backtest_id,))
    total_trades = cursor.fetchone()[0]
    print(f"总交易记录数: {total_trades}")
    
    # 获取买入和卖出总数
    cursor.execute("""
        SELECT 
            COUNT(*) as total_trades,
            SUM(CASE WHEN sell_time IS NOT NULL THEN 1 ELSE 0 END) as completed_trades,
            SUM(CASE WHEN sell_time IS NULL THEN 1 ELSE 0 END) as incomplete_trades
        FROM backtest_paired_trades 
        WHERE backtest_id = %s
    """, (backtest_id,))
    
    trade_counts = cursor.fetchone()
    print(f"总交易数: {trade_counts[0]}, 完成交易数: {trade_counts[1]}, 未完成交易数: {trade_counts[2]}")
    
    # 计算买入和卖出总金额
    cursor.execute("""
        SELECT 
            SUM(buy_amount * buy_price) as total_buy_value,
            SUM(CASE WHEN sell_time IS NOT NULL THEN sell_amount * sell_price ELSE 0 END) as total_sell_value
        FROM backtest_paired_trades 
        WHERE backtest_id = %s
    """, (backtest_id,))
    
    value_sums = cursor.fetchone()
    total_buy_value = float(value_sums[0])
    total_sell_value = float(value_sums[1])
    print(f"总买入金额: {total_buy_value:.2f}, 总卖出金额: {total_sell_value:.2f}, 差额: {total_sell_value - total_buy_value:.2f}")
    
    # 获取未完成交易详情
    cursor.execute("""
        SELECT id, buy_time, buy_price, buy_amount, buy_value, remaining
        FROM backtest_paired_trades 
        WHERE backtest_id = %s AND sell_time IS NULL
        ORDER BY buy_time
    """, (backtest_id,))
    
    incomplete_trades = cursor.fetchall()
    
    if incomplete_trades:
        print("\n未完成交易详情:")
        print("ID\t买入时间\t\t买入价格\t买入数量\t买入金额\t剩余数量")
        print("-" * 80)
        
        total_remaining_value = 0
        total_remaining_shares = 0
        
        for trade in incomplete_trades:
            trade_id = trade[0]
            buy_time = trade[1]
            buy_price = float(trade[2])
            buy_amount = int(trade[3])
            buy_value = float(trade[4])
            remaining = int(trade[5])
            
            print(f"{trade_id}\t{buy_time}\t{buy_price:.4f}\t{buy_amount}\t{buy_value:.2f}\t{remaining}")
            
            total_remaining_shares += remaining
            total_remaining_value += remaining * buy_price
        
        # 获取最后一次卖出价格
        cursor.execute("""
            SELECT sell_price FROM backtest_paired_trades 
            WHERE backtest_id = %s AND sell_price IS NOT NULL 
            ORDER BY sell_time DESC LIMIT 1
        """, (backtest_id,))
        
        last_price_result = cursor.fetchone()
        if last_price_result:
            last_price = float(last_price_result[0])
            print(f"\n最后一次卖出价格: {last_price:.4f}")
            
            # 计算未完成交易的估值
            estimated_value = total_remaining_shares * last_price
            print(f"未完成交易总股数: {total_remaining_shares}")
            print(f"未完成交易成本: {total_remaining_value:.2f}")
            print(f"未完成交易估值: {estimated_value:.2f}")
            print(f"未完成交易盈亏: {estimated_value - total_remaining_value:.2f}")
            
            # 计算总现金流
            total_cash_flow = total_sell_value + estimated_value - total_buy_value
            print(f"\n总现金流(包含未完成交易估值): {total_cash_flow:.2f}")
    
    # 计算已完成交易的盈亏
    cursor.execute("""
        SELECT 
            SUM((sell_amount * sell_price) - (sell_amount * buy_price)) as completed_profit
        FROM backtest_paired_trades 
        WHERE backtest_id = %s AND sell_time IS NOT NULL
    """, (backtest_id,))
    
    completed_profit = float(cursor.fetchone()[0])
    print(f"\n已完成交易总盈亏(卖出价值 - 买入成本): {completed_profit:.2f}")
    
    # 详细分析每笔已完成交易
    cursor.execute("""
        SELECT 
            id, buy_time, buy_price, buy_amount, buy_value, 
            sell_time, sell_price, sell_amount, sell_value, remaining
        FROM backtest_paired_trades 
        WHERE backtest_id = %s AND sell_time IS NOT NULL
        ORDER BY buy_time
        LIMIT 10  -- 只查看前10笔交易，避免输出过多
    """, (backtest_id,))
    
    completed_trades = cursor.fetchall()
    
    if completed_trades:
        print("\n已完成交易样本(前10笔):")
        print("ID\t买入时间\t\t买入价格\t买入数量\t买入金额\t卖出时间\t\t卖出价格\t卖出数量\t卖出金额\t剩余\t盈亏\t收益率(%)")
        print("-" * 120)
        
        for trade in completed_trades:
            trade_id = trade[0]
            buy_time = trade[1]
            buy_price = float(trade[2])
            buy_amount = int(trade[3])
            buy_value = float(trade[4])
            sell_time = trade[5]
            sell_price = float(trade[6])
            sell_amount = int(trade[7])
            sell_value = float(trade[8])
            remaining = int(trade[9])
            
            # 计算这笔交易的盈亏
            cost_basis = sell_amount * buy_price  # 卖出部分的买入成本
            profit = sell_value - cost_basis
            profit_rate = (profit / cost_basis) * 100 if cost_basis > 0 else 0
            
            print(f"{trade_id}\t{buy_time}\t{buy_price:.4f}\t{buy_amount}\t{buy_value:.2f}\t"
                  f"{sell_time}\t{sell_price:.4f}\t{sell_amount}\t{sell_value:.2f}\t{remaining}\t"
                  f"{profit:.2f}\t{profit_rate:.2f}")
    
    # 检查数据一致性
    print("\n数据一致性检查:")
    print(f"回测结果中的总盈亏: {backtest_info_dict['total_profit']:.2f}")
    print(f"交易记录计算的差额: {total_sell_value - total_buy_value:.2f}")
    print(f"加上未完成交易估值后的现金流: {total_cash_flow:.2f}")
    
    # 检查买入卖出股数是否平衡
    cursor.execute("""
        SELECT 
            SUM(buy_amount) as total_buy_shares,
            SUM(CASE WHEN sell_time IS NOT NULL THEN sell_amount ELSE 0 END) as total_sell_shares,
            SUM(remaining) as total_remaining_shares
        FROM backtest_paired_trades 
        WHERE backtest_id = %s
    """, (backtest_id,))
    
    shares = cursor.fetchone()
    total_buy_shares = int(shares[0])
    total_sell_shares = int(shares[1])
    total_remaining_shares_db = int(shares[2])
    
    print(f"\n股数平衡检查:")
    print(f"总买入股数: {total_buy_shares}")
    print(f"总卖出股数: {total_sell_shares}")
    print(f"剩余股数: {total_remaining_shares_db}")
    print(f"买入 - 卖出 = {total_buy_shares - total_sell_shares} (应等于剩余股数: {total_remaining_shares_db})")
    
    # 检查股数不平衡的原因
    shares_diff = total_buy_shares - total_sell_shares - total_remaining_shares_db
    if shares_diff != 0:
        print(f"\n股数不平衡，差额: {shares_diff}")
        
        # 检查是否有卖出数量大于买入数量的交易
        cursor.execute("""
            SELECT id, buy_time, buy_price, buy_amount, sell_time, sell_price, sell_amount, remaining
            FROM backtest_paired_trades 
            WHERE backtest_id = %s AND sell_amount > buy_amount
            ORDER BY buy_time
        """, (backtest_id,))
        
        invalid_trades = cursor.fetchall()
        if invalid_trades:
            print("\n发现卖出数量大于买入数量的交易:")
            print("ID\t买入时间\t\t买入价格\t买入数量\t卖出时间\t\t卖出价格\t卖出数量\t剩余")
            print("-" * 80)
            
            for trade in invalid_trades:
                trade_id = trade[0]
                buy_time = trade[1]
                buy_price = float(trade[2])
                buy_amount = int(trade[3])
                sell_time = trade[4]
                sell_price = float(trade[5])
                sell_amount = int(trade[6])
                remaining = int(trade[7])
                
                print(f"{trade_id}\t{buy_time}\t{buy_price:.4f}\t{buy_amount}\t{sell_time}\t{sell_price:.4f}\t{sell_amount}\t{remaining}")
        
        # 检查remaining字段是否正确
        cursor.execute("""
            SELECT id, buy_time, buy_amount, sell_amount, remaining,
                   (buy_amount - sell_amount) as calculated_remaining
            FROM backtest_paired_trades 
            WHERE backtest_id = %s AND remaining != (buy_amount - sell_amount)
            ORDER BY buy_time
        """, (backtest_id,))
        
        incorrect_remaining = cursor.fetchall()
        if incorrect_remaining:
            print("\n发现remaining字段不正确的交易:")
            print("ID\t买入时间\t\t买入数量\t卖出数量\t数据库剩余\t计算剩余\t差额")
            print("-" * 80)
            
            for trade in incorrect_remaining:
                trade_id = trade[0]
                buy_time = trade[1]
                buy_amount = int(trade[2])
                sell_amount = int(trade[3])
                remaining = int(trade[4])
                calculated_remaining = int(trade[5])
                
                print(f"{trade_id}\t{buy_time}\t{buy_amount}\t{sell_amount}\t{remaining}\t{calculated_remaining}\t{remaining - calculated_remaining}")
    
    # 检查是否有重复计算的交易
    cursor.execute("""
        SELECT buy_time, buy_price, buy_amount, COUNT(*) as count
        FROM backtest_paired_trades 
        WHERE backtest_id = %s
        GROUP BY buy_time, buy_price, buy_amount
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 10
    """, (backtest_id,))
    
    duplicate_trades = cursor.fetchall()
    if duplicate_trades:
        print("\n发现可能重复的交易:")
        print("买入时间\t\t买入价格\t买入数量\t重复次数")
        print("-" * 60)
        
        for trade in duplicate_trades:
            buy_time = trade[0]
            buy_price = float(trade[1])
            buy_amount = int(trade[2])
            count = int(trade[3])
            
            print(f"{buy_time}\t{buy_price:.4f}\t{buy_amount}\t{count}")
    
    # 关闭连接
    cursor.close()
    conn.close()

if __name__ == "__main__":
    analyze_trades(184) 