import psycopg2
from backtest_gui.utils.db_connector import DBConnector

def check_trades_remaining(backtest_id):
    """检查指定回测ID的交易记录，特别是remaining字段"""
    try:
        # 获取数据库连接
        db_connector = DBConnector()
        conn = db_connector.get_connection()
        cursor = conn.cursor()
        
        # 获取所有配对交易记录
        cursor.execute("""
            SELECT id, level, grid_type, 
                   buy_time, buy_price, buy_amount, buy_value, 
                   sell_time, sell_price, sell_amount, sell_value,
                   remaining, band_profit, band_profit_rate, status
            FROM backtest_paired_trades 
            WHERE backtest_id = %s
            ORDER BY buy_time
        """, (backtest_id,))
        
        trades = cursor.fetchall()
        
        # 统计信息
        total_trades = len(trades)
        completed_trades = 0
        incomplete_trades = 0
        trades_with_remaining = 0
        total_remaining = 0
        
        print(f"回测ID: {backtest_id} 的交易记录分析")
        print(f"总交易数: {total_trades}")
        print("-" * 100)
        print("交易ID\t买入时间\t\t买入价格\t买入数量\t卖出时间\t\t卖出价格\t卖出数量\t剩余数量\t状态")
        print("-" * 100)
        
        # 分析每条交易记录
        for trade in trades:
            trade_id = trade[0]
            buy_time = trade[3]
            buy_price = float(trade[4])
            buy_amount = int(trade[5])
            sell_time = trade[7]
            sell_price = float(trade[8]) if trade[8] is not None else 0
            sell_amount = int(trade[9]) if trade[9] is not None else 0
            remaining = int(trade[11]) if trade[11] is not None else 0
            status = trade[14]
            
            # 统计信息
            if sell_time is not None:
                completed_trades += 1
                if remaining > 0:
                    trades_with_remaining += 1
                    total_remaining += remaining
            else:
                incomplete_trades += 1
                
            # 打印交易详情
            sell_price_str = f"{sell_price:.4f}" if sell_price > 0 else "0.0000"
            print(f"{trade_id}\t{buy_time}\t{buy_price:.4f}\t{buy_amount}\t"
                  f"{sell_time if sell_time else 'N/A'}\t"
                  f"{sell_price_str}\t"
                  f"{sell_amount}\t{remaining}\t{status}")
        
        # 打印统计信息
        print("-" * 100)
        print(f"已完成交易: {completed_trades}")
        print(f"未完成交易: {incomplete_trades}")
        print(f"有剩余份额的交易: {trades_with_remaining}")
        print(f"总剩余份额: {total_remaining}")
        
        # 检查是否有买入后未卖出的交易
        cursor.execute("""
            SELECT COUNT(*), SUM(buy_amount)
            FROM backtest_paired_trades 
            WHERE backtest_id = %s AND sell_time IS NULL
        """, (backtest_id,))
        
        unsold_result = cursor.fetchone()
        unsold_count = int(unsold_result[0]) if unsold_result[0] else 0
        unsold_shares = int(unsold_result[1]) if unsold_result[1] else 0
        
        print(f"买入未卖出的交易: {unsold_count}条, 共{unsold_shares}份")
        
        # 释放连接
        db_connector.release_connection(conn)
        
        return {
            'total_trades': total_trades,
            'completed_trades': completed_trades,
            'incomplete_trades': incomplete_trades,
            'trades_with_remaining': trades_with_remaining,
            'total_remaining': total_remaining,
            'unsold_count': unsold_count,
            'unsold_shares': unsold_shares
        }
        
    except Exception as e:
        print(f"检查交易记录时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    # 检查指定的回测ID
    backtest_id = 197  # 根据您的截图，这是我们要检查的回测ID
    result = check_trades_remaining(backtest_id) 