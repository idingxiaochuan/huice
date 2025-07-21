import psycopg2
from backtest_gui.utils.db_connector import DBConnector

def check_remaining_shares(backtest_id):
    """检查指定回测ID的剩余份额情况"""
    try:
        # 获取数据库连接
        db_connector = DBConnector()
        conn = db_connector.get_connection()
        cursor = conn.cursor()
        
        # 检查买入卖出后剩余的底仓
        cursor.execute("""
            SELECT COUNT(*), SUM(remaining)
            FROM backtest_paired_trades 
            WHERE backtest_id = %s AND remaining > 0 AND sell_time IS NOT NULL
        """, (backtest_id,))
        
        partially_sold_result = cursor.fetchone()
        partially_sold_count = int(partially_sold_result[0]) if partially_sold_result[0] else 0
        partially_sold_shares = int(partially_sold_result[1]) if partially_sold_result[1] else 0
        
        # 检查买入未卖出的底仓
        cursor.execute("""
            SELECT COUNT(*), SUM(buy_amount)
            FROM backtest_paired_trades 
            WHERE backtest_id = %s AND sell_time IS NULL
        """, (backtest_id,))
        
        unsold_result = cursor.fetchone()
        unsold_count = int(unsold_result[0]) if unsold_result[0] else 0
        unsold_shares = int(unsold_result[1]) if unsold_result[1] else 0
        
        # 释放连接
        db_connector.release_connection(conn)
        
        print(f"回测ID: {backtest_id}")
        print(f"买入卖出后剩余底仓: {partially_sold_count}条记录, 共{partially_sold_shares}份")
        print(f"买入未卖出底仓: {unsold_count}条记录, 共{unsold_shares}份")
        print(f"总剩余份额: {partially_sold_shares + unsold_shares}")
        
        return {
            'partially_sold': {
                'count': partially_sold_count,
                'shares': partially_sold_shares
            },
            'unsold': {
                'count': unsold_count,
                'shares': unsold_shares
            }
        }
        
    except Exception as e:
        print(f"检查剩余份额时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    # 检查最近的几个回测ID
    for backtest_id in [193, 194, 195]:
        result = check_remaining_shares(backtest_id)
        print("-" * 50) 