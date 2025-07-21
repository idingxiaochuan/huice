#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
修复数据库中的remaining字段，确保它反映每次卖出后留下的底仓份额
"""
import psycopg2
from backtest_gui.utils.db_connector import DBConnector

def fix_remaining_field():
    """修复数据库中的remaining字段"""
    try:
        # 获取数据库连接
        db_connector = DBConnector()
        conn = db_connector.get_connection()
        cursor = conn.cursor()
        
        # 查询所有有卖出记录的交易
        cursor.execute("""
            SELECT id, buy_amount, sell_amount
            FROM backtest_paired_trades
            WHERE sell_time IS NOT NULL
        """)
        
        trades = cursor.fetchall()
        print(f"找到 {len(trades)} 条有卖出记录的交易")
        
        # 统计信息
        updated_count = 0
        error_count = 0
        
        # 修复remaining字段
        for trade_id, buy_amount, sell_amount in trades:
            try:
                # 计算剩余份额
                if buy_amount is not None and sell_amount is not None:
                    remaining = buy_amount - sell_amount
                    
                    # 更新数据库
                    cursor.execute("""
                        UPDATE backtest_paired_trades
                        SET remaining = %s
                        WHERE id = %s
                    """, (remaining, trade_id))
                    
                    updated_count += 1
            except Exception as e:
                print(f"处理交易ID {trade_id} 时出错: {str(e)}")
                error_count += 1
        
        # 提交事务
        conn.commit()
        
        print(f"成功修复 {updated_count} 条交易记录的remaining字段")
        print(f"处理过程中出现 {error_count} 个错误")
        
        # 查询修复后的统计信息
        cursor.execute("SELECT COUNT(*) FROM backtest_paired_trades WHERE remaining > 0")
        remaining_count = cursor.fetchone()[0]
        print(f"修复后，有 {remaining_count} 条交易记录的remaining字段大于0")
        
        # 释放连接
        db_connector.release_connection(conn)
        
        return True
        
    except Exception as e:
        print(f"修复remaining字段时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def fix_specific_backtest(backtest_id):
    """修复特定回测ID的remaining字段"""
    try:
        # 获取数据库连接
        db_connector = DBConnector()
        conn = db_connector.get_connection()
        cursor = conn.cursor()
        
        # 查询该回测的所有有卖出记录的交易
        cursor.execute("""
            SELECT id, buy_amount, sell_amount
            FROM backtest_paired_trades
            WHERE backtest_id = %s AND sell_time IS NOT NULL
        """, (backtest_id,))
        
        trades = cursor.fetchall()
        print(f"找到回测ID {backtest_id} 的 {len(trades)} 条有卖出记录的交易")
        
        # 统计信息
        updated_count = 0
        error_count = 0
        
        # 修复remaining字段
        for trade_id, buy_amount, sell_amount in trades:
            try:
                # 计算剩余份额
                if buy_amount is not None and sell_amount is not None:
                    remaining = buy_amount - sell_amount
                    
                    # 更新数据库
                    cursor.execute("""
                        UPDATE backtest_paired_trades
                        SET remaining = %s
                        WHERE id = %s
                    """, (remaining, trade_id))
                    
                    updated_count += 1
            except Exception as e:
                print(f"处理交易ID {trade_id} 时出错: {str(e)}")
                error_count += 1
        
        # 提交事务
        conn.commit()
        
        print(f"成功修复回测ID {backtest_id} 的 {updated_count} 条交易记录的remaining字段")
        print(f"处理过程中出现 {error_count} 个错误")
        
        # 查询修复后的统计信息
        cursor.execute("""
            SELECT COUNT(*) FROM backtest_paired_trades 
            WHERE backtest_id = %s AND remaining > 0
        """, (backtest_id,))
        remaining_count = cursor.fetchone()[0]
        print(f"修复后，回测ID {backtest_id} 有 {remaining_count} 条交易记录的remaining字段大于0")
        
        # 释放连接
        db_connector.release_connection(conn)
        
        return True
        
    except Exception as e:
        print(f"修复remaining字段时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # 修复特定回测ID
        backtest_id = int(sys.argv[1])
        print(f"修复回测ID {backtest_id} 的remaining字段...")
        fix_specific_backtest(backtest_id)
    else:
        # 修复所有记录
        print("修复所有交易记录的remaining字段...")
        fix_remaining_field() 