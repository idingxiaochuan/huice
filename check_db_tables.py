#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检查数据库表结构
"""

from backtest_gui.db.database import Database

def check_table_structure():
    """检查表结构"""
    # 连接数据库
    db = Database()
    if not db.connect():
        print("数据库连接失败")
        return
        
    try:
        # 获取数据库连接
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # 检查fund_strategy_bindings表结构
        cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'fund_strategy_bindings' 
        ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print("fund_strategy_bindings表结构:")
        for col in columns:
            print(f"{col[0]}: {col[1]}")
            
        # 检查stock_quotes表结构
        cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'stock_quotes' 
        ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print("\nstock_quotes表结构:")
        for col in columns:
            print(f"{col[0]}: {col[1]}")
            
        # 查看示例数据
        cursor.execute("""
        SELECT id, fund_code, strategy_id, is_default 
        FROM fund_strategy_bindings 
        LIMIT 5
        """)
        
        rows = cursor.fetchall()
        print("\nfund_strategy_bindings示例数据:")
        for row in rows:
            print(row)
            
        # 检查是否有绑定关系
        cursor.execute("""
        SELECT fs.fund_code, bs.name 
        FROM fund_strategy_bindings fs
        JOIN band_strategies bs ON fs.strategy_id = bs.id
        LIMIT 5
        """)
        
        rows = cursor.fetchall()
        print("\n基金与策略绑定关系:")
        for row in rows:
            print(f"基金: {row[0]}, 策略: {row[1]}")
            
    except Exception as e:
        print(f"查询失败: {str(e)}")
        import traceback
        traceback.print_exc()
        
if __name__ == "__main__":
    check_table_structure() 