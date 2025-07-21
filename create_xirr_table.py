#!/usr/bin/env python
# -*- coding: utf-8 -*-

from backtest_gui.utils.db_connector import DBConnector

def create_xirr_table():
    """创建backtest_xirr表"""
    print("创建backtest_xirr表...")
    
    # 获取数据库连接
    db = DBConnector()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    try:
        # 检查表是否已存在
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'backtest_xirr'
            )
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            print("表backtest_xirr已存在，检查是否需要更新结构...")
            
            # 检查表结构
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'backtest_xirr'
                ORDER BY ordinal_position
            """)
            columns = [col[0] for col in cursor.fetchall()]
            print("当前表结构:", columns)
            
            # 检查是否有主键约束
            cursor.execute("""
                SELECT conname, pg_get_constraintdef(oid) 
                FROM pg_constraint 
                WHERE conrelid = 'backtest_xirr'::regclass
            """)
            constraints = cursor.fetchall()
            print("当前约束:", constraints)
            
            # 如果没有主键约束，添加一个
            has_primary_key = any('PRIMARY KEY' in constraint[1] for constraint in constraints)
            if not has_primary_key:
                print("添加主键约束...")
                cursor.execute("""
                    ALTER TABLE backtest_xirr 
                    ADD PRIMARY KEY (backtest_id)
                """)
                conn.commit()
            
            # 检查是否需要添加新列
            required_columns = [
                'backtest_id', 'xirr', 'total_buy_value', 'total_sell_value',
                'remaining_shares', 'remaining_value', 'total_cash_flow', 'calculation_time'
            ]
            
            for col in required_columns:
                if col not in columns:
                    print(f"添加列 {col}...")
                    if col == 'backtest_id':
                        cursor.execute(f"ALTER TABLE backtest_xirr ADD COLUMN {col} INTEGER NOT NULL")
                    elif col in ['xirr', 'total_buy_value', 'total_sell_value', 'remaining_value', 'total_cash_flow']:
                        cursor.execute(f"ALTER TABLE backtest_xirr ADD COLUMN {col} NUMERIC(15, 4)")
                    elif col == 'remaining_shares':
                        cursor.execute(f"ALTER TABLE backtest_xirr ADD COLUMN {col} INTEGER")
                    elif col == 'calculation_time':
                        cursor.execute(f"ALTER TABLE backtest_xirr ADD COLUMN {col} TIMESTAMP WITH TIME ZONE")
                    conn.commit()
            
            print("表结构更新完成")
            
        else:
            print("创建新表backtest_xirr...")
            cursor.execute("""
                CREATE TABLE backtest_xirr (
                    backtest_id INTEGER NOT NULL PRIMARY KEY,
                    xirr NUMERIC(15, 4),
                    total_buy_value NUMERIC(15, 4),
                    total_sell_value NUMERIC(15, 4),
                    remaining_shares INTEGER,
                    remaining_value NUMERIC(15, 4),
                    total_cash_flow NUMERIC(15, 4),
                    calculation_time TIMESTAMP WITH TIME ZONE
                )
            """)
            conn.commit()
            print("表创建成功")
        
    except Exception as e:
        conn.rollback()
        print(f"错误: {e}")
    finally:
        cursor.close()
        conn.close()
    
    print("操作完成")

if __name__ == "__main__":
    create_xirr_table() 