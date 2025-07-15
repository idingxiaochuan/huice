#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
查看数据库表
"""
from backtest_gui.db.database import Database

def main():
    """主函数"""
    # 创建数据库连接
    db = Database()
    db.connect()
    
    # 获取连接
    conn = db.get_connection()
    
    try:
        # 创建游标
        cursor = conn.cursor()
        
        # 查询表名
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema='public'
        """)
        
        # 获取结果
        tables = cursor.fetchall()
        
        # 打印表名
        print("Available tables:")
        for table in tables:
            print(f"  - {table[0]}")
            
        # 查询特定表的结构
        for table_name in [t[0] for t in tables if 'stock' in t[0]]:
            print(f"\nStructure of {table_name}:")
            cursor.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
            """)
            columns = cursor.fetchall()
            for col in columns:
                print(f"  - {col[0]}: {col[1]}")
            
            # 查询表中的记录数
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"  Total records: {count}")
            
    finally:
        # 释放连接
        db.release_connection(conn)

if __name__ == "__main__":
    main() 