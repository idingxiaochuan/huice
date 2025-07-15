#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检查数据库中的基金和数据级别
"""

from backtest_gui.db.database import Database

def check_fund_data():
    """检查基金数据"""
    # 连接数据库
    db = Database()
    if not db.connect():
        print("数据库连接失败")
        return
        
    try:
        # 获取数据库连接
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # 查询基金和数据级别
        cursor.execute("""
        SELECT DISTINCT fund_code, data_level 
        FROM stock_quotes 
        ORDER BY fund_code, data_level
        """)
        
        # 处理结果
        fund_data_levels = {}  # 用于存储每个基金的数据级别
        
        for row in cursor.fetchall():
            fund_code = row[0]
            data_level = row[1]
            
            # 记录基金的数据级别
            if fund_code not in fund_data_levels:
                fund_data_levels[fund_code] = []
            if data_level not in fund_data_levels[fund_code]:
                fund_data_levels[fund_code].append(data_level)
        
        # 打印结果
        print(f"数据库中共有 {len(fund_data_levels)} 个基金")
        for fund_code, data_levels in fund_data_levels.items():
            # 添加市场后缀
            if fund_code.startswith(('5', '6', '7', '0')):
                display_code = f"{fund_code}.SH"
            else:
                display_code = f"{fund_code}.SZ"
                
            print(f"基金: {display_code}, 数据级别: {data_levels}")
            
    except Exception as e:
        print(f"查询失败: {str(e)}")
        import traceback
        traceback.print_exc()
        
if __name__ == "__main__":
    check_fund_data() 