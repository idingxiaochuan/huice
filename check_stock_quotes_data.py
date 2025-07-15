from backtest_gui.db.database import Database

# 连接数据库
db = Database()
if db.connect():
    print("数据库连接成功")
    
    try:
        # 获取stock_quotes表中的记录数
        query = "SELECT COUNT(*) FROM stock_quotes"
        count_result = db.execute_query(query)
        count = count_result[0][0] if count_result else 0
        print(f"\nstock_quotes表中共有 {count} 条记录")
        
        # 获取表中的示例数据
        query = "SELECT * FROM stock_quotes LIMIT 5"
        sample_data = db.execute_query(query)
        if sample_data:
            print("\n示例数据:")
            for row in sample_data:
                print(row)
        else:
            print("\n没有找到数据")
            
        # 查看不同fund_code的数据量
        query = "SELECT fund_code, data_level, COUNT(*) FROM stock_quotes GROUP BY fund_code, data_level ORDER BY fund_code, data_level"
        fund_counts = db.execute_query(query)
        if fund_counts:
            print("\n各基金数据量统计:")
            print("基金代码\t数据级别\t记录数")
            print("-" * 40)
            for row in fund_counts:
                print(f"{row[0]}\t{row[1]}\t{row[2]}")
    except Exception as e:
        print(f"查询出错: {str(e)}")
else:
    print("数据库连接失败") 