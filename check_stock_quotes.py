from backtest_gui.db.database import Database

# 连接数据库
db = Database()
if db.connect():
    print("数据库连接成功")
    
    try:
        # 检查stock_quotes表是否存在
        query = "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'stock_quotes')"
        result = db.execute_query(query)
        table_exists = result[0][0] if result else False
        
        if table_exists:
            print("stock_quotes表存在")
            
            # 获取表结构
            query = "SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = 'stock_quotes' ORDER BY ordinal_position"
            columns = db.execute_query(query)
            
            print("\nstock_quotes表结构:")
            print("列名\t\t数据类型\t\t长度")
            print("-" * 60)
            for col in columns:
                col_name, data_type, max_length = col
                max_length_str = str(max_length) if max_length is not None else "N/A"
                print(f"{col_name}\t\t{data_type}\t\t{max_length_str}")
            
            # 获取表中的记录数
            query = "SELECT COUNT(*) FROM stock_quotes"
            count_result = db.execute_query(query)
            count = count_result[0][0] if count_result else 0
            print(f"\n表中共有 {count} 条记录")
            
            # 获取表中的示例数据
            query = "SELECT * FROM stock_quotes LIMIT 5"
            sample_data = db.execute_query(query)
            if sample_data:
                print("\n示例数据:")
                for row in sample_data:
                    print(row)
        else:
            print("stock_quotes表不存在")
            
            # 检查market_data表是否存在
            query = "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'market_data')"
            result = db.execute_query(query)
            market_data_exists = result[0][0] if result else False
            
            if market_data_exists:
                print("\nmarket_data表存在，将查看其结构:")
                
                # 获取表结构
                query = "SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = 'market_data' ORDER BY ordinal_position"
                columns = db.execute_query(query)
                
                print("\nmarket_data表结构:")
                print("列名\t\t数据类型\t\t长度")
                print("-" * 60)
                for col in columns:
                    col_name, data_type, max_length = col
                    max_length_str = str(max_length) if max_length is not None else "N/A"
                    print(f"{col_name}\t\t{data_type}\t\t{max_length_str}")
                
                # 获取表中的记录数
                query = "SELECT COUNT(*) FROM market_data"
                count_result = db.execute_query(query)
                count = count_result[0][0] if count_result else 0
                print(f"\n表中共有 {count} 条记录")
                
                # 获取表中的示例数据
                query = "SELECT * FROM market_data LIMIT 5"
                sample_data = db.execute_query(query)
                if sample_data:
                    print("\n示例数据:")
                    for row in sample_data:
                        print(row)
    except Exception as e:
        print(f"查询出错: {str(e)}")
else:
    print("数据库连接失败") 