from backtest_gui.db.database import Database

# 连接数据库
db = Database()
if db.connect():
    print("数据库连接成功")
    
    try:
        # 检查stock_quotes表的约束
        query = """
        SELECT constraint_name, constraint_type 
        FROM information_schema.table_constraints 
        WHERE table_name = 'stock_quotes'
        """
        constraints = db.execute_query(query)
        
        print("\nstock_quotes表的约束:")
        print("约束名\t\t约束类型")
        print("-" * 40)
        for constraint in constraints:
            print(f"{constraint[0]}\t\t{constraint[1]}")
            
        # 检查唯一约束的列
        query = """
        SELECT tc.constraint_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_name = 'stock_quotes'
        AND tc.constraint_type = 'UNIQUE'
        ORDER BY tc.constraint_name, kcu.ordinal_position
        """
        unique_columns = db.execute_query(query)
        
        print("\n唯一约束的列:")
        print("约束名\t\t列名")
        print("-" * 40)
        for column in unique_columns:
            print(f"{column[0]}\t\t{column[1]}")
            
    except Exception as e:
        print(f"查询出错: {str(e)}")
else:
    print("数据库连接失败") 