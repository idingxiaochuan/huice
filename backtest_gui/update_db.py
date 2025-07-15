#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
更新数据库表结构
"""
import os
import sys
import traceback
import psycopg2

# 获取当前文件所在目录
current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(current_dir)

# 将项目根目录添加到Python路径
sys.path.insert(0, project_dir)

# 导入设置
try:
    import settings
    DB_HOST = settings.DB_HOST
    DB_PORT = settings.DB_PORT
    DB_NAME = settings.DB_NAME
    DB_USER = settings.DB_USER
    DB_PASSWORD = settings.DB_PASSWORD
except ImportError:
    print("警告: 无法导入settings.py，使用默认配置")
    # 默认数据库连接参数
    DB_HOST = '127.0.0.1'
    DB_PORT = 5432
    DB_NAME = 'huice'
    DB_USER = 'postgres'
    DB_PASSWORD = 'postgres'

# 获取utils目录中的db_schema.sql文件路径
sql_file = os.path.join(current_dir, "utils", "db_schema.sql")

def update_database_schema():
    """更新数据库表结构"""
    conn = None
    try:
        # 连接数据库
        print(f"正在连接数据库: {DB_HOST}:{DB_PORT}/{DB_NAME}")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        
        cursor = conn.cursor()
        
        # 读取SQL文件
        if os.path.exists(sql_file):
            print(f"读取SQL文件: {sql_file}")
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_script = f.read()
                
                # 执行SQL脚本
                print("执行SQL脚本...")
                cursor.execute(sql_script)
                
                # 提交事务
                conn.commit()
                print("数据库表结构已更新")
        else:
            print(f"错误: SQL文件不存在: {sql_file}")
            
    except Exception as e:
        print(f"更新数据库表结构时出错: {str(e)}")
        traceback.print_exc()
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("数据库连接已关闭")

if __name__ == "__main__":
    update_database_schema() 