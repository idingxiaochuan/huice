#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
验证所有时间戳修复是否已正确应用
"""

import pandas as pd
import sys
import os
import traceback
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入数据库模块和时间工具
from backtest_gui.db.database import Database
from backtest_gui.utils.time_utils import convert_timestamp_to_datetime

def verify_database_dates():
    """验证数据库中的日期是否正确"""
    print("=== 验证数据库中的日期 ===")
    
    # 连接数据库
    db = Database()
    if not db.connect():
        print("连接数据库失败")
        return False
    
    try:
        # 查询stock_quotes表的统计信息
        sq_query = """
        SELECT 
            COUNT(*) as total_count,
            COUNT(CASE WHEN date < '2000-01-01' THEN 1 END) as invalid_count,
            MIN(date) as min_date,
            MAX(date) as max_date
        FROM stock_quotes
        """
        
        sq_stats = db.execute_query(sq_query)
        
        print("stock_quotes表:")
        print(f"总记录数: {sq_stats[0]['total_count'] if sq_stats else '未知'}")
        print(f"无效日期记录数: {sq_stats[0]['invalid_count'] if sq_stats else '未知'}")
        print(f"最早日期: {sq_stats[0]['min_date'] if sq_stats else '未知'}")
        print(f"最晚日期: {sq_stats[0]['max_date'] if sq_stats else '未知'}")
        
        # 查询market_data表的统计信息
        md_query = """
        SELECT 
            COUNT(*) as total_count,
            COUNT(CASE WHEN date < '2000-01-01' THEN 1 END) as invalid_count,
            MIN(date) as min_date,
            MAX(date) as max_date
        FROM market_data
        """
        
        md_stats = db.execute_query(md_query)
        
        print("\nmarket_data表:")
        print(f"总记录数: {md_stats[0]['total_count'] if md_stats else '未知'}")
        print(f"无效日期记录数: {md_stats[0]['invalid_count'] if md_stats else '未知'}")
        print(f"最早日期: {md_stats[0]['min_date'] if md_stats else '未知'}")
        print(f"最晚日期: {md_stats[0]['max_date'] if md_stats else '未知'}")
        
        # 验证结果
        invalid_count = 0
        if sq_stats and sq_stats[0]['invalid_count'] > 0:
            invalid_count += sq_stats[0]['invalid_count']
        if md_stats and md_stats[0]['invalid_count'] > 0:
            invalid_count += md_stats[0]['invalid_count']
        
        if invalid_count > 0:
            print(f"\n警告: 仍有 {invalid_count} 条记录的日期在2000年之前")
            return False
        else:
            print("\n验证通过: 所有日期都在2000年之后")
            return True
    
    except Exception as e:
        print(f"验证过程中发生错误: {str(e)}")
        traceback.print_exc()
        return False

def test_timestamp_conversion():
    """测试时间戳转换函数是否正确"""
    print("\n=== 测试时间戳转换函数 ===")
    
    # 创建测试数据
    test_cases = [
        # 毫秒级时间戳 (QMT返回的格式)
        1716998400000,  # 2024-05-29 16:00:00
        
        # 秒级时间戳
        1716998400,     # 2024-05-29 16:00:00
    ]
    
    # 测试每个时间戳
    all_correct = True
    for ts in test_cases:
        try:
            result = convert_timestamp_to_datetime(ts)
            expected = pd.to_datetime(ts / 1000, unit='s') if ts > 10000000000 else pd.to_datetime(ts, unit='s')
            is_correct = result == expected
            
            print(f"时间戳 {ts} -> {result}, 预期 {expected}, 正确: {is_correct}")
            
            if not is_correct:
                all_correct = False
                
        except Exception as e:
            print(f"时间戳 {ts} 转换失败: {str(e)}")
            all_correct = False
    
    if all_correct:
        print("时间戳转换函数测试通过")
    else:
        print("时间戳转换函数测试失败")
    
    return all_correct

def verify_code_files():
    """验证代码文件中是否使用了正确的时间戳转换方法"""
    print("\n=== 验证代码文件 ===")
    
    files_to_check = [
        "backtest_gui/fund_data_fetcher.py",
        "backtest_gui/minute_data_fetcher.py",
        "backtest_gui/db/database.py",
        "backtest_gui/utils/time_utils.py"
    ]
    
    all_correct = True
    for file_path in files_to_check:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 检查是否导入了convert_timestamp_to_datetime
            has_import = "from backtest_gui.utils.time_utils import convert_timestamp_to_datetime" in content or \
                         "import backtest_gui.utils.time_utils" in content or \
                         "from .utils.time_utils import convert_timestamp_to_datetime" in content
            
            # 检查是否使用了convert_timestamp_to_datetime函数
            uses_function = "convert_timestamp_to_datetime(" in content
            
            # 检查是否存在未修复的代码
            has_unfixed_code = "pd.to_datetime(df['time'])" in content or \
                              "pd.to_datetime(row['time'])" in content or \
                              "pd.to_datetime(time_val)" in content or \
                              "pd.to_datetime(timestamp)" in content
            
            # 特殊处理time_utils.py文件
            if file_path == "backtest_gui/utils/time_utils.py":
                # time_utils.py中可以使用pd.to_datetime，但需要确保正确处理毫秒级时间戳
                has_correct_conversion = "pd.to_datetime(timestamp / 1000, unit='s')" in content or \
                                        "pd.to_datetime(ts_num / 1000, unit='s')" in content
                
                # 重新判断是否有未修复代码
                has_unfixed_code = False
                
                print(f"文件: {file_path}")
                print(f"  - 包含正确的时间戳转换: {'是' if has_correct_conversion else '否'}")
                print(f"  - 使用转换函数: {'是' if uses_function else '否'}")
                
                if not has_correct_conversion:
                    all_correct = False
            else:
                # 其他文件应该导入并使用转换函数
                print(f"文件: {file_path}")
                print(f"  - 导入时间工具模块: {'是' if has_import else '否'}")
                print(f"  - 使用转换函数: {'是' if uses_function else '否'}")
                print(f"  - 存在未修复代码: {'是' if has_unfixed_code else '否'}")
                
                if not has_import or not uses_function or has_unfixed_code:
                    all_correct = False
        
        except Exception as e:
            print(f"检查文件 {file_path} 失败: {str(e)}")
            all_correct = False
    
    if all_correct:
        print("\n代码文件验证通过")
    else:
        print("\n代码文件验证失败，可能有未修复的代码")
    
    return all_correct

def main():
    """主函数"""
    print("=== 验证所有时间戳修复 ===\n")
    
    # 验证数据库中的日期
    db_verified = verify_database_dates()
    
    # 测试时间戳转换函数
    conversion_verified = test_timestamp_conversion()
    
    # 验证代码文件
    code_verified = verify_code_files()
    
    # 总结
    print("\n=== 验证结果总结 ===")
    print(f"1. 数据库日期验证: {'通过' if db_verified else '失败'}")
    print(f"2. 时间戳转换函数: {'通过' if conversion_verified else '失败'}")
    print(f"3. 代码文件验证: {'通过' if code_verified else '失败'}")
    
    if db_verified and conversion_verified and code_verified:
        print("\n所有验证都通过，时间戳问题已成功修复！")
    else:
        print("\n部分验证未通过，可能仍有问题需要解决。")

if __name__ == "__main__":
    main() 