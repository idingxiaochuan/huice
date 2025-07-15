#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
应用时间戳修复到数据库中已有的数据
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

def fix_stock_quotes_table():
    """修复stock_quotes表中的时间戳"""
    print("=== 开始修复stock_quotes表中的时间戳 ===")
    
    # 连接数据库
    db = Database()
    if not db.connect():
        print("连接数据库失败")
        return False
    
    try:
        # 查询所有日期在1970年附近的记录
        query = """
        SELECT id, fund_code, data_level, date, time
        FROM stock_quotes
        WHERE date < '2000-01-01'
        ORDER BY id
        """
        
        invalid_records = db.execute_query(query)
        
        if not invalid_records:
            print("没有找到需要修复的记录")
            return True
        
        print(f"找到 {len(invalid_records)} 条需要修复的记录")
        
        # 获取连接和游标
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # 修复每条记录
        fixed_count = 0
        error_count = 0
        
        for record in invalid_records:
            try:
                record_id = record['id']
                time_val = record['time']
                
                if time_val is None:
                    print(f"警告: ID={record_id} 的记录没有time值，无法修复")
                    error_count += 1
                    continue
                
                # 转换时间戳
                new_date = convert_timestamp_to_datetime(time_val)
                
                if new_date is None:
                    print(f"警告: ID={record_id} 的time值 {time_val} 无法转换为有效日期")
                    error_count += 1
                    continue
                
                # 更新记录
                update_sql = """
                UPDATE stock_quotes
                SET date = %s
                WHERE id = %s
                """
                
                cursor.execute(update_sql, (new_date, record_id))
                fixed_count += 1
                
                # 每100条记录提交一次
                if fixed_count % 100 == 0:
                    conn.commit()
                    print(f"已修复 {fixed_count} 条记录...")
                
            except Exception as e:
                print(f"修复记录 ID={record.get('id', 'unknown')} 失败: {str(e)}")
                error_count += 1
        
        # 提交剩余的更新
        conn.commit()
        
        print(f"修复完成: 成功修复 {fixed_count} 条记录，失败 {error_count} 条")
        
        # 验证修复结果
        verify_query = """
        SELECT COUNT(*) as count
        FROM stock_quotes
        WHERE date < '2000-01-01'
        """
        
        remaining = db.execute_query(verify_query)
        remaining_count = remaining[0]['count'] if remaining else 0
        
        print(f"验证结果: 还有 {remaining_count} 条记录日期在2000年之前")
        
        return fixed_count > 0
        
    except Exception as e:
        print(f"修复过程中发生错误: {str(e)}")
        traceback.print_exc()
        return False
    finally:
        if 'conn' in locals() and conn:
            db.release_connection(conn)

def fix_market_data_table():
    """修复market_data表中的时间戳"""
    print("\n=== 开始修复market_data表中的时间戳 ===")
    
    # 连接数据库
    db = Database()
    if not db.connect():
        print("连接数据库失败")
        return False
    
    try:
        # 查询所有日期在1970年附近的记录
        query = """
        SELECT id, symbol, date, time, freq
        FROM market_data
        WHERE date < '2000-01-01'
        ORDER BY id
        """
        
        invalid_records = db.execute_query(query)
        
        if not invalid_records:
            print("没有找到需要修复的记录")
            return True
        
        print(f"找到 {len(invalid_records)} 条需要修复的记录")
        
        # 获取连接和游标
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # 修复每条记录
        fixed_count = 0
        error_count = 0
        
        for record in invalid_records:
            try:
                record_id = record['id']
                time_val = record['time']
                
                if time_val is None:
                    print(f"警告: ID={record_id} 的记录没有time值，无法修复")
                    error_count += 1
                    continue
                
                # 转换时间戳
                new_date = convert_timestamp_to_datetime(time_val)
                
                if new_date is None:
                    print(f"警告: ID={record_id} 的time值 {time_val} 无法转换为有效日期")
                    error_count += 1
                    continue
                
                # 更新记录
                update_sql = """
                UPDATE market_data
                SET date = %s
                WHERE id = %s
                """
                
                cursor.execute(update_sql, (new_date, record_id))
                fixed_count += 1
                
                # 每100条记录提交一次
                if fixed_count % 100 == 0:
                    conn.commit()
                    print(f"已修复 {fixed_count} 条记录...")
                
            except Exception as e:
                print(f"修复记录 ID={record.get('id', 'unknown')} 失败: {str(e)}")
                error_count += 1
        
        # 提交剩余的更新
        conn.commit()
        
        print(f"修复完成: 成功修复 {fixed_count} 条记录，失败 {error_count} 条")
        
        # 验证修复结果
        verify_query = """
        SELECT COUNT(*) as count
        FROM market_data
        WHERE date < '2000-01-01'
        """
        
        remaining = db.execute_query(verify_query)
        remaining_count = remaining[0]['count'] if remaining else 0
        
        print(f"验证结果: 还有 {remaining_count} 条记录日期在2000年之前")
        
        return fixed_count > 0
        
    except Exception as e:
        print(f"修复过程中发生错误: {str(e)}")
        traceback.print_exc()
        return False
    finally:
        if 'conn' in locals() and conn:
            db.release_connection(conn)

def generate_fix_report():
    """生成修复报告"""
    print("\n=== 生成修复报告 ===")
    
    # 连接数据库
    db = Database()
    if not db.connect():
        print("连接数据库失败")
        return
    
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
        
        # 生成报告
        report = f"""
时间戳修复报告
======================
生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

stock_quotes表:
--------------
总记录数: {sq_stats[0]['total_count'] if sq_stats else '未知'}
无效日期记录数: {sq_stats[0]['invalid_count'] if sq_stats else '未知'}
最早日期: {sq_stats[0]['min_date'] if sq_stats else '未知'}
最晚日期: {sq_stats[0]['max_date'] if sq_stats else '未知'}

market_data表:
-------------
总记录数: {md_stats[0]['total_count'] if md_stats else '未知'}
无效日期记录数: {md_stats[0]['invalid_count'] if md_stats else '未知'}
最早日期: {md_stats[0]['min_date'] if md_stats else '未知'}
最晚日期: {md_stats[0]['max_date'] if md_stats else '未知'}

修复说明:
-------
1. 问题原因: QMT系统返回的是毫秒级时间戳(如1716998400000)，但之前的代码未正确处理，导致日期显示为1970年附近
2. 修复方法: 使用convert_timestamp_to_datetime函数正确处理毫秒级时间戳
3. 修复范围: 所有日期在2000年之前的记录
4. 修复结果: 请参考上述统计信息

预防措施:
-------
1. 已创建统一的时间戳转换函数，确保所有地方使用相同的转换方法
2. 增加了更详细的日志和错误处理，便于排查问题
3. 添加了数据验证步骤，确保转换后的日期在合理范围内
        """
        
        # 保存报告
        report_file = "timestamp_fix_report.txt"
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(report)
        
        print(f"修复报告已保存到 {report_file}")
        
    except Exception as e:
        print(f"生成修复报告失败: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    # 修复stock_quotes表
    fix_stock_quotes_table()
    
    # 修复market_data表
    fix_market_data_table()
    
    # 生成修复报告
    generate_fix_report() 