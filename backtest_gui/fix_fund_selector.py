#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
修复fund_selector.py文件中的缩进问题
"""

import os
import re

def fix_file():
    """修复fund_selector.py文件中的缩进问题"""
    file_path = 'gui/components/fund_selector.py'
    
    # 备份原文件
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 保存备份
        with open(file_path + '.bak', 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"已备份原文件到 {file_path}.bak")
        
        # 修复缩进问题
        pattern = r'(# 如果没有名称，尝试从funds表获取\s+if fund_name == display_code:)\s+try:'
        replacement = r'\1\n                    try:'
        
        content = re.sub(pattern, replacement, content)
        
        # 修复后续缩进
        pattern2 = r'(cursor\.execute\(name_query, \(display_code,\)\)\s+name_result = cursor\.fetchone\(\))\s+'
        replacement2 = r'\1\n                        '
        
        content = re.sub(pattern2, replacement2, content)
        
        # 保存修复后的文件
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"已修复 {file_path} 文件的缩进问题")
        return True
    else:
        print(f"未找到 {file_path} 文件")
        return False

if __name__ == "__main__":
    fix_file() 