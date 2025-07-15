#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
修复fund_data_fetcher.py文件中的问题
"""

import os
import re

def fix_file():
    """修复fund_data_fetcher.py文件"""
    # 备份原文件
    if os.path.exists('fund_data_fetcher.py'):
        with open('fund_data_fetcher.py', 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 保存备份
        with open('fund_data_fetcher.py.backup', 'w', encoding='utf-8') as f:
            f.write(content)
        
        print("已备份原文件到 fund_data_fetcher.py.backup")
        
        # 修复问题1: system_time.sleep -> time.sleep
        content = content.replace('system_time.sleep', 'time.sleep')
        
        # 修复问题2: 从datetime导入datetime
        if 'from datetime import datetime' not in content:
            content = content.replace('import datetime', 'import datetime\nfrom datetime import datetime, timedelta')
        
        # 修复问题3: 修复缩进和语法错误
        # 查找可能有问题的代码块
        pattern = r'(self\._xtdata\.download_history_data.*?)\s+# 等待下载完成\s+time\.sleep\(1\)'
        fixed_code = '''                # 使用增量下载模式
                self._xtdata.download_history_data(self.symbol, period=period, incrementally=True)
                print(f"下载 {self.symbol} 的 {period} 数据完成")
            except Exception as e:
                print(f"下载历史数据失败: {str(e)}")
                traceback.print_exc()
            
            # 等待下载完成
            time.sleep(1)'''
        
        content = re.sub(pattern, fixed_code, content, flags=re.DOTALL)
        
        # 修复问题4: 确保try-except结构完整
        pattern2 = r'(# 获取市场数据.*?)try:'
        fixed_code2 = '''            # 获取市场数据
            fields = ['time', 'open', 'high', 'low', 'close', 'volume', 'amount']
            self.progress_signal.emit(50, 100, f"查询 {self.symbol} 的 {self.data_level} 数据...")
            
            try:'''
        
        content = re.sub(pattern2, fixed_code2, content, flags=re.DOTALL)
        
        # 保存修复后的文件
        with open('fund_data_fetcher.py', 'w', encoding='utf-8') as f:
            f.write(content)
        
        print("已修复fund_data_fetcher.py文件")
        return True
    else:
        print("未找到fund_data_fetcher.py文件")
        return False

if __name__ == "__main__":
    fix_file() 