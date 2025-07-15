#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import platform
import traceback

def find_qmt_path():
    """查找QMT交易端安装路径
    
    Returns:
        str: QMT交易端安装路径，未找到则返回None
    """
    try:
        # 尝试常见安装路径
        possible_paths = []
        
        # 根据操作系统添加可能的路径
        if platform.system() == 'Windows':
            # 常见的QMT安装路径
            drives = ['C:', 'D:', 'E:', 'F:']
            
            # 添加国金QMT可能的安装路径
            for drive in drives:
                possible_paths.extend([
                    os.path.join(drive, '国金证券QMT交易端'),
                    os.path.join(drive, '国金QMT交易端'),
                    os.path.join(drive, '国金QMT交易端模拟'),
                    os.path.join(drive, 'Program Files', '国金证券QMT交易端'),
                    os.path.join(drive, 'Program Files (x86)', '国金证券QMT交易端'),
                ])
                
        # 检查已添加的路径
        for path in possible_paths:
            # 检查userdata_mini目录是否存在
            userdata_path = os.path.join(path, 'userdata_mini')
            if os.path.exists(userdata_path) and os.path.isdir(userdata_path):
                bin_path = os.path.join(path, 'bin.x64')
                if os.path.exists(bin_path):
                    print(f"找到有效的QMT路径: {bin_path}\\..\\userdata_mini")
                    return os.path.join(bin_path)
                
        # 如果找不到，尝试使用环境变量
        qmt_path = os.environ.get('QMT_PATH')
        if qmt_path and os.path.exists(qmt_path):
            return qmt_path
            
        # 如果还是找不到，尝试直接使用当前环境中的xtquant
        try:
            import xtquant
            return os.path.dirname(os.path.dirname(xtquant.__file__))
        except ImportError:
            pass
            
        # 如果以上方法都失败，返回None
        print("未找到QMT安装路径")
        return None
        
    except Exception as e:
        print(f"查找QMT路径异常: {str(e)}")
        traceback.print_exc()
        return None
        
if __name__ == "__main__":
    # 测试函数
    path = find_qmt_path()
    if path:
        print(f"QMT安装路径: {path}")
    else:
        print("未找到QMT安装路径") 