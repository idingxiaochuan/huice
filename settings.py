#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
系统配置模块
提供系统全局配置参数
"""

import os
import sys

# QMT客户端路径设置
# 尝试多个可能的路径
QMT_PATHS = [
    r"D:\国金QMT交易端模拟\bin.x64\..\userdata_mini",  # 模拟交易客户端路径
    r"D:\国金QMT交易端模拟\userdata_mini",  # 模拟交易客户端路径
    r"D:\国金证券QMT交易端\bin.x64\..\userdata_mini",  # 实盘交易客户端路径
    r"D:\国金证券QMT交易端\userdata_mini",  # 实盘交易客户端路径
]

# 交易账号信息
ACCOUNT_ID = "39264482"  # 默认账号，使用时应当从命令行参数或配置文件中读取
ACCOUNT_TYPE = "STOCK"  # 账户类型：STOCK-股票账户, CREDIT-信用账户, FUTURE-期货账户

# 数据库配置
DB_HOST = '127.0.0.1'  # 使用IP地址而不是主机名
DB_PORT = 5432
DB_NAME = 'huice'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'  # 修改为正确的密码

# 数据存储配置
DATA_DIR = "./data"  # CSV文件存储目录
SAVE_TO_DB = True    # 是否保存到数据库
SAVE_TO_CSV = True   # 是否保存到CSV文件

def find_qmt_path():
    """
    查找有效的QMT客户端路径
    :return: 有效的路径或None
    """
    for path in QMT_PATHS:
        if os.path.exists(path):
            print(f"找到有效的QMT路径: {path}")
            return path
    
    print("未找到有效的QMT路径，将使用默认路径")
    return QMT_PATHS[0]  # 使用默认路径

# 更新QMT路径
QMT_PATH = find_qmt_path() 