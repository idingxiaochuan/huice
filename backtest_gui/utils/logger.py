#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

# 设置日志目录
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

# 配置日志格式
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

def get_logger(name, log_level=logging.INFO):
    """
    获取指定名称的日志器
    
    Args:
        name: 日志器名称
        log_level: 日志级别，默认为INFO
        
    Returns:
        logger: 日志器实例
    """
    # 创建日志器
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # 如果已经有处理器，则不再添加
    if logger.handlers:
        return logger
        
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    
    # 创建文件处理器
    log_file = os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y%m%d')}.log")
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(log_level)
    
    # 设置日志格式
    formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # 添加处理器
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

# 全局日志器
app_logger = get_logger('backtest_app')

def log_info(msg):
    """记录信息日志"""
    app_logger.info(msg)
    
def log_error(msg, exc_info=False):
    """记录错误日志"""
    app_logger.error(msg, exc_info=exc_info)
    
def log_warning(msg):
    """记录警告日志"""
    app_logger.warning(msg)
    
def log_debug(msg):
    """记录调试日志"""
    app_logger.debug(msg) 