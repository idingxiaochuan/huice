#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
配置管理模块 - 用于管理系统配置
"""
import os
import yaml
import json

class Config:
    """配置管理类"""
    
    def __init__(self, config_file=None):
        """初始化配置管理器
        
        Args:
            config_file: 配置文件路径，如果不指定则使用默认路径
        """
        # 默认配置
        self._default_config = {
            'database': {
                'host': '127.0.0.1',
                'port': 5432,
                'dbname': 'huice',
                'user': 'postgres',
                'password': 'postgres'
            },
            'backtest': {
                'initial_capital': 100000.0,
                'batch_size': 100,
                'default_stock': '515170.SH'
            },
            'ui': {
                'chart_height': 600,
                'trade_panel_height': 200,
                'refresh_interval': 200
            }
        }
        
        # 当前配置
        self._config = self._default_config.copy()
        
        # 配置文件路径
        self._config_file = config_file or os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'config.yaml'
        )
        
        # 加载配置
        self.load_config()
        
    def load_config(self):
        """从文件加载配置
        
        Returns:
            bool: 是否成功加载
        """
        try:
            if os.path.exists(self._config_file):
                with open(self._config_file, 'r', encoding='utf-8') as f:
                    loaded_config = yaml.safe_load(f)
                    
                if loaded_config:
                    # 合并配置
                    self._update_config(self._config, loaded_config)
                return True
            else:
                # 配置文件不存在，创建默认配置文件
                self.save_config()
                return False
        except Exception as e:
            print(f"加载配置失败: {str(e)}")
            return False
            
    def save_config(self):
        """保存配置到文件
        
        Returns:
            bool: 是否成功保存
        """
        try:
            with open(self._config_file, 'w', encoding='utf-8') as f:
                yaml.dump(self._config, f, default_flow_style=False, allow_unicode=True)
            return True
        except Exception as e:
            print(f"保存配置失败: {str(e)}")
            return False
            
    def get(self, key, default=None):
        """获取配置项
        
        Args:
            key: 配置键，支持点号分隔的多级键，如 'database.host'
            default: 默认值，如果配置项不存在则返回此值
            
        Returns:
            配置项值
        """
        if '.' in key:
            parts = key.split('.')
            value = self._config
            for part in parts:
                if part in value:
                    value = value[part]
                else:
                    return default
            return value
        else:
            return self._config.get(key, default)
            
    def set(self, key, value):
        """设置配置项
        
        Args:
            key: 配置键，支持点号分隔的多级键，如 'database.host'
            value: 配置值
            
        Returns:
            bool: 是否成功设置
        """
        try:
            if '.' in key:
                parts = key.split('.')
                config = self._config
                for part in parts[:-1]:
                    if part not in config:
                        config[part] = {}
                    config = config[part]
                config[parts[-1]] = value
            else:
                self._config[key] = value
            return True
        except Exception:
            return False
            
    def reset_to_default(self):
        """重置为默认配置"""
        self._config = self._default_config.copy()
        self.save_config()
        
    def _update_config(self, target, source):
        """递归更新配置
        
        Args:
            target: 目标配置
            source: 源配置
        """
        for key, value in source.items():
            if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                self._update_config(target[key], value)
            else:
                target[key] = value
                
    @property
    def database_config(self):
        """获取数据库配置
        
        Returns:
            dict: 数据库配置字典
        """
        return self._config.get('database', {})
        
    @property
    def backtest_config(self):
        """获取回测配置
        
        Returns:
            dict: 回测配置字典
        """
        return self._config.get('backtest', {}) 