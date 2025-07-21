#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
波段策略模块 - 实现网格交易策略
"""
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime
import traceback


class GridLevel:
    """网格级别配置"""
    
    def __init__(self, level, grid_type, buy_price, sell_price, buy_shares, sell_shares):
        """初始化网格级别
        
        Args:
            level: 级别编号
            grid_type: 网格类型 (NORMAL, SMALL, MEDIUM, LARGE)
            buy_price: 买入价格
            sell_price: 卖出价格
            buy_shares: 买入数量
            sell_shares: 卖出数量
        """
        self.level = level
        self.grid_type = grid_type
        self.buy_price = buy_price
        self.sell_price = sell_price
        self.buy_shares = buy_shares
        self.sell_shares = sell_shares
        
    def __str__(self):
        return (f"Level {self.level} ({self.grid_type}): "
                f"Buy: {self.buy_price} x {self.buy_shares}, "
                f"Sell: {self.sell_price} x {self.sell_shares}")


class BandStrategy:
    """波段交易策略"""
    
    def __init__(self, fund_code='515170', db_connector=None):
        """初始化波段策略
        
        Args:
            fund_code: 基金代码
            db_connector: 数据库连接器
        """
        self.fund_code = fund_code
        self.db_connector = db_connector
        self.grid_levels = []
        
        # 初始化买入和卖出状态字典
        self.buy_status = {}
        self.sell_status = {}
        
        # 跟踪配对交易信息
        self.paired_trades = {}  # 格式: {level: [{'buy_time', 'buy_price', 'buy_amount', 'buy_value', 'sell_time', 'sell_price', 'sell_amount', 'sell_value', 'status'}]}
        self.open_trades = {}    # 格式: {level: {'buy_time', 'buy_price', 'buy_amount', 'buy_value'}}
        
        # 删除价格变化检测相关变量
        # self.last_processed_price = None
        # self.last_signal_time = {}
        
        # 从数据库加载网格配置
        self.load_grid_config()
        
        # 初始化每个网格级别的状态
        for grid in self.grid_levels:
            self.buy_status[grid.level] = False
            self.sell_status[grid.level] = False
            self.paired_trades[grid.level] = []
            self.open_trades[grid.level] = None
            # self.last_signal_time[grid.level] = None
    
    def init_strategy(self):
        """初始化策略"""
        # 加载网格配置
        self.load_grid_config()
        
        # 初始化买卖状态
        self.buy_status = {}
        self.sell_status = {}
        for level in self.grid_levels:
            self.buy_status[level.level] = False
            self.sell_status[level.level] = False
            
        # 初始化交易记录
        self.open_trades = {}
        self.paired_trades = {}
        for level in self.grid_levels:
            self.open_trades[level.level] = None
            self.paired_trades[level.level] = []
        
        # 删除测试信号相关代码
        # self.add_test_signals = True
        # self.test_signal_interval = 50
        # self.data_point_count = 0
        # self.last_hour_signal_time = {}
        # self.last_date_signal = None
    
    def load_grid_config(self):
        """从数据库加载网格配置"""
        conn = None
        try:
            # 获取数据库连接
            if not self.db_connector:
                print(f"无法加载网格配置：数据库连接器未初始化")
                self.grid_levels = self._get_default_grid_config()
                return
                
            conn = self.db_connector.get_connection()
            if not conn:
                print(f"无法获取数据库连接，将使用默认网格配置")
                self.grid_levels = self._get_default_grid_config()
                return
                
            try:
                cursor = conn.cursor()
                
                # 查询基金绑定的策略
                cursor.execute("""
                    SELECT fs.strategy_id, bs.name
                    FROM fund_strategy_bindings fs
                    JOIN band_strategies bs ON fs.strategy_id = bs.id
                    WHERE fs.fund_code = %s
                    ORDER BY fs.is_default DESC
                    LIMIT 1
                """, (self.fund_code,))
                
                strategy = cursor.fetchone()
                
                if not strategy:
                    print(f"基金 {self.fund_code} 没有绑定的波段策略，将使用默认网格配置")
                    self.grid_levels = self._get_default_grid_config()
                    return
                    
                strategy_id = strategy[0]
                strategy_name = strategy[1]
                
                # 查询网格级别配置
                cursor.execute("""
                    SELECT level, grid_type, buy_price, sell_price, buy_shares, sell_shares
                    FROM grid_levels
                    WHERE strategy_id = %s
                    ORDER BY level
                """, (strategy_id,))
                
                grid_data = cursor.fetchall()
                
                if not grid_data:
                    print(f"策略 {strategy_id} ({strategy_name}) 没有网格级别配置，将使用默认网格配置")
                    self.grid_levels = self._get_default_grid_config()
                    return
                    
                # 创建网格级别对象
                self.grid_levels = []
                for level, grid_type, buy_price, sell_price, buy_shares, sell_shares in grid_data:
                    grid = GridLevel(
                        level=level,
                        grid_type=grid_type,
                        buy_price=float(buy_price),
                        sell_price=float(sell_price),
                        buy_shares=float(buy_shares),
                        sell_shares=float(sell_shares)
                    )
                    self.grid_levels.append(grid)
                    
                print(f"成功从数据库加载 {len(self.grid_levels)} 个网格级别配置，策略ID: {strategy_id}, 策略名称: {strategy_name}")
                
            except Exception as e:
                print(f"加载网格配置异常: {str(e)}")
                traceback.print_exc()
                self.grid_levels = self._get_default_grid_config()
            finally:
                if conn:
                    self.db_connector.release_connection(conn)
                    
        except Exception as e:
            print(f"加载网格配置异常: {str(e)}")
            traceback.print_exc()
            self.grid_levels = self._get_default_grid_config()
    
    def _get_default_grid_config(self):
        """获取默认网格配置"""
        print("使用内存中的默认网格配置")
        return [
            GridLevel(1, "NORMAL", 0.9800, 1.0000, 1000, 1000),
            GridLevel(2, "NORMAL", 0.9600, 0.9800, 1000, 1000),
            GridLevel(3, "NORMAL", 0.9400, 0.9600, 1000, 1000),
            GridLevel(4, "NORMAL", 0.9200, 0.9400, 1000, 1000),
            GridLevel(5, "NORMAL", 0.9000, 0.9200, 1000, 1000),
            GridLevel(6, "SMALL", 0.8800, 0.9000, 2000, 2000),
            GridLevel(7, "SMALL", 0.8600, 0.8800, 2000, 2000),
            GridLevel(8, "SMALL", 0.8400, 0.8600, 2000, 2000),
            GridLevel(9, "SMALL", 0.8200, 0.8400, 2000, 2000),
            GridLevel(10, "SMALL", 0.8000, 0.8200, 2000, 2000),
            GridLevel(11, "MEDIUM", 0.7800, 0.8000, 4000, 4000),
            GridLevel(12, "MEDIUM", 0.7600, 0.7800, 4000, 4000),
            GridLevel(13, "MEDIUM", 0.7400, 0.7600, 4000, 4000),
            GridLevel(14, "MEDIUM", 0.7200, 0.7400, 4000, 4000),
            GridLevel(15, "MEDIUM", 0.7000, 0.7200, 4000, 4000),
            GridLevel(16, "LARGE", 0.6800, 0.7000, 8000, 8000),
            GridLevel(17, "LARGE", 0.6600, 0.6800, 8000, 8000),
            GridLevel(18, "LARGE", 0.6400, 0.6600, 8000, 8000),
            GridLevel(19, "LARGE", 0.6200, 0.6400, 8000, 8000),
            GridLevel(20, "LARGE", 0.6000, 0.6200, 8000, 8000)
        ]
    
    def process_tick(self, time, price):
        """处理每个时间点的数据
        
        Args:
            time: 当前时间
            price: 当前价格
            
        Returns:
            list: 交易信号列表，每个信号为一个字典，包含 {type, price, amount} 等信息
        """
        signals = []
        checked_levels = set()
        
        # 删除测试信号生成代码
        # if hasattr(self, 'add_test_signals') and self.add_test_signals:
        #     self.data_point_count += 1
        #     if self.data_point_count % self.test_signal_interval == 0:
        #         # 交替添加买入和卖出信号
        #         if self.data_point_count % (self.test_signal_interval * 2) == 0:
        #             print(f"添加测试买入信号: 时间={time}, 价格={price}")
        #             signals.append({
        #                 'time': time,
        #                 'type': '买入',
        #                 'price': price,
        #                 'amount': 1000,
        #                 'level': 1,
        #                 'grid_type': 'TEST'
        #             })
        #         else:
        #             print(f"添加测试卖出信号: 时间={time}, 价格={price}")
        #             signals.append({
        #                 'time': time,
        #                 'type': '卖出',
        #                 'price': price,
        #                 'amount': 1000,
        #                 'level': 1,
        #                 'grid_type': 'TEST'
        #             })
        #             
        #     # 在特定日期添加额外的测试信号，但要控制频率
        #     try:
        #         if hasattr(time, 'strftime'):
        #             date_str = time.strftime('%Y-%m-%d')
        #             hour_str = time.strftime('%H')
        #             minute_str = time.strftime('%M')
        #             
        #             # 只在每小时的00分钟添加固定小时测试信号，避免每分钟都添加
        #             if hour_str in ['09', '10', '11', '13', '14'] and minute_str == '00':
        #                 # 检查该小时是否已经生成过信号
        #                 if hour_str not in self.last_hour_signal_time or self.last_hour_signal_time[hour_str] != date_str:
        #                     print(f"添加固定小时测试买入信号: 日期={date_str}, 小时={hour_str}, 价格={price}")
        #                     signals.append({
        #                         'time': time,
        #                         'type': '买入',
        #                         'price': price,
        #                         'amount': 2000,
        #                         'level': 2,
        #                         'grid_type': 'FIXED_HOUR'
        #                     })
        #                     # 记录该小时已生成信号
        #                     self.last_hour_signal_time[hour_str] = date_str
        #             
        #             # 在图表中间位置添加一些固定的测试信号，但每个日期只添加一次
        #             if date_str in ['2025-06-15', '2025-06-20', '2025-06-25', '2025-06-30', '2025-07-05']:
        #                 if self.last_date_signal != date_str:
        #                     print(f"添加固定日期测试买入信号: 日期={date_str}, 价格={price}")
        #                     signals.append({
        #                         'time': time,
        #                         'type': '买入',
        #                         'price': price,
        #                         'amount': 3000,
        #                         'level': 3,
        #                         'grid_type': 'FIXED_DATE'
        #                     })
        #                     self.last_date_signal = date_str
        #             elif date_str in ['2025-06-18', '2025-06-23', '2025-06-28', '2025-07-03', '2025-07-08']:
        #                 if self.last_date_signal != date_str:
        #                     print(f"添加固定日期测试卖出信号: 日期={date_str}, 价格={price}")
        #                     signals.append({
        #                         'time': time,
        #                         'type': '卖出',
        #                         'price': price,
        #                         'amount': 3000,
        #                         'level': 3,
        #                         'grid_type': 'FIXED_DATE'
        #                     })
        #                     self.last_date_signal = date_str
        #     except Exception as e:
        #         print(f"添加固定日期测试信号出错: {str(e)}")

        # 打印调试信息（仅在特定时间点，且降低频率）
        try:
            # 只有在成功转换为datetime对象且有second属性时执行
            # 减少输出频率，仅在小时变化时打印
            if (hasattr(time, 'second') and hasattr(time, 'minute') and 
                hasattr(time, 'hour') and time.second == 0 and time.minute == 0):
                
                # 每小时仅打印一次状态信息
                print(f"处理价格数据点: 时间={time}, 价格={price:.4f}")
                
                # 生成简化的状态信息
                status_info = []
                for level in self.grid_levels:
                    if self.buy_status[level.level]:
                        status = "已买入" if not self.sell_status[level.level] else "已卖出"
                        status_info.append(f"{level.level}:{status}")
                if status_info:
                    print("  状态: " + ", ".join(status_info))
        except:
            # 如果访问属性失败，跳过打印调试信息
            pass
        
        # 删除价格变化检测代码
        # if not price_changed and self.last_processed_price is not None:
        #     # 如果价格没有变化，只返回测试信号
        #     return signals
        
        # 存储已经检查过的级别，避免重复检查
        checked_levels = set()
        
        # 检查买入信号
        for level in self.grid_levels:
            # 跳过已检查的级别
            if level.level in checked_levels:
                continue
                
            checked_levels.add(level.level)
            
            # 如果价格低于或等于买入价且该级别尚未买入或已完成一个完整的买卖周期
            if price <= level.buy_price and (not self.buy_status[level.level] or (self.buy_status[level.level] and self.sell_status[level.level])):
                # 重置状态，开始新的买卖周期
                if self.buy_status[level.level] and self.sell_status[level.level]:
                    self.buy_status[level.level] = False
                    self.sell_status[level.level] = False
                
                self.buy_status[level.level] = True
                
                # 记录买入交易信息
                buy_amount = level.buy_shares
                buy_value = level.buy_price * buy_amount
                
                # 保存为未完成的配对交易
                self.open_trades[level.level] = {
                    'buy_time': time,
                    'buy_price': level.buy_price,
                    'buy_amount': buy_amount,
                    'buy_value': buy_value,
                    'grid_type': level.grid_type
                }
                
                # 打印买入信号生成 (减少频率)
                print(f"生成买入信号: 档位={level.level}({level.grid_type}), 价格={level.buy_price}")
                
                signals.append({
                    'time': time,
                    'type': '买入',
                    'price': level.buy_price,
                    'amount': level.buy_shares,
                    'level': level.level,
                    'grid_type': level.grid_type
                })
                
            # 如果价格高于或等于卖出价且该级别已经买入但尚未卖出
            elif price >= level.sell_price and self.buy_status[level.level] and not self.sell_status[level.level]:
                # 重置状态，开始新的买卖周期
                self.sell_status[level.level] = True
                
                # 记录卖出交易信息
                sell_amount = level.sell_shares
                sell_value = level.sell_price * sell_amount
                
                # 如果有未完成的配对交易，完成它
                if self.open_trades[level.level]:
                    buy_trade = self.open_trades[level.level]
                    
                    # 计算卖出部分的波段收益
                    band_profit = sell_value - (buy_trade['buy_value'] / buy_trade['buy_amount'] * sell_amount)
                    sell_band_profit_rate = (band_profit / (buy_trade['buy_value'] / buy_trade['buy_amount'] * sell_amount)) * 100 if sell_amount > 0 else 0
                    
                    # 计算剩余份额
                    remaining_shares = buy_trade['buy_amount'] - sell_amount
                    
                    # 创建完整的配对交易记录
                    paired_trade = {
                        'buy_time': buy_trade['buy_time'],
                        'buy_price': buy_trade['buy_price'],
                        'buy_amount': buy_trade['buy_amount'],
                        'buy_value': buy_trade['buy_value'],
                        'sell_time': time,
                        'sell_price': level.sell_price,
                        'sell_amount': sell_amount,
                        'sell_value': sell_value,
                        'remaining': remaining_shares,  # 修改：正确记录剩余份额
                        'remaining_shares': remaining_shares,  # 新字段：剩余份额
                        'band_profit': band_profit,
                        'band_profit_rate': 0,  # 旧字段，保持兼容
                        'sell_band_profit_rate': sell_band_profit_rate,  # 新字段：卖出部分收益率
                        'status': '已完成',
                        'grid_type': level.grid_type
                    }
                    
                    # 添加到配对交易列表
                    self.paired_trades[level.level].append(paired_trade)
                    
                    # 清除未完成交易
                    self.open_trades[level.level] = None
                
                # 打印卖出信号生成
                print(f"生成卖出信号: 档位={level.level}({level.grid_type}), 价格={level.sell_price}, 利润率={sell_band_profit_rate:.2f}%")
                
                signals.append({
                    'time': time,
                    'type': '卖出',
                    'price': level.sell_price,
                    'amount': level.sell_shares,
                    'level': level.level,
                    'grid_type': level.grid_type
                })
                
                # 卖出完成后立即重置状态，使下一次价格满足条件时可以再次买入
                self.buy_status[level.level] = False
                self.sell_status[level.level] = False
                
        return signals
    
    def get_all_paired_trades(self):
        """获取所有配对交易记录
        
        Returns:
            list: 所有配对交易记录列表
        """
        all_trades = []
        for level, trades in self.paired_trades.items():
            all_trades.extend(trades)
            
        # 添加未完成的交易
        for level, trade in self.open_trades.items():
            if trade:
                # 创建未完成的配对交易记录
                incomplete_trade = {
                    'buy_time': trade['buy_time'],
                    'buy_price': trade['buy_price'],
                    'buy_amount': trade['buy_amount'],
                    'buy_value': trade['buy_value'],
                    'sell_time': None,
                    'sell_price': None,
                    'sell_amount': None,
                    'sell_value': None,
                    'remaining': trade['buy_amount'],  # 旧字段，保持兼容
                    'remaining_shares': trade['buy_amount'],  # 新字段：剩余份额
                    'band_profit': None,
                    'band_profit_rate': None,
                    'sell_band_profit_rate': None,  # 新字段：卖出部分收益率
                    'status': '进行中',
                    'level': level,
                    'grid_type': trade['grid_type']
                }
                all_trades.append(incomplete_trade)
                
        return all_trades
        
    def save_paired_trades_to_db(self, backtest_id):
        """保存配对交易记录到数据库
        
        Args:
            backtest_id: 回测ID
            
        Returns:
            bool: 是否成功保存
        """
        if not self.db_connector:
            print("无法保存配对交易：数据库连接器未初始化")
            return False
            
        conn = None
        try:
            conn = self.db_connector.get_connection()
            if not conn:
                print("无法获取数据库连接")
                return False
                
            cursor = conn.cursor()
            
            # 获取所有配对交易
            all_trades = self.get_all_paired_trades()
            
            # 保存到数据库
            for trade in all_trades:
                # 准备数据
                level = trade.get('level', 0)
                grid_type = trade.get('grid_type', 'UNKNOWN')
                
                # 处理时间，确保转换为数据库接受的格式
                import pandas as pd
                import numpy as np
                
                # 转换买入时间
                buy_time = trade['buy_time']
                if isinstance(buy_time, (np.datetime64, pd.Timestamp)):
                    buy_time = pd.Timestamp(buy_time).to_pydatetime()
                
                # 转换卖出时间
                sell_time = trade['sell_time']
                if sell_time is not None and isinstance(sell_time, (np.datetime64, pd.Timestamp)):
                    sell_time = pd.Timestamp(sell_time).to_pydatetime()
                
                buy_price = trade['buy_price']
                buy_amount = trade['buy_amount']
                buy_value = trade['buy_value']
                sell_price = trade['sell_price']
                sell_amount = trade['sell_amount']
                sell_value = trade['sell_value']
                remaining = trade['remaining']
                remaining_shares = trade.get('remaining_shares', remaining)  # 使用新字段，如果没有则使用旧字段
                band_profit = trade['band_profit']
                band_profit_rate = trade['band_profit_rate']
                sell_band_profit_rate = trade.get('sell_band_profit_rate', band_profit_rate)  # 使用新字段，如果没有则使用旧字段
                status = trade['status']
                
                # 插入数据
                cursor.execute("""
                    INSERT INTO backtest_paired_trades 
                    (backtest_id, level, grid_type, buy_time, buy_price, buy_amount, buy_value,
                    sell_time, sell_price, sell_amount, sell_value, remaining, remaining_shares, 
                    band_profit, band_profit_rate, sell_band_profit_rate, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    backtest_id, level, grid_type, buy_time, buy_price, buy_amount, buy_value,
                    sell_time, sell_price, sell_amount, sell_value, remaining, remaining_shares,
                    band_profit, band_profit_rate, sell_band_profit_rate, status
                ))
            
            # 提交事务
            conn.commit()
            
            print(f"成功保存 {len(all_trades)} 条配对交易记录到数据库")
            return True
            
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"保存配对交易记录失败: {str(e)}")
            traceback.print_exc()
            return False
        finally:
            if conn:
                self.db_connector.release_connection(conn) 