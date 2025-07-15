#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
回测工作线程模块 - 用于在后台执行回测任务
"""
import traceback
import time
import numpy as np
import pandas as pd
import threading
from PyQt5.QtCore import QThread, pyqtSignal, QCoreApplication, QEventLoop, QTimer, Qt
from PyQt5.QtWidgets import QApplication

class BacktestWorker(QThread):
    """回测工作线程，用于在后台执行回测任务"""
    
    # 定义信号
    progress_signal = pyqtSignal(int, int, str)  # 进度信号(当前进度, 总进度, 描述)
    chart_update_signal = pyqtSignal(object, object, int, list, list)  # 图表更新信号(模块, 数据, 当前索引, 买入信号, 卖出信号)
    completed_signal = pyqtSignal(object, object, list, list, float, float, int)  # 完成信号(模块, 数据, 买入信号, 卖出信号, 总收益率, 总收益, 回测ID)
    error_signal = pyqtSignal(str)  # 错误信号
    status_signal = pyqtSignal(str)  # 状态信号，用于更新状态栏
    
    def __init__(self, module, stock_data, band_strategy, db_connector, 
                 pure_code, start_date, end_date, strategy_id, strategy_name):
        """初始化回测工作线程
        
        Args:
            module: 回测模块
            stock_data: 股票数据
            band_strategy: 波段策略对象
            db_connector: 数据库连接器
            pure_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            strategy_id: 策略ID
            strategy_name: 策略名称
        """
        super().__init__()
        self.module = module
        self.stock_data = stock_data  # 保存完整数据，用于第一批次加载
        self.stock_data_preview = stock_data.head(100) if stock_data is not None else pd.DataFrame()  # 只保留预览数据用于图表初始化
        self.data_length = len(stock_data) if stock_data is not None else 0
        self.band_strategy = band_strategy
        self.db_connector = db_connector
        self.pure_code = pure_code
        self.start_date = start_date
        self.end_date = end_date
        self.strategy_id = strategy_id
        self.strategy_name = strategy_name
        self.is_cancelled = False
        self.total_records = None  # 数据库中的总记录数
        self.last_date = None  # 上一批次的最后日期
        
        # 设置线程优先级为低，避免与UI线程竞争
        self.setPriority(QThread.LowPriority)
        
        # 保存查询参数，用于分批加载数据
        self.data_level = None
        if stock_data is not None:
            if 'data_level' in stock_data.attrs:
                self.data_level = stock_data.attrs['data_level']
                print(f"从stock_data.attrs获取数据级别: {self.data_level}")
            
        if self.data_level is None:
            # 尝试从文件名或其他属性中推断数据级别
            import pandas as pd
            if isinstance(stock_data, pd.DataFrame) and len(stock_data) > 0:
                # 检查股票数据的时间间隔来推断级别
                if 'date' in stock_data.columns and len(stock_data) > 1:
                    try:
                        times = pd.to_datetime(stock_data['date'])
                        time_diff = (times.iloc[1] - times.iloc[0]).total_seconds()
                        
                        # 根据时间差推断数据级别
                        if time_diff <= 60:
                            self.data_level = "1min"
                        elif time_diff <= 300:
                            self.data_level = "5min"
                        elif time_diff <= 3600:
                            self.data_level = "hour"
                        else:
                            self.data_level = "day"
                            
                        print(f"根据时间间隔推断数据级别: {self.data_level}")
                    except Exception as e:
                        print(f"推断数据级别失败: {str(e)}")
                        self.data_level = "1min"  # 默认使用1min
            
        # 如果仍然没有确定数据级别，使用默认值
        if self.data_level is None:
            self.data_level = "1min"  # 默认使用1min而非日线
            
        # 添加UI更新控制变量
        self.last_ui_update_time = 0
        self.ui_update_interval = 0.5  # 最多每0.5秒更新一次UI
        self.max_chart_updates = 20  # 整个回测过程中最多更新图表的次数
        
        # 线程状态标志
        self.is_running = False
        
        # 查询数据库中的总记录数
        self._get_total_records()
        
        print(f"初始化回测工作线程: 预加载数据长度={self.data_length}, 数据库总记录数={self.total_records}, 数据级别={self.data_level}")
    
    def cancel(self):
        """取消回测"""
        self.is_cancelled = True
    
    def is_backtest_running(self):
        """检查回测是否正在运行
        
        Returns:
            bool: 如果回测正在运行则返回True，否则返回False
        """
        return self.is_running and self.isRunning()
        
    def process_events(self):
        """处理所有待处理的事件，确保UI响应
        
        此方法会将控制权暂时交还给主事件循环，使UI保持响应
        """
        # 使用静态方法处理事件，避免创建新的事件循环
        QApplication.processEvents()
        
        # 每次处理事件后，短暂暂停一下，给其他线程和系统处理的机会
        self.msleep(1)
        
    def _get_total_records(self):
        """查询数据库中符合条件的总记录数"""
        try:
            if not self.db_connector:
                print("无法获取总记录数：数据库连接器未初始化")
                return
                
            conn = self.db_connector.get_connection()
            if not conn:
                print("无法获取数据库连接")
                return
                
            try:
                cursor = conn.cursor()
                
                # 打印查询参数
                print(f"查询总记录数: fund_code={self.pure_code}, 日期={self.start_date}至{self.end_date}, 数据级别={self.data_level}")
                
                # 查询总记录数
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM stock_quotes
                    WHERE fund_code = %s AND date BETWEEN %s AND %s AND data_level = %s
                """, (self.pure_code, self.start_date, self.end_date, self.data_level))
                
                # 获取结果
                result = cursor.fetchone()
                if result and result[0]:
                    self.total_records = result[0]
                    print(f"数据库中共有 {self.total_records} 条符合条件的记录")
                else:
                    print("未能获取总记录数")
                    self.total_records = self.data_length
            finally:
                self.db_connector.release_connection(conn)
                    
        except Exception as e:
            print(f"获取总记录数失败: {str(e)}")
            traceback.print_exc()
            self.total_records = self.data_length

    def run(self):
        """执行回测任务"""
        try:
            # 导入必要的库
            import pandas as pd
            import numpy as np
            
            # 设置运行标志
            self.is_running = True
            self.status_signal.emit("正在后台执行回测...")
            
            if self.data_length == 0:
                self.error_signal.emit("股票数据为空")
                self.is_running = False
                return
                
            # 初始化交易信号列表
            buy_signals = []
            sell_signals = []
            
            # 初始化资金和持仓
            initial_capital = 1000000  # 初始资金100万
            current_capital = initial_capital
            position = 0  # 当前持仓数量
            
            # 打印数据级别信息
            print(f"回测数据级别: {self.data_level}")
            
            # 检查是否取消回测
            if self.is_cancelled:
                print("回测已取消")
                self.is_running = False
                return
            
            # 显示进度对话框
            self.progress_signal.emit(0, 100, "正在准备数据...")
            
            # 简化版：直接使用预加载的数据或一次性加载所有数据
            if self.stock_data is not None and len(self.stock_data) > 0:
                print(f"使用预加载的数据进行回测，共 {len(self.stock_data)} 条记录")
                data = self.stock_data
            else:
                # 如果没有预加载数据，尝试一次性加载
                print("尝试从数据库一次性加载所有数据")
                data = self._load_all_data()
                
                if data is None or len(data) == 0:
                    self.error_signal.emit("无法加载回测数据")
                    self.is_running = False
                    return
                    
            print(f"开始处理 {len(data)} 条数据")
            
            # 记录数据开始和结束时间
            start_time = data['date'].min() if 'date' in data.columns else None
            end_time = data['date'].max() if 'date' in data.columns else None
            print(f"数据时间范围: {start_time} 至 {end_time}")
            
            # 初始化波段策略
            if self.band_strategy:
                self.band_strategy.init_strategy()
            
            # 记录第一个价格点，用于计算相对收益
            first_price = data.iloc[0]['close'] if len(data) > 0 and 'close' in data.columns else None
            
            # 处理所有价格数据
            total_data_points = len(data)
            all_data_points = []  # 用于存储所有处理过的数据点
            
            # 记录开始时间
            start_process_time = time.time()
            
            # 一次性处理所有数据，但每处理一定数量后更新进度
            progress_step = max(1, total_data_points // 100)  # 每1%更新一次进度
            last_progress_update = 0
            
            for i, row in enumerate(data.itertuples()):
                # 检查是否已取消
                if self.is_cancelled:
                    print("回测已取消")
                    break
                
                # 每隔一定数量更新进度
                if i % progress_step == 0 or i == total_data_points - 1:
                    # 计算进度百分比
                    progress_pct = min(100, int((i + 1) / total_data_points * 100))
                    
                    # 计算预计剩余时间
                    elapsed_time = time.time() - start_process_time
                    if i > 0:
                        estimated_total_time = elapsed_time * total_data_points / i
                        estimated_remaining_time = estimated_total_time - elapsed_time
                        time_str = f", 预计剩余时间: {estimated_remaining_time:.1f}秒"
                    else:
                        time_str = ""
                    
                    # 发送进度信号
                    self.progress_signal.emit(
                        i + 1, 
                        total_data_points, 
                        f"已处理 {i + 1}/{total_data_points} 条数据 ({progress_pct}%){time_str}"
                    )
                    
                    # 处理事件，保持UI响应
                    if (i - last_progress_update) >= 10000:  # 每处理1万条数据处理一次事件
                        self.process_events()
                        last_progress_update = i
                
                # 获取当前行的数据
                try:
                    # 确定日期和价格列
                    if hasattr(row, 'date'):
                        current_time = row.date
                    elif hasattr(row, 'time'):
                        current_time = row.time
                    else:
                        # 如果没有日期列，创建一个假的日期
                        current_time = pd.Timestamp.now()
                    
                    if hasattr(row, 'close'):
                        current_price = row.close
                    else:
                        # 如果没有价格列，使用1.0作为默认价格
                        current_price = 1.0
                    
                    # 存储处理的数据点
                    data_point = {}
                    for column in data.columns:
                        if hasattr(row, column):
                            data_point[column] = getattr(row, column)
                    
                    all_data_points.append(data_point)
                    
                    # 确保日期时间格式正确
                    try:
                        # 如果是字符串，转换为datetime
                        if isinstance(current_time, str):
                            current_time = pd.to_datetime(current_time)
                    except Exception as e:
                        print(f"处理时间格式出错: {str(e)}, 类型: {type(current_time)}")
                    
                    # 应用波段策略
                    signals = self.band_strategy.process_tick(current_time, current_price)
                    
                    # 收集买入卖出信号
                    for signal in signals:
                        if signal['type'] == '买入':
                            # 确保时间格式正确
                            signal_time = signal['time']
                            if isinstance(signal_time, str):
                                try:
                                    signal_time = pd.to_datetime(signal_time)
                                except:
                                    pass
                                    
                            buy_signals.append({
                                'time': signal_time,
                                'price': signal['price'],
                                'amount': signal['amount'],
                                'level': signal['level'],
                                'grid_type': signal.get('grid_type', 'UNKNOWN')  # 添加grid_type信息
                            })
                        elif signal['type'] == '卖出':
                            # 确保时间格式正确
                            signal_time = signal['time']
                            if isinstance(signal_time, str):
                                try:
                                    signal_time = pd.to_datetime(signal_time)
                                except:
                                    pass
                                    
                            sell_signals.append({
                                'time': signal_time,
                                'price': signal['price'],
                                'amount': signal['amount'],
                                'level': signal['level'],
                                'grid_type': signal.get('grid_type', 'UNKNOWN')  # 添加grid_type信息
                            })
                        
                except Exception as e:
                    print(f"处理数据点出错: {str(e)}")
                    continue
            
            # 计算总处理时间
            total_time = time.time() - start_process_time
            print(f"数据处理完成，总耗时: {total_time:.2f}秒，平均每条数据 {total_time/total_data_points*1000:.3f}毫秒")
            
            # 所有数据处理完毕后，进行最终图表更新
            if not self.is_cancelled:
                # 更新状态栏，指示即将绘制图表
                self.status_signal.emit(f"回测计算完成，正在绘制图表...")
                
                # 最终更新图表，确保显示完整数据
                self._update_chart(all_data_points, buy_signals, sell_signals, first_price, total_data_points, final_update=True)
                
                # 计算回测结果
                initial_capital = 1000000.0  # 初始资金100万
                final_capital = initial_capital
                total_profit = 0.0
                total_profit_rate = 0.0
                
                # 计算总收益
                if buy_signals and sell_signals:
                    total_buy_value = sum(signal['price'] * signal['amount'] for signal in buy_signals)
                    total_sell_value = sum(signal['price'] * signal['amount'] for signal in sell_signals)
                    total_profit = total_sell_value - total_buy_value
                    if total_buy_value > 0:
                        total_profit_rate = (total_profit / total_buy_value) * 100
                    final_capital = initial_capital + total_profit
                
                # 打印处理结果
                print(f"回测完成: 总共处理 {total_data_points}/{total_data_points} 条数据")
                print(f"生成买入信号: {len(buy_signals)} 个, 卖出信号: {len(sell_signals)} 个")
                print(f"总收益: {total_profit:.2f}, 收益率: {total_profit_rate:.2f}%")
                
                # 保存回测结果到数据库
                backtest_id = self._save_backtest_results(
                    self.pure_code, 
                    self.start_date, 
                    self.end_date, 
                    initial_capital, 
                    final_capital, 
                    total_profit, 
                    total_profit_rate, 
                    self.strategy_id, 
                    self.strategy_name,
                    len(buy_signals),
                    len(sell_signals)
                )
                
                # 如果成功保存了回测结果，保存配对交易记录
                if backtest_id:
                    self.band_strategy.save_paired_trades_to_db(backtest_id)
                    
                    # 创建一个包含所有价格点的DataFrame作为最终结果
                    if all_data_points:
                        final_data = pd.DataFrame(all_data_points)
                        print(f"准备发送完成信号: 数据点数量={len(final_data)}, 买入信号={len(buy_signals)}, 卖出信号={len(sell_signals)}")
                    else:
                        final_data = pd.DataFrame()
                        print("警告: 没有数据点可用于最终图表")
                    
                    # 确保数据不为空且包含必要的列
                    if not final_data.empty:
                        # 确保数据按日期排序
                        if 'date' in final_data.columns:
                            final_data = final_data.sort_values('date')
                            
                        # 打印数据样本以便调试
                        print(f"最终数据样本: {final_data.head(3)}")
                        print(f"数据列: {final_data.columns.tolist()}")
                    
                    # 发出完成信号
                    self.completed_signal.emit(
                        self.module,
                        final_data,
                        buy_signals,
                        sell_signals,
                        total_profit_rate,
                        total_profit,
                        backtest_id
                    )
                    
                    # 更新状态栏
                    self.status_signal.emit(f"回测完成: 买入信号 {len(buy_signals)} 个，卖出信号 {len(sell_signals)} 个，收益率 {total_profit_rate:.2f}%")
                    
                    # 确保主线程有机会处理信号
                    self.process_events()
                else:
                    self.error_signal.emit("保存回测结果失败")
                    
            # 重置运行标志
            self.is_running = False
            print("回测工作线程执行完毕")

        except Exception as e:
            print(f"回测执行错误: {str(e)}")
            traceback.print_exc()
            self.error_signal.emit(f"回测执行错误: {str(e)}")
        finally:
            # 回测完成，重置运行标志
            self.is_running = False
            self.status_signal.emit("后台回测已完成")
    
    def _update_chart(self, all_data_points, buy_signals, sell_signals, first_price, processed_count, final_update=False):
        """更新图表显示
        
        Args:
            all_data_points: 所有数据点列表
            buy_signals: 买入信号列表
            sell_signals: 卖出信号列表
            first_price: 第一个价格点
            processed_count: 已处理的数据点数量
            final_update: 是否为最终更新
        """
        try:
            # 如果是频繁更新且间隔太短，则跳过
            current_time = time.time()
            if not final_update and current_time - self.last_ui_update_time < self.ui_update_interval:
                return
                
            self.last_ui_update_time = current_time
            
            # 创建临时数据帧，包含所有已处理的数据点
            temp_df = pd.DataFrame(all_data_points)
            if temp_df.empty:
                return
                
            # 进行数据采样，减少内存占用和处理开销
            original_length = len(temp_df)
            sample_df = temp_df
            
            # 根据数据量进行不同程度的采样
            if original_length > 100000:
                # 对于超大数据量，只保留关键点
                print(f"数据量过大 ({original_length}条)，进行1/100采样用于图表更新")
                sample_df = temp_df.iloc[::100].copy()
            elif original_length > 50000:
                print(f"数据量较大 ({original_length}条)，进行1/50采样用于图表更新")
                sample_df = temp_df.iloc[::50].copy()
            elif original_length > 10000:
                print(f"数据量中等 ({original_length}条)，进行1/10采样用于图表更新")
                sample_df = temp_df.iloc[::10].copy()
                
            # 确保按日期排序
            sample_df = sample_df.sort_values('date')
            
            # 归一化价格数据，使第一个点为1.0
            if first_price is not None and first_price > 0:
                sample_df['normalized_close'] = sample_df['close'] / first_price
            
            # 增强买卖信号，添加更多信息以便调试
            enhanced_buy_signals = []
            for signal in buy_signals:
                enhanced_signal = signal.copy()
                # 确保信号有所有需要的属性
                if 'time' not in enhanced_signal:
                    print(f"警告: 买入信号缺少time属性")
                if 'price' not in enhanced_signal:
                    print(f"警告: 买入信号缺少price属性")
                if 'level' not in enhanced_signal:
                    print(f"警告: 买入信号缺少level属性")
                # 添加调试信息
                enhanced_signal['debug_info'] = f"买入点: 时间={enhanced_signal.get('time', 'unknown')}, 价格={enhanced_signal.get('price', 0.0)}"
                enhanced_buy_signals.append(enhanced_signal)
                
            enhanced_sell_signals = []
            for signal in sell_signals:
                enhanced_signal = signal.copy()
                # 确保信号有所有需要的属性
                if 'time' not in enhanced_signal:
                    print(f"警告: 卖出信号缺少time属性")
                if 'price' not in enhanced_signal:
                    print(f"警告: 卖出信号缺少price属性")
                if 'level' not in enhanced_signal:
                    print(f"警告: 卖出信号缺少level属性")
                # 添加调试信息
                enhanced_signal['debug_info'] = f"卖出点: 时间={enhanced_signal.get('time', 'unknown')}, 价格={enhanced_signal.get('price', 0.0)}"
                enhanced_sell_signals.append(enhanced_signal)
            
            # 在最终更新时，打印信号信息以便调试
            if final_update:
                print(f"最终更新图表: 共 {len(sample_df)} 条数据 (原始:{original_length}条), {len(enhanced_buy_signals)} 个买入信号, {len(enhanced_sell_signals)} 个卖出信号")
                if enhanced_buy_signals:
                    print(f"买入信号示例: {enhanced_buy_signals[0]}")
                if enhanced_sell_signals:
                    print(f"卖出信号示例: {enhanced_sell_signals[0]}")
                    
            # 发出图表更新信号
            self.chart_update_signal.emit(
                self.module, 
                sample_df,  # 发送采样后的数据
                processed_count,
                enhanced_buy_signals,
                enhanced_sell_signals
            )
            
        except Exception as e:
            print(f"更新图表错误: {str(e)}")
            traceback.print_exc()
    
    def _load_batch_data(self, batch_size, last_date=None):
        """从数据库加载批次数据
        
        Args:
            batch_size: 批次大小
            last_date: 上一批次的最后日期，用于连续加载
            
        Returns:
            DataFrame: 批次数据
        """
        try:
            import pandas as pd
            
            # 检查数据级别
            if not self.data_level:
                self.data_level = "1min"  # 如果未设置，默认使用1min
                print(f"未设置数据级别，使用默认值: {self.data_level}")
            
            # 如果是第一次加载，直接返回预先加载的数据
            if last_date is None and self.stock_data is not None:
                print(f"使用预加载的数据，范围: 0-{len(self.stock_data)}")
                # 如果预加载数据超过了请求的范围，则截取所需部分
                if len(self.stock_data) > batch_size:
                    return self.stock_data.iloc[:batch_size]
                else:
                    return self.stock_data
            
            # 如果数据库连接器不可用，返回空DataFrame
            if not self.db_connector:
                print("无法加载批次数据：数据库连接器未初始化")
                return pd.DataFrame()
                
            conn = self.db_connector.get_connection()
            if not conn:
                print("无法获取数据库连接")
                return pd.DataFrame()
                
            try:
                cursor = conn.cursor()
                
                # 构建查询
                if last_date is None:
                    # 第一批次，直接按日期排序取前N条
                    query = """
                        SELECT date, open, high, low, close, volume, amount
                        FROM stock_quotes
                        WHERE fund_code = %s AND date BETWEEN %s AND %s AND data_level = %s
                        ORDER BY date ASC
                        LIMIT %s
                    """
                    params = (self.pure_code, self.start_date, self.end_date, self.data_level, batch_size)
                    print(f"查询第一批次数据: fund_code={self.pure_code}, 日期范围={self.start_date}至{self.end_date}, 数据级别={self.data_level}, LIMIT={batch_size}")
                else:
                    # 后续批次，从上一批次的最后日期之后开始取
                    query = """
                        SELECT date, open, high, low, close, volume, amount
                        FROM stock_quotes
                        WHERE fund_code = %s AND date BETWEEN %s AND %s AND data_level = %s AND date > %s
                        ORDER BY date ASC
                        LIMIT %s
                    """
                    params = (self.pure_code, self.start_date, self.end_date, self.data_level, last_date, batch_size)
                    print(f"查询后续批次数据: fund_code={self.pure_code}, 日期范围={self.start_date}至{self.end_date}, 数据级别={self.data_level}, 上次日期之后={last_date}, LIMIT={batch_size}")
                
                # 执行查询
                cursor.execute(query, params)
                
                # 获取结果
                results = cursor.fetchall()
                
                # 将结果转换为DataFrame
                if results and len(results) > 0:
                    batch_data = pd.DataFrame(results, columns=[
                        'date', 'open', 'high', 'low', 'close', 'volume', 'amount'
                    ])
                    
                    # 确保日期列是datetime类型
                    batch_data['date'] = pd.to_datetime(batch_data['date'])
                    
                    # 确保数值列是float类型
                    for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
                        batch_data[col] = pd.to_numeric(batch_data[col], errors='coerce')
                    
                    # 确保数据按日期排序
                    batch_data = batch_data.sort_values('date')
                    
                    # 过滤掉无效价格
                    valid_data = batch_data[batch_data['close'] > 0]
                    if len(valid_data) > 0:
                        batch_data = valid_data
                    
                    # 为返回的DataFrame设置数据级别属性
                    batch_data.attrs['data_level'] = self.data_level
                    
                    print(f"成功加载批次数据: {len(batch_data)} 条记录, 日期范围: {batch_data['date'].min()} 至 {batch_data['date'].max()}, 数据级别: {self.data_level}")
                    return batch_data
                else:
                    print(f"批次查询未返回任何数据，可能已到达数据末尾")
                    # 返回空DataFrame而不是None，避免后续处理错误
                    empty_df = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
                    empty_df.attrs['data_level'] = self.data_level
                    return empty_df
            finally:
                self.db_connector.release_connection(conn)
                
        except Exception as e:
            import pandas as pd
            print(f"加载批次数据失败: {str(e)}")
            traceback.print_exc()
            # 返回空DataFrame而不是None，避免后续处理错误
            empty_df = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
            empty_df.attrs['data_level'] = self.data_level if hasattr(self, 'data_level') else "1min"
            return empty_df
    
    def _save_backtest_results(self, fund_code, start_date, end_date, initial_capital, 
                              final_capital, total_profit, total_profit_rate, strategy_id, strategy_name,
                              num_buy_signals, num_sell_signals):
        """保存回测结果到数据库
        
        Args:
            fund_code: 基金代码
            start_date: 开始日期
            end_date: 结束日期
            initial_capital: 初始资金
            final_capital: 最终资金
            total_profit: 总收益
            total_profit_rate: 总收益率
            strategy_id: 策略ID
            strategy_name: 策略名称
            num_buy_signals: 买入信号数量
            num_sell_signals: 卖出信号数量
            
        Returns:
            int: 回测ID，如果保存失败则返回None
        """
        try:
            import time
            import threading
            
            # 更新状态信息
            self.status_signal.emit("正在保存回测结果...")
            
            if not self.db_connector:
                print("无法保存回测结果：数据库连接器未初始化")
                return None
                
            # 创建一个锁，避免多个线程同时访问数据库
            db_lock = threading.Lock()
            
            # 使用锁保护数据库操作
            with db_lock:
                conn = None
                try:
                    # 获取连接并设置超时
                    start_time = time.time()
                    timeout = 10  # 10秒超时
                    
                    while not conn and time.time() - start_time < timeout:
                        conn = self.db_connector.get_connection()
                        if not conn:
                            print("等待数据库连接...")
                            time.sleep(0.5)
                    
                    if not conn:
                        print("无法获取数据库连接：超时")
                        return None
                    
                    # 设置游标
                    cursor = conn.cursor()
                    
                    # 首先检查表是否已经存在所需的列
                    has_count_columns = False
                    try:
                        cursor.execute("""
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_name='backtest_results' 
                            AND (column_name='buy_count' OR column_name='sell_count')
                        """)
                        has_count_columns = len(cursor.fetchall()) >= 2
                    except Exception as e:
                        print(f"检查表结构失败: {str(e)}")
                        # 继续执行，使用默认列
                    
                    # 准备基本数据
                    params = [fund_code, start_date, end_date, initial_capital, final_capital,
                              total_profit, total_profit_rate, strategy_id, strategy_name]
                    
                    # 根据表结构选择SQL
                    if has_count_columns:
                        # 添加买卖信号数量
                        params.extend([num_buy_signals, num_sell_signals])
                        
                        # 构建SQL
                        sql = """
                            INSERT INTO backtest_results 
                            (stock_code, start_date, end_date, initial_capital, final_capital, 
                            total_profit, total_profit_rate, backtest_time, strategy_id, strategy_name,
                            buy_count, sell_count)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s)
                            RETURNING id
                        """
                    else:
                        # 使用基本列
                        sql = """
                            INSERT INTO backtest_results 
                            (stock_code, start_date, end_date, initial_capital, final_capital, 
                            total_profit, total_profit_rate, backtest_time, strategy_id, strategy_name)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s)
                            RETURNING id
                        """
                    
                    # 执行SQL并获取ID
                    self.status_signal.emit("执行数据库插入...")
                    cursor.execute(sql, params)
                    
                    # 获取新插入的回测ID
                    result = cursor.fetchone()
                    if not result:
                        print("插入成功但未返回ID")
                        conn.rollback()
                        return None
                        
                    backtest_id = result[0]
                    
                    # 提交事务
                    self.status_signal.emit("提交数据库事务...")
                    conn.commit()
                    
                    print(f"成功保存回测结果，ID: {backtest_id}")
                    return backtest_id
                    
                except Exception as e:
                    if conn:
                        conn.rollback()
                    print(f"保存回测结果失败: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    return None
                finally:
                    if conn:
                        self.db_connector.release_connection(conn)
                    self.status_signal.emit("回测结果保存完成")
                    
        except Exception as e:
            print(f"保存回测结果过程中出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return None 

    def _load_all_data(self):
        """一次性加载所有数据"""
        try:
            if not self.db_connector:
                print("无法加载数据：数据库连接器未初始化")
                return None
                
            conn = self.db_connector.get_connection()
            if not conn:
                print("无法获取数据库连接")
                return None
                
            try:
                cursor = conn.cursor()
                
                # 查询符合条件的所有数据
                query = """
                    SELECT * 
                    FROM stock_quotes
                    WHERE fund_code = %s AND date BETWEEN %s AND %s AND data_level = %s
                    ORDER BY date
                """
                
                self.status_signal.emit("正在从数据库加载数据...")
                
                cursor.execute(query, (self.pure_code, self.start_date, self.end_date, self.data_level))
                
                # 获取列名
                columns = [desc[0] for desc in cursor.description]
                
                # 获取所有数据
                rows = cursor.fetchall()
                
                # 创建DataFrame
                df = pd.DataFrame(rows, columns=columns)
                
                print(f"从数据库加载了 {len(df)} 条记录")
                return df
                
            finally:
                self.db_connector.release_connection(conn)
                
        except Exception as e:
            print(f"加载数据失败: {str(e)}")
            traceback.print_exc()
            return None 