#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
分钟线数据获取器独立进程
在独立进程中运行，避免GIL锁问题
"""

import os
import sys
import time
import traceback
import json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from PyQt5.QtCore import QThread, pyqtSignal
from .db.database import StockDatabase
from .utils.time_utils import convert_timestamp_to_datetime, is_valid_date
from backtest_gui.utils.time_utils import convert_timestamp_to_datetime

class MinuteDataFetcher(QThread):
    update_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    finished_signal = pyqtSignal(bool)
    
    def __init__(self, fund_code, data_level, start_date, end_date, parent=None):
        super().__init__(parent)
        self.fund_code = fund_code
        self.data_level = data_level
        self.start_date = start_date
        self.end_date = end_date
        self.db = StockDatabase()
        
    def run(self):
        try:
            self.update_signal.emit(f"开始获取 {self.fund_code} {self.data_level} 级别数据...")
            
            # 检查数据库中是否已存在相同基金、级别和时间段的数据
            if self.check_existing_data():
                self.update_signal.emit(f"数据库中已存在 {self.fund_code} {self.data_level} 在指定时间段的数据，跳过获取")
                self.finished_signal.emit(True)
                return
                
            # 根据数据级别选择不同的获取方法
            if self.data_level == '1min':
                self.fetch_1min_data()
            elif self.data_level == 'day':
                self.fetch_daily_data()
            elif self.data_level == 'week':
                self.fetch_weekly_data()
            elif self.data_level == 'month':
                self.fetch_monthly_data()
            else:
                self.update_signal.emit(f"不支持的数据级别: {self.data_level}")
                self.finished_signal.emit(False)
                return
                
            self.update_signal.emit(f"{self.fund_code} {self.data_level} 数据获取完成")
            self.finished_signal.emit(True)
        except Exception as e:
            self.update_signal.emit(f"数据获取错误: {str(e)}")
            self.finished_signal.emit(False)
    
    def check_existing_data(self):
        """
        检查数据库中是否已存在相同基金、级别和时间段的数据
        
        Returns:
            bool: 如果数据已存在返回True，否则返回False
        """
        try:
            # 将日期字符串转换为datetime对象
            start_dt = datetime.strptime(self.start_date, '%Y%m%d')
            end_dt = datetime.strptime(self.end_date, '%Y%m%d')
            
            # 转换为时间戳范围
            start_timestamp = int(start_dt.timestamp() * 1000)
            end_timestamp = int(end_dt.timestamp() * 1000) + 24*60*60*1000  # 加上一天
            
            # 查询数据库中该基金代码和数据级别在指定时间范围内的记录数
            query = f"""
            SELECT COUNT(*) as count FROM stock_quotes 
            WHERE fund_code = '{self.fund_code}' 
            AND data_level = '{self.data_level}'
            AND time >= {start_timestamp}
            AND time <= {end_timestamp}
            """
            
            result = self.db.execute_query(query)
            if result and len(result) > 0:
                count = result[0]['count']
                # 如果记录数大于0，说明已有数据
                return count > 0
                
            return False
        except Exception as e:
            self.update_signal.emit(f"检查现有数据时出错: {str(e)}")
            return False
            
    def fetch_1min_data(self):
        # 实现1分钟数据获取逻辑
        # ... 现有代码 ...
        pass
        
    def fetch_daily_data(self):
        # 实现日线数据获取逻辑
        # ... 现有代码 ...
        pass
        
    def fetch_weekly_data(self):
        # 实现周线数据获取逻辑
        # ... 现有代码 ...
        pass
        
    def fetch_monthly_data(self):
        # 实现月线数据获取逻辑
        # ... 现有代码 ...
        pass

def find_qmt_path():
    """寻找QMT的安装路径"""
    possible_paths = [
        r"D:\国金QMT交易端模拟\userdata_mini",
        r"C:\国金QMT交易端模拟\userdata_mini",
        r"D:\国金证券QMT交易端\userdata_mini",
        r"C:\国金证券QMT交易端\userdata_mini",
        r"C:\Program Files\国金证券QMT交易端\userdata_mini",
        r"C:\Program Files (x86)\国金证券QMT交易端\userdata_mini",
        r"C:\Program Files\国金QMT交易端模拟\userdata_mini",
        r"C:\Program Files (x86)\国金QMT交易端模拟\userdata_mini",
        r"C:\Users\Administrator\Desktop\国金QMT交易端模拟\userdata_mini",
        r"C:\Users\Administrator\Desktop\国金证券QMT交易端\userdata_mini",
        r"D:国金证券QMT交易端\bin.x64\..\userdata_mini",
        r"D:国金证券QMT交易端\userdata_mini",
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            lib_paths = [
                os.path.join(path, 'lib', 'site-packages'),
                os.path.join(path, 'lib'),
                os.path.join(os.path.dirname(path), 'lib'),
                os.path.join(os.path.dirname(path), 'lib', 'site-packages'),
            ]
            
            for lib_path in lib_paths:
                if os.path.exists(lib_path) and lib_path not in sys.path:
                    print(f"找到QMT路径: {path}")
                    print(f"添加库路径: {lib_path}")
                    sys.path.insert(0, lib_path)
                    return path
    
    print("未找到QMT路径，请确保已安装QMT交易端")
    return None

def fetch_minute_data(symbol, start_date, end_date, data_level, output_file):
    """获取分钟级数据"""
    result = {
        "success": False,
        "message": "",
        "data": [],
        "data_count": 0
    }
    
    try:
        # 查找QMT路径
        qmt_path = find_qmt_path()
        if not qmt_path:
            result["message"] = "未找到QMT路径"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False)
            return result
        
        print(f"找到有效的QMT路径: {qmt_path}")
        
        # 导入xtdata模块
        sys.path.append(qmt_path)
        try:
            import xtdata
        except ImportError:
            result["message"] = "无法导入xtdata模块，请确认QMT安装正确"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False)
            return result
        
        # 初始化xtdata
        try:
            xtdata.init()
        except Exception as e:
            result["message"] = f"初始化xtdata失败: {str(e)}"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False)
            return result
        
        # 尝试不同的周期格式
        periods_to_try = []
        
        if data_level == "1min" or data_level == "1m":
            periods_to_try = ["1min", "1m", "1"]
        elif data_level == "5min" or data_level == "5m":
            periods_to_try = ["5min", "5m", "5"]
        elif data_level == "15min" or data_level == "15m":
            periods_to_try = ["15min", "15m", "15"]
        elif data_level == "30min" or data_level == "30m":
            periods_to_try = ["30min", "30m", "30"]
        elif data_level == "60min" or data_level == "1h":
            periods_to_try = ["60min", "60m", "1h"]
        else:
            periods_to_try = [data_level]
        
        # 尝试获取数据
        for period in periods_to_try:
            try:
                print(f"尝试获取 {symbol} 的 {period} 数据...")
                df = xtdata.get_market_data(
                    ['time', 'open', 'high', 'low', 'close', 'volume', 'amount'], 
                    [symbol], 
                    period=period, 
                    start_time=start_date, 
                    end_time=end_date
                )
                if df and symbol in df:
                    print(f"成功获取 {symbol} 的 {period} 数据")
                    df = pd.DataFrame(df[symbol])
                    break
            except Exception as e:
                print(f"获取 {symbol} 的 {period} 数据失败: {str(e)}")
        else:
            print(f"警告: 所有周期格式的下载都失败，尝试继续获取数据")
        
        # 尝试获取数据的替代方法（绕过GIL锁问题）
        methods_to_try = [
            try_get_market_data_ex,
            try_subscribe_and_get,
            try_get_l2_quote,
        ]
        
        for method in methods_to_try:
            try:
                print(f"尝试使用方法 {method.__name__} 获取数据...")
                df = method(xtdata, symbol, periods_to_try, start_date, end_date)
                if df is not None and not df.empty:
                    # 添加date列(如果不存在)
                    if 'date' not in df.columns and 'time' in df.columns:
                        # 打印时间列信息
                        print(f"time列类型: {df['time'].dtype}")
                        print(f"time列前5个值: {df['time'].head().tolist()}")
                        
                        # 确保time列是数值类型
                        try:
                            if not pd.api.types.is_numeric_dtype(df['time']):
                                print("time列不是数值类型，尝试转换...")
                                df['time'] = pd.to_numeric(df['time'], errors='coerce')
                                print(f"转换后time列类型: {df['time'].dtype}")
                        except Exception as e:
                            print(f"转换time列类型失败: {str(e)}")
                        
                        # 检查time列是否有NaN值
                        if df['time'].isna().any():
                            print(f"警告: time列有 {df['time'].isna().sum()} 个NaN值")
                            # 填充NaN值
                            df = df.dropna(subset=['time'])
                            print(f"删除NaN后剩余记录数: {len(df)}")
                        
                        # 逐行转换时间戳，避免批量处理可能的错误
                        print("开始逐行转换时间戳...")
                        from backtest_gui.utils.time_utils import convert_timestamp_to_datetime
                        dates = []
                        for i, ts in enumerate(df['time']):
                            try:
                                date = convert_timestamp_to_datetime(ts)
                                if date is None:
                                    print(f"警告: 第{i}行时间戳 {ts} 转换结果为None")
                                    # 使用当前时间作为默认值
                                    date = pd.Timestamp.now()
                                dates.append(date)
                            except Exception as e:
                                print(f"警告: 第{i}行时间戳 {ts} 转换失败: {str(e)}")
                                # 使用当前时间作为默认值
                                dates.append(pd.Timestamp.now())
                        
                        # 将转换后的日期添加到DataFrame
                        df['date'] = dates
                        print(f"时间戳转换完成，date列前5个值: {[str(d) for d in df['date'].head().tolist()]}")
                    
                    # 添加symbol列
                    if 'symbol' not in df.columns:
                        df['symbol'] = symbol
                        
                    # 添加freq列
                    if 'freq' not in df.columns:
                        df['freq'] = data_level
                    
                    # 检查是否有无效数据
                    invalid_rows = df[df['date'].isna()].shape[0]
                    if invalid_rows > 0:
                        print(f"警告: 有 {invalid_rows} 行数据的日期无效，将被过滤")
                        df = df.dropna(subset=['date'])
                    
                    # 转换DataFrame为Python字典列表
                    df_dict = df.to_dict('records')
                    
                    # 保存到JSON输出文件
                    result["success"] = True
                    result["message"] = f"成功使用 {method.__name__} 获取 {symbol} 的 {data_level} 数据"
                    result["data"] = df_dict
                    result["data_count"] = len(df_dict)
                    
                    with open(output_file, 'w', encoding='utf-8') as f:
                        json.dump(result, f, ensure_ascii=False)
                    
                    print(f"成功获取 {symbol} 的 {data_level} 数据，共 {len(df_dict)} 条记录，保存到 {output_file}")
                    return result
            except Exception as e:
                print(f"使用 {method.__name__} 获取数据失败: {str(e)}")
                traceback.print_exc()
        
        # 如果所有方法都失败，返回错误
        result["message"] = f"无法获取 {symbol} 的 {data_level} 数据，请检查QMT连接和数据可用性"
        
        # 保存结果
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False)
            
        return result
        
    except Exception as e:
        result["message"] = f"获取 {symbol} 的 {data_level} 数据异常: {str(e)}"
        traceback.print_exc()
        
        # 即使失败也保存结果
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False)
        except:
            pass
            
        return result

def try_get_market_data_ex(xtdata, symbol, periods, start_date, end_date):
    """尝试使用get_market_data_ex获取数据"""
    fields = ['time', 'open', 'high', 'low', 'close', 'volume', 'amount']
    
    for period in periods:
        try:
            # 根据文档，正确的周期格式应该是：
            # 1m, 5m, 15m, 30m, 60m, 1d, 1w, 1mon
            if period == "1min":
                api_period = "1m"
            elif period == "5min":
                api_period = "5m"
            elif period == "15min":
                api_period = "15m"
            elif period == "30min":
                api_period = "30m"
            elif period == "60min" or period == "1h":
                api_period = "60m"
            elif period == "day" or period == "1d":
                api_period = "1d"
            elif period == "week":
                api_period = "1w"
            elif period == "month":
                api_period = "1mon"
            else:
                api_period = period
                
            print(f"尝试获取 {symbol} 的 {period} 数据...")
            data = xtdata.get_market_data_ex(
                fields, 
                [symbol], 
                period=api_period, 
                start_time=start_date, 
                end_time=end_date
            )
            if data and symbol in data:
                print(f"成功获取 {symbol} 的 {period} 数据")
                return pd.DataFrame(data[symbol])
        except Exception as e:
            print(f"获取 {symbol} 的 {period} 数据失败: {str(e)}")
    
    return None

def try_subscribe_and_get(xtdata, symbol, periods, start_date, end_date):
    """尝试先订阅再获取数据"""
    fields = ['time', 'open', 'high', 'low', 'close', 'volume', 'amount']
    
    for period in periods:
        try:
            # 根据文档，正确的周期格式应该是：
            # 1m, 5m, 15m, 30m, 60m, 1d, 1w, 1mon
            if period == "1min":
                api_period = "1m"
            elif period == "5min":
                api_period = "5m"
            elif period == "15min":
                api_period = "15m"
            elif period == "30min":
                api_period = "30m"
            elif period == "60min" or period == "1h":
                api_period = "60m"
            elif period == "day" or period == "1d":
                api_period = "1d"
            elif period == "week":
                api_period = "1w"
            elif period == "month":
                api_period = "1mon"
            else:
                api_period = period
                
            print(f"尝试订阅 {symbol} 的 {period} 数据...")
            xtdata.subscribe_quote(symbol, period=api_period, count=10)
            time.sleep(1)  # 等待订阅完成
            
            data = xtdata.get_market_data_ex(
                fields, 
                [symbol], 
                period=api_period,
                start_time=start_date, 
                end_time=end_date
            )
            if data and symbol in data:
                print(f"成功通过订阅获取 {symbol} 的 {period} 数据")
                return pd.DataFrame(data[symbol])
        except Exception as e:
            print(f"通过订阅获取 {symbol} 的 {period} 数据失败: {str(e)}")
    
    return None

def try_get_l2_quote(xtdata, symbol, periods, start_date, end_date):
    """尝试获取L2行情"""
    try:
        if hasattr(xtdata, 'get_l2_quote'):
            print(f"尝试获取 {symbol} 的L2行情...")
            quote_data = xtdata.get_l2_quote([symbol])
            if quote_data and symbol in quote_data:
                print(f"成功获取 {symbol} 的L2行情")
                # 转换为K线格式
                df = pd.DataFrame([quote_data[symbol]])
                
                # 添加必要的字段
                if 'time' not in df.columns:
                    df['time'] = int(datetime.now().timestamp() * 1000)
                if 'open' not in df.columns and 'last' in df.columns:
                    df['open'] = df['last']
                if 'high' not in df.columns and 'last' in df.columns:
                    df['high'] = df['last']
                if 'low' not in df.columns and 'last' in df.columns:
                    df['low'] = df['last']
                if 'close' not in df.columns and 'last' in df.columns:
                    df['close'] = df['last']
                
                return df
    except Exception as e:
        print(f"获取L2行情失败: {str(e)}")
    
    return None

if __name__ == "__main__":
    # 从命令行参数获取输入
    if len(sys.argv) < 6:
        print("Usage: python minute_data_fetcher.py <symbol> <start_date> <end_date> <data_level> <output_file>")
        sys.exit(1)
    
    symbol = sys.argv[1]
    start_date = sys.argv[2]
    end_date = sys.argv[3]
    data_level = sys.argv[4]
    output_file = sys.argv[5]
    
    result = fetch_minute_data(symbol, start_date, end_date, data_level, output_file)
    
    # 输出结果为JSON
    print(json.dumps(result))
    
    # 正常退出，返回状态码
    sys.exit(0 if result["success"] else 1) 