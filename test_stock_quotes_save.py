from backtest_gui.fund_data_fetcher import FundDataFetcher
from PyQt5.QtWidgets import QApplication
import sys
import traceback
import pandas as pd
from datetime import datetime

def on_progress(current, total, message):
    print(f'Progress: {current}/{total} - {message}')

def on_completed(success, message, data):
    print(f'Completed: {success} - {message}')
    if success and data is not None:
        print(f'Data shape: {data.shape}')
        print(f'First few rows:')
        print(data.head())
        
        # 检查数据中的列
        print(f'Data columns: {data.columns.tolist()}')
        
        # 检查数据类型
        print(f'Data types: {data.dtypes}')
        
        # 检查时间戳和日期
        if 'time' in data.columns:
            print("\n时间戳示例:")
            for i in range(min(5, len(data))):
                time_val = data['time'].iloc[i]
                print(f"原始时间戳: {time_val}")
                # 转换时间戳
                date_from_timestamp = pd.to_datetime(time_val / 1000, unit='s')
                print(f"转换后日期: {date_from_timestamp}")
                
        if 'date' in data.columns:
            print("\n日期列示例:")
            for i in range(min(5, len(data))):
                date_val = data['date'].iloc[i]
                print(f"日期值: {date_val}, 类型: {type(date_val)}")
                
        # 尝试手动修复日期并保存到数据库
        print("\n尝试手动修复日期并检查结果:")
        try:
            # 创建数据副本
            fixed_data = data.copy()
            # 修复日期列
            if 'time' in fixed_data.columns:
                fixed_data['date'] = pd.to_datetime(fixed_data['time'] / 1000, unit='s')
                print("日期已修复，前5行:")
                for i in range(min(5, len(fixed_data))):
                    print(f"索引: {fixed_data.index[i]}, 时间戳: {fixed_data['time'].iloc[i]}, 修复后日期: {fixed_data['date'].iloc[i]}")
                
                # 检查原始数据是否被修改
                print("\n检查原始数据是否被修改:")
                for i in range(min(5, len(data))):
                    print(f"原始数据 - 索引: {data.index[i]}, 日期值: {data['date'].iloc[i]}")
        except Exception as e:
            print(f"手动修复日期时出错: {str(e)}")
            traceback.print_exc()

def on_error(message):
    print(f'Error: {message}')
    traceback.print_exc()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    fetcher = FundDataFetcher()
    
    # Connect signals
    fetcher.progress_signal.connect(on_progress)
    fetcher.completed_signal.connect(on_completed)
    fetcher.error_signal.connect(on_error)
    
    # Fetch data with day period
    print("Testing data save to stock_quotes table...")
    fetcher.fetch_data("515170.SH", data_level="day", save_to_db=True)
    
    # Run the application
    sys.exit(app.exec_()) 