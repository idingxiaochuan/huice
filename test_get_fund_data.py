#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试从QMT获取基金数据并保存到数据库的完整流程
"""

from PyQt5.QtCore import QCoreApplication
import sys
from backtest_gui.fund_data_fetcher import FundDataFetcher
import time

def main():
    # 创建应用
    app = QCoreApplication(sys.argv)
    
    # 创建数据获取器
    fetcher = FundDataFetcher()
    
    # 设置信号处理函数
    def on_progress(current, total, message):
        print(f"进度: {current}/{total} - {message}")
    
    def on_completed(success, message, data):
        print(f"完成: {'成功' if success else '失败'} - {message}")
        if data is not None:
            print(f"获取到 {len(data)} 条数据")
            print("数据示例:")
            print(data.head())
            
            # 检查日期是否正确
            if 'date' in data.columns:
                print("\n检查日期是否正确:")
                for i, (_, row) in enumerate(data.head().iterrows()):
                    print(f"记录 {i+1}:")
                    print(f"  - 时间戳: {row['time']}")
                    print(f"  - 日期: {row['date']}")
                    print(f"  - 年份: {row['date'].year}")
        
        # 延迟退出，以便查看结果
        print("\n测试完成，3秒后退出...")
        time.sleep(3)
        app.quit()
    
    def on_error(message):
        print(f"错误: {message}")
        app.quit()
    
    fetcher.progress_signal.connect(on_progress)
    fetcher.completed_signal.connect(on_completed)
    fetcher.error_signal.connect(on_error)
    
    # 测试获取数据
    symbol = "515170.SH"  # 测试用ETF
    start_date = "20240501"  # 2024年5月1日
    end_date = "20240610"    # 2024年6月10日
    data_level = "day"       # 日线数据
    
    print(f"开始获取 {symbol} 的 {data_level} 数据，日期范围: {start_date} - {end_date}")
    fetcher.fetch_data(symbol, start_date=start_date, end_date=end_date, data_level=data_level, save_to_db=True)
    
    # 运行应用
    sys.exit(app.exec_())

if __name__ == "__main__":
    main() 