# 用前须知

## xtdata提供和MiniQmt的交互接口，本质是和MiniQmt建立连接，由MiniQmt处理行情数据请求，再把结果回传返回到python层。使用的行情服务器以及能获取到的行情数据和MiniQmt是一致的，要检查数据或者切换连接时直接操作MiniQmt即可。

## 对于数据获取接口，使用时需要先确保MiniQmt已有所需要的数据，如果不足可以通过补充数据接口补充，再调用数据获取接口获取。

## 对于订阅接口，直接设置数据回调，数据到来时会由回调返回。订阅接收到的数据一般会保存下来，同种数据不需要再单独补充。

# 代码讲解

# 从本地python导入xtquant库，如果出现报错则说明安装失败
from xtquant import xtdata
import time
# 确保pandas已安装
try:
    import pandas as pd
except ImportError:
    print("pandas未安装，请执行: pip install pandas")
    exit(1)

# 设定一个标的列表
code_list = ["515170.SH"]
# 设定获取数据的周期
period = "1m"

# 下载标的行情数据
try:
    # 下载单个股票数据，避免在循环中多次下载导致中断
    print("开始下载行情数据...")
    xtdata.download_history_data(code_list[0], period=period, incrementally=True)
    print("行情数据下载完成")
    
    # 可选：下载财务和板块数据
    # print("开始下载财务数据...")
    # xtdata.download_financial_data(code_list)
    # print("开始下载板块数据...")
    # xtdata.download_sector_data()
except KeyboardInterrupt:
    print("数据下载被中断，继续执行程序...")
except Exception as e:
    print(f"数据下载出错: {e}")

# 读取本地历史行情数据
try:
    print("获取历史行情数据...")
    history_data = xtdata.get_market_data_ex([], code_list, period=period, count=30)  # 限制只获取最近30条数据
    print(history_data)
    print("=" * 20)
except Exception as e:
    print(f"获取历史数据失败: {e}")

# 尝试订阅实时数据
try:
    print("开始订阅实时行情...")
    # 向服务器订阅数据
    for i in code_list:
        xtdata.subscribe_quote(i, period=period, count=10)  # 限制只获取10条实时行情

    # 等待订阅完成
    time.sleep(1)

    # 获取订阅后的行情
    kline_data = xtdata.get_market_data_ex([], code_list, period=period)
    print("实时行情数据:")
    print(kline_data)

    # 只获取3次实时数据更新，避免无限循环
    print("开始监控实时行情更新...")
    for i in range(3):
        kline_data = xtdata.get_market_data_ex([], code_list, period=period)
        print(f"第{i+1}次更新:")
        print(kline_data)
        time.sleep(3)  # 三秒后再次获取行情
    
    # 定义回调函数
    def f(data):
        code_list = list(data.keys())    # 获取到本次触发的标的代码
        kline_in_callback = xtdata.get_market_data_ex([], code_list, period=period)    # 在回调中获取klines数据
        print("回调获取到的数据:")
        print(kline_in_callback)

    # 设置回调订阅，运行30秒后退出
    for i in code_list:
        xtdata.subscribe_quote(i, period=period, count=10, callback=f)
    
    print("程序将运行30秒后自动退出...")
    # 使用timeout运行，避免程序无限阻塞
    xtdata.run(timeout=30)
    
except Exception as e:
    print(f"订阅或获取实时数据失败: {e}")

print("程序执行完毕")



