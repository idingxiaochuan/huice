# 交易专用XIRR计算器说明

## 功能概述

交易专用XIRR计算器是一个用于计算回测交易年化收益率的工具，与标准XIRR计算器的主要区别在于：

1. **不考虑初始资金**：标准XIRR计算器会将回测的初始资金作为第一笔负现金流，而交易专用XIRR计算器只考虑实际发生的交易现金流。
2. **只计算实际交易**：只计算买入和卖出交易产生的现金流，更准确地反映交易策略本身的收益率。
3. **更符合实际交易场景**：适用于只想评估交易策略本身效果，而不考虑初始资金规模影响的场景。

## 计算原理

交易专用XIRR计算器基于以下现金流：

1. **买入交易**：每笔买入交易的金额作为负现金流，时间为交易日期
2. **卖出交易**：每笔卖出交易的金额作为正现金流，时间为交易日期
3. **最终持仓**：如果回测结束时有持仓，将持仓市值作为正现金流，时间为回测结束日期

与标准XIRR计算器的区别是，不将初始资金作为第一笔负现金流。

## 使用方法


### GUI界面

1. 打开交易报告窗口
2. 通过查询条件筛选回测记录
3. 在回测汇总表中选择一条回测记录
4. 点击下方的"计算交易专用XIRR"按钮
5. 查看XIRR计算结果
6. 可选择导出到Excel保存详细结果

### 编程API

可以在您的代码中直接使用交易专用XIRR计算器：

```python
from backtest_gui.utils.xirr_calculator_trades_only import XIRRCalculatorTradesOnly
from backtest_gui.utils.db_connector import DBConnector

# 创建数据库连接器
db_connector = DBConnector()

# 创建交易专用XIRR计算器
calculator = XIRRCalculatorTradesOnly(db_connector)

# 计算指定回测ID的XIRR
result = calculator.calculate_backtest_xirr(123)
if result and result['xirr'] is not None:
    print(f"交易专用XIRR = {result['xirr']:.2f}%")
else:
    print("无法计算交易专用XIRR")

# 导出到Excel
calculator.export_to_excel(123, "trades_only_xirr_results.xlsx")
```

## Excel导出

导出的Excel文件包含两个工作表：

1. **基本信息**：回测基本信息和XIRR计算结果
2. **现金流数据**：详细的现金流记录，包含一个Excel XIRR函数公式用于验证计算结果

## 与Excel示例对比

交易专用XIRR计算器的计算方法与Excel中的XIRR函数计算方法一致，只是不包含初始资金作为第一笔负现金流。这种计算方式更适合评估交易策略本身的效果，不受初始资金规模的影响。

在Excel中，您可以通过以下方式验证计算结果：

1. 在一列中输入交易日期
2. 在另一列中输入对应的现金流（买入为负，卖出为正）
3. 使用`=XIRR(现金流范围, 日期范围)`函数计算XIRR

## 注意事项

1. **未完成交易**：如果回测结束时仍有持仓（未完成交易），XIRR计算会将当前持仓价值作为最后一个现金流。此时计算结果会受到最后持仓估值的影响。

2. **计算失败**：在以下情况下XIRR可能无法计算：
   - 所有现金流均为同一符号（全正或全负）
   - 没有足够的现金流数据（至少需要一笔买入和一笔卖出）
   - 数据异常导致无解

3. **合理范围限制**：为避免计算结果异常，XIRR计算器会限制结果在合理范围内：
   - 如果XIRR大于1000%或小于-90%，视为计算错误 