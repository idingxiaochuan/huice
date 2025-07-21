# 回测交易XIRR年化收益率计算功能

## 功能概述

XIRR（扩展内部收益率）是一种常用的计算不规则间隔现金流的年化收益率的方法。在投资和回测分析中，XIRR能够更准确地反映投资的真实收益情况，考虑了资金投入和收回的时间价值。

本模块为回测系统增加了XIRR计算功能，用于分析回测交易的真实年化收益率，提供多种使用方式：

1. 命令行工具：直接通过脚本计算并查看XIRR
2. GUI界面集成：在交易报告窗口添加XIRR计算按钮
3. Excel导出：将XIRR计算结果和现金流数据导出到Excel

## 使用方法

### 命令行工具

使用`xirr_backtest_trades.py`脚本，可以快速查询回测的XIRR：

```bash
# 列出所有回测记录
python xirr_backtest_trades.py -l

# 计算指定回测ID的XIRR
python xirr_backtest_trades.py 123

# 计算并导出Excel
python xirr_backtest_trades.py 123 -o xirr_results.xlsx

# 仅显示XIRR结果，不导出Excel
python xirr_backtest_trades.py 123 --no-excel
```

### GUI界面

1. 打开交易报告窗口
2. 通过查询条件筛选回测记录
3. 在回测汇总表中选择一条回测记录
4. 点击下方的"计算XIRR年化收益率"按钮
5. 查看XIRR计算结果
6. 可选择"导出到Excel"按钮保存详细结果

### 编程API

可以在您的代码中直接使用XIRR计算器：

```python
from backtest_gui.utils.xirr_calculator import XIRRCalculator
from backtest_gui.utils.db_connector import DBConnector

# 创建数据库连接器
db_connector = DBConnector()

# 创建XIRR计算器
calculator = XIRRCalculator(db_connector)

# 计算指定回测ID的XIRR
result = calculator.calculate_backtest_xirr(123)
if result and result['xirr'] is not None:
    print(f"XIRR = {result['xirr']:.2f}%")
else:
    print("无法计算XIRR")

# 导出到Excel
calculator.export_to_excel(123, "xirr_results.xlsx")
```

## XIRR计算原理

XIRR计算基于以下现金流：

1. **初始资金**：作为负现金流，时间为回测开始日期
2. **买入交易**：每笔买入交易的金额作为负现金流，时间为交易日期
3. **卖出交易**：每笔卖出交易的金额作为正现金流，时间为交易日期
4. **最终持仓**：如果回测结束时有持仓，将持仓市值作为正现金流，时间为回测结束日期

XIRR满足以下等式：
0 = ∑[CFi / (1 + XIRR)^((di - d0)/365)]

其中：
- CFi：第i个现金流
- di：第i个现金流的日期
- d0：第一个现金流的日期

## 特殊说明

1. **未完成交易**：如果回测结束时仍有持仓（未完成交易），XIRR计算会将当前持仓价值作为最后一个现金流。此时计算结果会受到最后持仓估值的影响。

2. **Excel导出**：导出的Excel包含两个工作表：
   - 基本信息：回测基本信息和XIRR计算结果
   - 现金流数据：详细的现金流记录，包含一个Excel XIRR函数公式用于验证计算结果

3. **计算失败**：在以下情况下XIRR可能无法计算：
   - 所有现金流均为同一符号（全正或全负）
   - 没有足够的现金流数据
   - 数据异常导致无解

## 依赖库

- pandas：用于数据处理
- numpy：用于数值计算
- scipy：用于XIRR的数值求解
- openpyxl：用于Excel导出功能 