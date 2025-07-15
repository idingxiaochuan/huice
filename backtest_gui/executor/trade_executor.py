class TradeExecutor:
    """交易执行器"""
    
    def __init__(self, initial_capital=100000.0):
        """初始化交易执行器
        
        Args:
            initial_capital: 初始资金
        """
        # 账户状态
        self.initial_capital = initial_capital  # 初始资金
        self.cash = initial_capital  # 当前现金
        self.total_shares = 0  # 持有股票总数
        self.avg_cost = 0.0  # 平均成本
        self.total_cost = 0.0  # 总成本
        self.total_value = 0.0  # 持仓市值
        self.total_assets = initial_capital  # 总资产
        self.total_profit = 0.0  # 总盈亏
        self.position_profit = 0.0  # 持仓盈亏
        self.current_price = None  # 当前价格
        
        # 新增：记录最大资金占用
        self.max_capital_used = 0.0  # 最大资金占用
        
        # 交易记录
        self.trades = []
        self.paired_trades = []
        
        # 持仓记录（按网格级别）
        self.positions = {}
        
        # 回测ID
        self.backtest_id = None 