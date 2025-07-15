-- 创建回测结果表，用于保存回测的基本信息
CREATE TABLE IF NOT EXISTS backtest_results (
    id SERIAL PRIMARY KEY,
    stock_code VARCHAR(20) NOT NULL,               -- 基金代码
    start_date TIMESTAMP NOT NULL,                 -- 回测起始日期
    end_date TIMESTAMP NOT NULL,                   -- 回测结束日期
    initial_capital NUMERIC(15, 2) NOT NULL,       -- 初始资金
    final_capital NUMERIC(15, 2) NOT NULL,         -- 最终资金
    total_profit NUMERIC(15, 2) NOT NULL,          -- 总收益
    total_profit_rate NUMERIC(10, 4) NOT NULL,     -- 总收益率(%)
    backtest_time TIMESTAMP NOT NULL DEFAULT NOW(), -- 回测时间
    strategy_id INTEGER,                           -- 关联的波段策略ID
    strategy_name VARCHAR(100),                    -- 策略名称，冗余存储以便查询
    strategy_version_id INTEGER,                   -- 波段回测版本ID
    UNIQUE (stock_code, start_date, end_date, backtest_time)
);

-- 创建回测交易记录表，用于保存每笔交易
CREATE TABLE IF NOT EXISTS backtest_trades (
    id SERIAL PRIMARY KEY,
    backtest_id INTEGER NOT NULL REFERENCES backtest_results(id) ON DELETE CASCADE, -- 关联的回测ID
    trade_time TIMESTAMP NOT NULL,               -- 交易时间
    trade_type VARCHAR(10) NOT NULL,             -- 交易类型(买入/卖出)
    price NUMERIC(10, 4) NOT NULL,               -- 交易价格
    amount INTEGER NOT NULL,                     -- 交易数量
    trade_value NUMERIC(15, 2) NOT NULL,         -- 交易金额
    level INTEGER,                               -- 档位级别
    grid_type VARCHAR(20),                       -- 网格类型
    band_profit NUMERIC(15, 2),                  -- 波段收益
    band_profit_rate NUMERIC(10, 4),             -- 波段收益率(%)
    remaining INTEGER                            -- 剩余股数
);

-- 创建配对交易记录表，用于保存买卖配对信息
CREATE TABLE IF NOT EXISTS backtest_paired_trades (
    id SERIAL PRIMARY KEY,
    backtest_id INTEGER NOT NULL REFERENCES backtest_results(id) ON DELETE CASCADE, -- 关联的回测ID
    level INTEGER NOT NULL,                      -- 档位级别
    grid_type VARCHAR(20),                       -- 网格类型
    buy_time TIMESTAMP NOT NULL,                 -- 买入时间
    buy_price NUMERIC(10, 4) NOT NULL,           -- 买入价格
    buy_amount INTEGER NOT NULL,                 -- 买入数量
    buy_value NUMERIC(15, 2) NOT NULL,           -- 买入金额
    sell_time TIMESTAMP,                         -- 卖出时间
    sell_price NUMERIC(10, 4),                   -- 卖出价格
    sell_amount INTEGER,                         -- 卖出数量
    sell_value NUMERIC(15, 2),                   -- 卖出金额
    remaining INTEGER,                           -- 剩余股数
    band_profit NUMERIC(15, 2),                  -- 波段收益
    band_profit_rate NUMERIC(10, 4),             -- 波段收益率(%)
    status VARCHAR(10) NOT NULL DEFAULT '进行中'  -- 状态(进行中/已完成)
);

-- 创建持仓记录表，用于保存每次回测结束时的持仓信息
CREATE TABLE IF NOT EXISTS backtest_positions (
    id SERIAL PRIMARY KEY,
    backtest_id INTEGER NOT NULL REFERENCES backtest_results(id) ON DELETE CASCADE, -- 关联的回测ID
    position_amount INTEGER NOT NULL,           -- 持仓数量
    position_cost NUMERIC(10, 4) NOT NULL,      -- 持仓成本
    last_price NUMERIC(10, 4) NOT NULL,         -- 最后价格
    position_value NUMERIC(15, 2) NOT NULL      -- 持仓市值
);

-- 创建净值数据表，用于保存回测的净值数据
CREATE TABLE IF NOT EXISTS backtest_nav (
    id SERIAL PRIMARY KEY,
    backtest_id INTEGER NOT NULL REFERENCES backtest_results(id) ON DELETE CASCADE, -- 关联的回测ID
    time TIMESTAMP NOT NULL,                    -- 时间点
    nav NUMERIC(10, 4) NOT NULL                 -- 净值
);

-- 创建波段策略表，用于保存波段策略配置
CREATE TABLE IF NOT EXISTS band_strategies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,                    -- 策略名称
    description TEXT,                              -- 策略描述
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),   -- 创建时间
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()    -- 更新时间
);

-- 创建网格级别表，用于保存波段策略的网格级别配置
CREATE TABLE IF NOT EXISTS grid_levels (
    id SERIAL PRIMARY KEY,
    strategy_id INTEGER NOT NULL,                  -- 关联的波段策略ID
    level INTEGER NOT NULL,                        -- 级别编号
    grid_type VARCHAR(20) NOT NULL,                -- 网格类型 (NORMAL, SMALL, MEDIUM, LARGE)
    buy_price NUMERIC(10, 4) NOT NULL,             -- 买入价格
    sell_price NUMERIC(10, 4) NOT NULL,            -- 卖出价格
    buy_shares NUMERIC(15, 4) NOT NULL,            -- 买入数量
    sell_shares NUMERIC(15, 4) NOT NULL,           -- 卖出数量
    FOREIGN KEY (strategy_id) REFERENCES band_strategies(id) ON DELETE CASCADE,
    UNIQUE (strategy_id, level)
);

-- 创建基金与波段策略的绑定关系表
CREATE TABLE IF NOT EXISTS fund_strategy_bindings (
    id SERIAL PRIMARY KEY,
    fund_code VARCHAR(20) NOT NULL,                -- 基金代码
    strategy_id INTEGER NOT NULL,                  -- 关联的波段策略ID
    is_default BOOLEAN NOT NULL DEFAULT FALSE,     -- 是否为默认策略
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),   -- 创建时间
    FOREIGN KEY (strategy_id) REFERENCES band_strategies(id) ON DELETE CASCADE,
    UNIQUE (fund_code, strategy_id)
);

-- 索引创建
CREATE INDEX IF NOT EXISTS idx_backtest_results_stock_code ON backtest_results(stock_code);
CREATE INDEX IF NOT EXISTS idx_backtest_trades_backtest_id ON backtest_trades(backtest_id);
CREATE INDEX IF NOT EXISTS idx_backtest_paired_trades_backtest_id ON backtest_paired_trades(backtest_id);
CREATE INDEX IF NOT EXISTS idx_backtest_positions_backtest_id ON backtest_positions(backtest_id);
CREATE INDEX IF NOT EXISTS idx_backtest_nav_backtest_id ON backtest_nav(backtest_id);

-- 添加strategy_id、strategy_name和strategy_version_id字段到回测结果表（如果不存在）
DO $$
BEGIN
    -- 检查strategy_id字段是否存在
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name='backtest_results' AND column_name='strategy_id'
    ) THEN
        -- 添加strategy_id字段
        ALTER TABLE backtest_results ADD COLUMN strategy_id INTEGER;
    END IF;
    
    -- 检查strategy_name字段是否存在
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name='backtest_results' AND column_name='strategy_name'
    ) THEN
        -- 添加strategy_name字段
        ALTER TABLE backtest_results ADD COLUMN strategy_name VARCHAR(100);
    END IF;
    
    -- 检查strategy_version_id字段是否存在
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name='backtest_results' AND column_name='strategy_version_id'
    ) THEN
        -- 添加strategy_version_id字段
        ALTER TABLE backtest_results ADD COLUMN strategy_version_id INTEGER;
    END IF;
END
$$; 