"""
应用程序配置文件
"""

# 数据库配置
DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'huice'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'

# 文件存储路径
DATA_DIR = './data'
LOG_DIR = './logs'

# 功能开关
SAVE_TO_CSV = True
SAVE_TO_DB = True

# 初始资金设置
INITIAL_FUND = 10000000  # 1000万初始资金 