from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QListWidget, QListWidgetItem, 
                           QLabel, QPushButton, QGroupBox)
from PyQt5.QtCore import Qt, pyqtSignal
from ..db.database import StockDatabase

class StrategyListWidget(QWidget):
    """
    波段策略列表组件，根据选择的基金和数据级别显示对应的波段策略
    """
    strategy_selected_signal = pyqtSignal(str)  # 策略ID
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.db = StockDatabase()
        self.current_fund = None
        self.current_level = None
        self.init_ui()
        
    def init_ui(self):
        """初始化界面"""
        main_layout = QVBoxLayout(self)
        
        # 标题
        self.title_label = QLabel("波段策略列表")
        main_layout.addWidget(self.title_label)
        
        # 策略列表
        self.strategy_group = QGroupBox("波段策略")
        strategy_layout = QVBoxLayout(self.strategy_group)
        self.strategy_list = QListWidget()
        self.strategy_list.setSelectionMode(QListWidget.SingleSelection)
        strategy_layout.addWidget(self.strategy_list)
        
        main_layout.addWidget(self.strategy_group)
        
        # 无策略提示
        self.no_strategy_label = QLabel("当前基金和数据级别没有波段策略")
        self.no_strategy_label.setAlignment(Qt.AlignCenter)
        self.no_strategy_label.setVisible(False)
        main_layout.addWidget(self.no_strategy_label)
        
        # 创建新策略按钮
        self.create_btn = QPushButton("创建新策略")
        main_layout.addWidget(self.create_btn)
        
        # 连接信号
        self.strategy_list.itemClicked.connect(self.on_strategy_selected)
        
    def update_strategies(self, fund_code, data_level):
        """
        更新波段策略列表
        
        Args:
            fund_code: 基金代码
            data_level: 数据级别
        """
        self.current_fund = fund_code
        self.current_level = data_level
        
        # 更新标题
        self.title_label.setText(f"波段策略列表 - {fund_code} ({data_level})")
        
        try:
            # 清空现有列表
            self.strategy_list.clear()
            
            # 查询该基金和数据级别的所有波段策略
            query = f"""
            SELECT id, name, description 
            FROM wave_strategies 
            WHERE fund_code = '{fund_code}' 
            AND data_level = '{data_level}'
            ORDER BY name
            """
            results = self.db.execute_query(query)
            
            if results and len(results) > 0:
                for row in results:
                    strategy_id = row['id']
                    strategy_name = row['name']
                    strategy_desc = row['description'] or ""
                    
                    item = QListWidgetItem(f"{strategy_name} - {strategy_desc}")
                    item.setData(Qt.UserRole, strategy_id)
                    self.strategy_list.addItem(item)
                    
                self.strategy_group.setVisible(True)
                self.no_strategy_label.setVisible(False)
            else:
                self.strategy_group.setVisible(False)
                self.no_strategy_label.setVisible(True)
        except Exception as e:
            print(f"加载波段策略错误: {str(e)}")
            self.strategy_group.setVisible(False)
            self.no_strategy_label.setVisible(True)
            
    def on_strategy_selected(self, item):
        """当选择策略时，发出信号"""
        strategy_id = item.data(Qt.UserRole)
        self.strategy_selected_signal.emit(strategy_id)
        
    def get_selected_strategy(self):
        """获取当前选中的策略ID"""
        if self.strategy_list.currentItem():
            return self.strategy_list.currentItem().data(Qt.UserRole)
        return None 