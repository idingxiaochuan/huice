from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QListWidget, QListWidgetItem, 
                           QLabel, QComboBox, QPushButton, QGroupBox, QSplitter)
from PyQt5.QtCore import Qt, pyqtSignal
from ..db.database import StockDatabase

class FundListWidget(QWidget):
    """
    基金列表组件，左侧展示基金名称列表，右侧用下拉框展示所选基金的数据级别
    当选择基金和数据级别时，发出信号通知主界面更新波段策略列表
    """
    fund_selected_signal = pyqtSignal(str, str)  # 基金代码, 数据级别
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.db = StockDatabase()
        self.init_ui()
        self.load_funds()
        
    def init_ui(self):
        """初始化界面"""
        main_layout = QVBoxLayout(self)
        
        # 基金选择区域
        fund_group = QGroupBox("基金选择")
        fund_layout = QVBoxLayout(fund_group)
        
        # 基金代码输入区域
        input_layout = QHBoxLayout()
        input_layout.addWidget(QLabel("基金代码:"))
        self.fund_code_edit = QComboBox()
        self.fund_code_edit.setEditable(True)
        self.fund_code_edit.setMinimumWidth(200)
        input_layout.addWidget(self.fund_code_edit)
        
        # 查询按钮
        self.query_btn = QPushButton("查询")
        input_layout.addWidget(self.query_btn)
        
        fund_layout.addLayout(input_layout)
        
        # 创建水平分割器
        splitter = QSplitter(Qt.Horizontal)
        
        # 左侧基金列表
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(0, 0, 0, 0)
        
        self.fund_list = QListWidget()
        self.fund_list.setSelectionMode(QListWidget.SingleSelection)
        left_layout.addWidget(self.fund_list)
        
        # 右侧数据级别
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        right_layout.setContentsMargins(0, 0, 0, 0)
        
        right_layout.addWidget(QLabel("数据级别:"))
        self.level_combo = QComboBox()
        self.level_combo.setEnabled(False)  # 初始禁用，直到选择基金
        right_layout.addWidget(self.level_combo)
        right_layout.addStretch(1)
        
        # 添加到分割器
        splitter.addWidget(left_widget)
        splitter.addWidget(right_widget)
        splitter.setSizes([200, 100])  # 设置初始大小比例
        
        fund_layout.addWidget(splitter)
        
        main_layout.addWidget(fund_group)
        
        # 连接信号
        self.query_btn.clicked.connect(self.on_query_fund)
        self.fund_code_edit.currentTextChanged.connect(self.on_fund_code_changed)
        self.fund_list.itemClicked.connect(self.on_fund_selected)
        self.level_combo.currentTextChanged.connect(self.on_level_changed)
        
    def load_funds(self):
        """从数据库加载基金列表"""
        try:
            # 清空现有列表
            self.fund_code_edit.clear()
            self.fund_list.clear()
            self.level_combo.clear()
            self.level_combo.setEnabled(False)
            
            # 查询所有不同的基金代码
            query = "SELECT DISTINCT fund_code FROM stock_quotes ORDER BY fund_code"
            results = self.db.execute_query(query)
            
            if results:
                fund_codes = []
                
                for row in results:
                    fund_code = row['fund_code']
                    fund_codes.append(fund_code)
                    
                    # 添加到基金列表
                    item = QListWidgetItem(f"{fund_code}")
                    self.fund_list.addItem(item)
                
                # 添加到下拉框
                self.fund_code_edit.addItems(fund_codes)
        except Exception as e:
            print(f"加载基金列表错误: {str(e)}")
    
    def on_fund_code_changed(self, text):
        """当基金代码输入框内容变化时，过滤列表"""
        if not text:
            return
            
        # 查找匹配项
        items = self.fund_list.findItems(text, Qt.MatchStartsWith)
        if items:
            # 选中第一个匹配项
            self.fund_list.setCurrentItem(items[0])
            self.on_fund_selected(items[0])
    
    def on_query_fund(self):
        """查询按钮点击处理"""
        fund_code = self.fund_code_edit.currentText().strip()
        if not fund_code:
            return
            
        # 查找匹配项
        items = self.fund_list.findItems(fund_code, Qt.MatchStartsWith)
        if items:
            # 选中第一个匹配项
            self.fund_list.setCurrentItem(items[0])
            self.on_fund_selected(items[0])
        else:
            # 如果没找到，添加到列表
            item = QListWidgetItem(fund_code)
            self.fund_list.addItem(item)
            self.fund_list.setCurrentItem(item)
            self.on_fund_selected(item)
            
    def on_fund_selected(self, item):
        """当选择基金时，加载该基金的数据级别"""
        fund_code = item.text()
        try:
            # 清空现有级别列表
            self.level_combo.clear()
            self.level_combo.setEnabled(True)
            
            # 查询该基金的所有不同数据级别
            query = f"SELECT DISTINCT data_level FROM stock_quotes WHERE fund_code = '{fund_code}' ORDER BY data_level"
            results = self.db.execute_query(query)
            
            if results:
                for row in results:
                    data_level = row['data_level']
                    self.level_combo.addItem(data_level)
                    
                # 如果有数据级别，自动选择第一个并发出信号
                if self.level_combo.count() > 0:
                    data_level = self.level_combo.itemText(0)
                    self.fund_selected_signal.emit(fund_code, data_level)
            else:
                self.level_combo.setEnabled(False)
        except Exception as e:
            print(f"加载数据级别错误: {str(e)}")
            
    def on_level_changed(self, data_level):
        """当选择数据级别时，发出信号"""
        if data_level and self.fund_list.currentItem():
            fund_code = self.fund_list.currentItem().text()
            self.fund_selected_signal.emit(fund_code, data_level)
            
    def get_selected_fund(self):
        """获取当前选中的基金代码"""
        if self.fund_list.currentItem():
            return self.fund_list.currentItem().text()
        return None
        
    def get_selected_level(self):
        """获取当前选中的数据级别"""
        if self.level_combo.isEnabled():
            return self.level_combo.currentText()
        return None 