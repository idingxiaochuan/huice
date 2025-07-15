from PyQt5.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                           QSplitter, QTabWidget, QAction, QMenu, QStatusBar,
                           QTableWidget, QTableWidgetItem, QHeaderView, QGroupBox,
                           QPushButton, QLabel, QTextEdit)
from PyQt5.QtCore import Qt, pyqtSlot
from .fund_list_widget import FundListWidget
from .strategy_list_widget import StrategyListWidget
from .fetch_data_dialog import FetchDataDialog
from .create_strategy_dialog import CreateStrategyDialog
# 导入其他必要的组件...

class MainWindow(QMainWindow):
    """主窗口"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("交易系统")
        self.resize(1200, 800)
        self.init_ui()
        
    def init_ui(self):
        """初始化界面"""
        # 创建中央窗口
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)
        
        # 创建左侧菜单和右侧内容区域
        main_splitter = QSplitter(Qt.Horizontal)
        
        # 左侧菜单
        left_menu = self.create_left_menu()
        
        # 右侧内容区域 - 波段策略管理
        right_content = self.create_right_content()
        
        # 将左右面板添加到主分割器
        main_splitter.addWidget(left_menu)
        main_splitter.addWidget(right_content)
        main_splitter.setSizes([200, 1000])  # 设置左右比例
        
        main_layout.addWidget(main_splitter)
        
        # 创建状态栏
        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)
        self.statusBar.showMessage("就绪")
        
        # 创建菜单栏
        self.create_menus()
        
    def create_left_menu(self):
        """创建左侧菜单"""
        left_menu = QWidget()
        layout = QVBoxLayout(left_menu)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # 添加菜单项
        menu_items = [
            "我的交易",
            "行情回测",
            "数据分析",
            "数据整理"
        ]
        
        for item in menu_items:
            btn = QPushButton(item)
            btn.setStyleSheet("text-align: left; padding: 10px; font-size: 14px;")
            layout.addWidget(btn)
            
            # 连接信号
            if item == "数据整理":
                btn.setStyleSheet("text-align: left; padding: 10px; font-size: 14px; background-color: #e0e0e0;")
        
        layout.addStretch(1)  # 添加弹性空间
        
        return left_menu
        
    def create_right_content(self):
        """创建右侧内容区域"""
        right_content = QTabWidget()
        right_content.setTabPosition(QTabWidget.North)
        
        # 波段策略管理标签页
        wave_strategy_tab = QWidget()
        wave_layout = QVBoxLayout(wave_strategy_tab)
        
        # 标题
        title_layout = QHBoxLayout()
        title_layout.addWidget(QLabel("<h2>波段策略管理</h2>"))
        wave_layout.addLayout(title_layout)
        
        # 创建上下分割区域
        content_splitter = QSplitter(Qt.Vertical)
        
        # 上半部分 - 基金选择和波段策略列表
        upper_widget = QWidget()
        upper_layout = QVBoxLayout(upper_widget)
        upper_layout.setContentsMargins(0, 0, 0, 0)
        
        # 基金选择区域
        self.fund_list_widget = FundListWidget()
        upper_layout.addWidget(self.fund_list_widget)
        
        # 操作按钮区域
        button_layout = QHBoxLayout()
        
        # 获取行情按钮
        self.fetch_data_btn = QPushButton("获取行情数据")
        self.fetch_data_btn.clicked.connect(self.show_fetch_data_dialog)
        button_layout.addWidget(self.fetch_data_btn)
        
        # 删除行情按钮
        self.delete_data_btn = QPushButton("删除行情数据")
        self.delete_data_btn.clicked.connect(self.show_delete_data_dialog)
        button_layout.addWidget(self.delete_data_btn)
        
        button_layout.addStretch(1)
        
        upper_layout.addLayout(button_layout)
        
        # 波段策略列表
        strategy_layout = QHBoxLayout()
        
        # 波段策略列表
        strategy_group = QGroupBox("波段策略")
        strategy_group_layout = QVBoxLayout(strategy_group)
        
        self.strategy_table = QTableWidget()
        self.strategy_table.setColumnCount(4)
        self.strategy_table.setHorizontalHeaderLabels(["名称", "描述", "创建时间", "操作"])
        self.strategy_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        strategy_group_layout.addWidget(self.strategy_table)
        
        strategy_layout.addWidget(strategy_group)
        
        upper_layout.addLayout(strategy_layout)
        
        # 策略按钮区域
        strategy_button_layout = QHBoxLayout()
        strategy_button_layout.addStretch(1)
        
        self.create_btn = QPushButton("创建策略")
        self.create_btn.clicked.connect(self.show_create_strategy_dialog)
        
        self.edit_btn = QPushButton("编辑策略")
        self.edit_btn.clicked.connect(self.show_edit_strategy_dialog)
        self.edit_btn.setEnabled(False)  # 初始禁用，直到选择策略
        
        self.delete_btn = QPushButton("删除策略")
        self.delete_btn.clicked.connect(self.delete_strategy)
        self.delete_btn.setEnabled(False)  # 初始禁用，直到选择策略
        
        strategy_button_layout.addWidget(self.create_btn)
        strategy_button_layout.addWidget(self.edit_btn)
        strategy_button_layout.addWidget(self.delete_btn)
        
        upper_layout.addLayout(strategy_button_layout)
        
        # 下半部分 - 状态显示
        lower_widget = QWidget()
        lower_layout = QVBoxLayout(lower_widget)
        lower_layout.setContentsMargins(0, 0, 0, 0)
        
        # 行情状态
        status_group = QGroupBox("行情数据状态")
        status_layout = QVBoxLayout(status_group)
        
        self.status_text = QTextEdit()
        self.status_text.setReadOnly(True)
        self.status_text.setMaximumHeight(150)
        status_layout.addWidget(self.status_text)
        
        lower_layout.addWidget(status_group)
        
        # 添加到分割器
        content_splitter.addWidget(upper_widget)
        content_splitter.addWidget(lower_widget)
        content_splitter.setSizes([600, 200])  # 设置上下比例
        
        wave_layout.addWidget(content_splitter)
        
        # 添加到标签页
        right_content.addTab(wave_strategy_tab, "波段策略管理")
        
        # 连接信号
        self.fund_list_widget.fund_selected_signal.connect(self.on_fund_selected)
        self.strategy_table.itemSelectionChanged.connect(self.on_strategy_selection_changed)
        
        return right_content
        
    def create_menus(self):
        """创建菜单栏"""
        # 文件菜单
        file_menu = self.menuBar().addMenu("文件")
        
        # 添加菜单项
        exit_action = QAction("退出", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # 数据菜单
        data_menu = self.menuBar().addMenu("数据")
        
        # 添加获取行情数据菜单项
        fetch_data_action = QAction("获取行情数据", self)
        fetch_data_action.triggered.connect(self.show_fetch_data_dialog)
        data_menu.addAction(fetch_data_action)
        
        # 策略菜单
        strategy_menu = self.menuBar().addMenu("策略")
        
        # 添加创建策略菜单项
        create_strategy_action = QAction("创建波段策略", self)
        create_strategy_action.triggered.connect(self.show_create_strategy_dialog)
        strategy_menu.addAction(create_strategy_action)
        
        # 帮助菜单
        help_menu = self.menuBar().addMenu("帮助")
        
        # 添加关于菜单项
        about_action = QAction("关于", self)
        about_action.triggered.connect(self.show_about_dialog)
        help_menu.addAction(about_action)
        
    def on_strategy_selection_changed(self):
        """当策略选择变化时的处理"""
        selected_items = self.strategy_table.selectedItems()
        has_selection = len(selected_items) > 0
        
        # 启用/禁用编辑和删除按钮
        self.edit_btn.setEnabled(has_selection)
        self.delete_btn.setEnabled(has_selection)
        
    @pyqtSlot(str, str)
    def on_fund_selected(self, fund_code, data_level):
        """
        当基金和数据级别被选择时的处理
        
        Args:
            fund_code: 基金代码
            data_level: 数据级别
        """
        # 更新波段策略表格
        self.update_strategy_table(fund_code, data_level)
        
        # 更新状态文本
        self.update_status(fund_code, data_level)
        
        self.statusBar.showMessage(f"已选择基金: {fund_code}, 数据级别: {data_level}")
        
    def update_status(self, fund_code, data_level):
        """更新状态文本"""
        try:
            # 查询该基金该级别的数据范围
            query = f"""
            SELECT MIN(time) as min_time, MAX(time) as max_time, COUNT(*) as count
            FROM stock_quotes
            WHERE fund_code = '{fund_code}' 
            AND data_level = '{data_level}'
            """
            
            from ..db.database import StockDatabase
            db = StockDatabase()
            results = db.execute_query(query)
            
            if results and results[0]['count'] > 0:
                min_time = results[0]['min_time']
                max_time = results[0]['max_time']
                count = results[0]['count']
                
                # 转换时间戳为可读格式
                from ..utils.time_utils import convert_timestamp_to_datetime, format_datetime
                min_date = format_datetime(convert_timestamp_to_datetime(min_time))
                max_date = format_datetime(convert_timestamp_to_datetime(max_time))
                
                status = f"[{min_date}] 数据库未连接，无法加载基金列表\n"
                status += f"[{min_date}] 数据库未结构化检查并创建\n"
                status += f"[{min_date}] 数据库连接成功\n"
                status += f"[{min_date}] 已加载基金列表\n"
                status += f"[{max_date}] 选择基金: {fund_code}.SH - {fund_code}.SH [{data_level}, 5min, day, month, week]\n"
                status += f"[{max_date}] 加载基金 {fund_code}.SH 的波段策略"
                
                self.status_text.setText(status)
            else:
                self.status_text.setText(f"未找到 {fund_code} {data_level} 级别的数据")
        except Exception as e:
            print(f"更新状态错误: {str(e)}")
            self.status_text.setText(f"更新状态错误: {str(e)}")
        
    def update_strategy_table(self, fund_code, data_level):
        """
        更新波段策略表格
        
        Args:
            fund_code: 基金代码
            data_level: 数据级别
        """
        try:
            # 清空表格
            self.strategy_table.setRowCount(0)
            
            # 查询该基金和数据级别的所有波段策略
            query = f"""
            SELECT id, name, description, created_at 
            FROM wave_strategies 
            WHERE fund_code = '{fund_code}' 
            AND data_level = '{data_level}'
            ORDER BY name
            """
            
            from ..db.database import StockDatabase
            db = StockDatabase()
            results = db.execute_query(query)
            
            if results and len(results) > 0:
                self.strategy_table.setRowCount(len(results))
                
                for row_idx, row in enumerate(results):
                    strategy_id = row['id']
                    strategy_name = row['name']
                    strategy_desc = row['description'] or ""
                    created_at = row['created_at']
                    
                    # 设置单元格数据
                    name_item = QTableWidgetItem(strategy_name)
                    name_item.setData(Qt.UserRole, strategy_id)  # 存储策略ID
                    
                    self.strategy_table.setItem(row_idx, 0, name_item)
                    self.strategy_table.setItem(row_idx, 1, QTableWidgetItem(strategy_desc))
                    self.strategy_table.setItem(row_idx, 2, QTableWidgetItem(str(created_at)))
                    
                    # 操作按钮 - 这里可以添加编辑、删除按钮
                    # 为简化起见，我们使用表格选择和外部按钮来操作
            else:
                # 如果没有数据，显示提示行
                self.strategy_table.setRowCount(1)
                self.strategy_table.setSpan(0, 0, 1, 4)
                self.strategy_table.setItem(0, 0, QTableWidgetItem("当前基金和数据级别没有波段策略"))
        except Exception as e:
            print(f"加载波段策略错误: {str(e)}")
            self.strategy_table.setRowCount(1)
            self.strategy_table.setSpan(0, 0, 1, 4)
            self.strategy_table.setItem(0, 0, QTableWidgetItem(f"加载波段策略错误: {str(e)}"))
            
    def get_selected_strategy_id(self):
        """获取当前选中的策略ID"""
        selected_items = self.strategy_table.selectedItems()
        if not selected_items:
            return None
            
        # 获取第一个选中的单元格所在行的第一列单元格
        row = selected_items[0].row()
        item = self.strategy_table.item(row, 0)
        
        if item:
            return item.data(Qt.UserRole)
        return None
            
    def show_fetch_data_dialog(self):
        """显示获取行情数据对话框"""
        fund_code = self.fund_list_widget.get_selected_fund()
        data_level = self.fund_list_widget.get_selected_level()
        
        dialog = FetchDataDialog(self, fund_code, data_level)
        if dialog.exec_():
            # 对话框关闭后刷新基金列表
            self.fund_list_widget.load_funds()
            
            # 如果当前有选择的基金和级别，刷新状态
            if fund_code and data_level:
                self.update_status(fund_code, data_level)
    
    def show_delete_data_dialog(self):
        """显示删除行情数据对话框"""
        fund_code = self.fund_list_widget.get_selected_fund()
        data_level = self.fund_list_widget.get_selected_level()
        
        dialog = FetchDataDialog(self, fund_code, data_level)
        dialog.setWindowTitle("删除行情数据")
        if dialog.exec_():
            # 对话框关闭后刷新基金列表
            self.fund_list_widget.load_funds()
            
            # 如果当前有选择的基金和级别，刷新状态
            if fund_code and data_level:
                self.update_status(fund_code, data_level)
        
    def show_create_strategy_dialog(self):
        """显示创建波段策略对话框"""
        fund_code = self.fund_list_widget.get_selected_fund()
        data_level = self.fund_list_widget.get_selected_level()
        
        if fund_code and data_level:
            dialog = CreateStrategyDialog(fund_code, data_level, self)
            if dialog.exec_():
                # 对话框关闭后刷新策略列表
                self.update_strategy_table(fund_code, data_level)
        else:
            self.statusBar.showMessage("请先选择基金和数据级别")
            
    def show_edit_strategy_dialog(self):
        """显示编辑波段策略对话框"""
        fund_code = self.fund_list_widget.get_selected_fund()
        data_level = self.fund_list_widget.get_selected_level()
        strategy_id = self.get_selected_strategy_id()
        
        if fund_code and data_level and strategy_id:
            dialog = CreateStrategyDialog(fund_code, data_level, self, strategy_id)
            if dialog.exec_():
                # 对话框关闭后刷新策略列表
                self.update_strategy_table(fund_code, data_level)
        else:
            self.statusBar.showMessage("请先选择要编辑的策略")
            
    def delete_strategy(self):
        """删除选中的波段策略"""
        from PyQt5.QtWidgets import QMessageBox
        
        fund_code = self.fund_list_widget.get_selected_fund()
        data_level = self.fund_list_widget.get_selected_level()
        strategy_id = self.get_selected_strategy_id()
        
        if not strategy_id:
            self.statusBar.showMessage("请先选择要删除的策略")
            return
            
        # 确认删除
        reply = QMessageBox.question(
            self, 
            "确认删除", 
            "确定要删除选中的策略吗？此操作不可撤销。",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            from ..db.database import StockDatabase
            db = StockDatabase()
            
            if db.delete_wave_strategy(strategy_id):
                self.statusBar.showMessage("策略删除成功")
                # 刷新策略列表
                self.update_strategy_table(fund_code, data_level)
            else:
                self.statusBar.showMessage("策略删除失败")
        
    def show_about_dialog(self):
        """显示关于对话框"""
        # 在这里实现关于对话框
        self.statusBar.showMessage("关于交易系统") 