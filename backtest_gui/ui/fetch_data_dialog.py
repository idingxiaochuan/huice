from PyQt5.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit,
                           QPushButton, QComboBox, QDateEdit, QProgressBar,
                           QMessageBox, QGroupBox, QFormLayout)
from PyQt5.QtCore import Qt, QDate
from datetime import datetime
from ..minute_data_fetcher import MinuteDataFetcher
from ..db.database import StockDatabase

class FetchDataDialog(QDialog):
    """获取行情数据对话框"""
    
    def __init__(self, parent=None, fund_code=None, data_level=None):
        super().__init__(parent)
        self.setWindowTitle("获取行情数据")
        self.resize(500, 300)
        self.db = StockDatabase()
        self.data_fetcher = None
        self.init_ui()
        
        # 如果提供了基金代码和数据级别，则预先填充
        if fund_code:
            self.fund_code_edit.setText(fund_code)
        if data_level:
            index = self.data_level_combo.findText(data_level)
            if index >= 0:
                self.data_level_combo.setCurrentIndex(index)
        
    def init_ui(self):
        """初始化界面"""
        main_layout = QVBoxLayout(self)
        
        # 参数设置组
        params_group = QGroupBox("参数设置")
        params_layout = QFormLayout(params_group)
        
        # 基金代码输入
        self.fund_code_edit = QLineEdit()
        params_layout.addRow("基金代码:", self.fund_code_edit)
        
        # 数据级别选择
        self.data_level_combo = QComboBox()
        self.data_level_combo.addItems(["1min", "5min", "day", "week", "month"])
        params_layout.addRow("数据级别:", self.data_level_combo)
        
        # 日期范围选择
        date_layout = QHBoxLayout()
        
        self.start_date_edit = QDateEdit(QDate.currentDate().addMonths(-1))
        self.start_date_edit.setDisplayFormat("yyyy-MM-dd")
        self.start_date_edit.setCalendarPopup(True)
        date_layout.addWidget(self.start_date_edit)
        
        date_layout.addWidget(QLabel("至"))
        
        self.end_date_edit = QDateEdit(QDate.currentDate())
        self.end_date_edit.setDisplayFormat("yyyy-MM-dd")
        self.end_date_edit.setCalendarPopup(True)
        date_layout.addWidget(self.end_date_edit)
        
        params_layout.addRow("日期范围:", date_layout)
        
        main_layout.addWidget(params_group)
        
        # 数据检查按钮
        check_layout = QHBoxLayout()
        self.check_btn = QPushButton("检查现有数据")
        self.check_btn.clicked.connect(self.check_existing_data)
        check_layout.addWidget(self.check_btn)
        check_layout.addStretch(1)
        main_layout.addLayout(check_layout)
        
        # 进度显示
        progress_group = QGroupBox("进度")
        progress_layout = QVBoxLayout(progress_group)
        
        self.status_label = QLabel("准备获取数据...")
        progress_layout.addWidget(self.status_label)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        progress_layout.addWidget(self.progress_bar)
        
        main_layout.addWidget(progress_group)
        
        # 按钮区域
        button_layout = QHBoxLayout()
        
        self.fetch_button = QPushButton("开始获取")
        self.fetch_button.clicked.connect(self.start_fetch)
        button_layout.addWidget(self.fetch_button)
        
        self.delete_button = QPushButton("删除数据")
        self.delete_button.clicked.connect(self.delete_data)
        button_layout.addWidget(self.delete_button)
        
        self.cancel_button = QPushButton("取消")
        self.cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(self.cancel_button)
        
        main_layout.addLayout(button_layout)
        
    def check_existing_data(self):
        """检查数据库中是否已存在相同基金、级别和时间段的数据"""
        # 获取参数
        fund_code = self.fund_code_edit.text().strip()
        data_level = self.data_level_combo.currentText()
        start_date = self.start_date_edit.date().toString("yyyyMMdd")
        end_date = self.end_date_edit.date().toString("yyyyMMdd")
        
        # 参数验证
        if not fund_code:
            QMessageBox.warning(self, "参数错误", "请输入基金代码")
            return
            
        try:
            # 将日期字符串转换为datetime对象
            start_dt = datetime.strptime(start_date, '%Y%m%d')
            end_dt = datetime.strptime(end_date, '%Y%m%d')
            
            # 转换为时间戳范围
            start_timestamp = int(start_dt.timestamp() * 1000)
            end_timestamp = int(end_dt.timestamp() * 1000) + 24*60*60*1000  # 加上一天
            
            # 查询数据库中该基金代码和数据级别在指定时间范围内的记录数
            query = f"""
            SELECT COUNT(*) as count FROM stock_quotes 
            WHERE fund_code = '{fund_code}' 
            AND data_level = '{data_level}'
            AND time >= {start_timestamp}
            AND time <= {end_timestamp}
            """
            
            result = self.db.execute_query(query)
            if result and len(result) > 0:
                count = result[0]['count']
                # 如果记录数大于0，说明已有数据
                if count > 0:
                    QMessageBox.information(self, "数据检查", f"数据库中已存在 {fund_code} {data_level} 在指定时间段的数据，共 {count} 条记录。")
                else:
                    QMessageBox.information(self, "数据检查", f"数据库中不存在 {fund_code} {data_level} 在指定时间段的数据。")
        except Exception as e:
            QMessageBox.warning(self, "检查错误", f"检查现有数据时出错: {str(e)}")
        
    def start_fetch(self):
        """开始获取数据"""
        # 获取参数
        fund_code = self.fund_code_edit.text().strip()
        data_level = self.data_level_combo.currentText()
        start_date = self.start_date_edit.date().toString("yyyyMMdd")
        end_date = self.end_date_edit.date().toString("yyyyMMdd")
        
        # 参数验证
        if not fund_code:
            QMessageBox.warning(self, "参数错误", "请输入基金代码")
            return
            
        # 创建数据获取线程
        self.data_fetcher = MinuteDataFetcher(fund_code, data_level, start_date, end_date, self)
        
        # 连接信号
        self.data_fetcher.update_signal.connect(self.update_status)
        self.data_fetcher.progress_signal.connect(self.progress_bar.setValue)
        self.data_fetcher.finished_signal.connect(self.on_fetch_finished)
        
        # 禁用按钮，避免重复操作
        self.fetch_button.setEnabled(False)
        self.delete_button.setEnabled(False)
        self.cancel_button.setText("停止")
        self.cancel_button.clicked.disconnect()
        self.cancel_button.clicked.connect(self.stop_fetch)
        
        # 开始获取
        self.data_fetcher.start()
        
    def delete_data(self):
        """删除数据"""
        # 获取参数
        fund_code = self.fund_code_edit.text().strip()
        data_level = self.data_level_combo.currentText()
        start_date = self.start_date_edit.date().toString("yyyyMMdd")
        end_date = self.end_date_edit.date().toString("yyyyMMdd")
        
        # 参数验证
        if not fund_code:
            QMessageBox.warning(self, "参数错误", "请输入基金代码")
            return
            
        # 确认删除
        reply = QMessageBox.question(
            self, 
            "确认删除", 
            f"确定要删除 {fund_code} {data_level} 在指定时间段的数据吗？此操作不可撤销。",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply != QMessageBox.Yes:
            return
            
        try:
            # 将日期字符串转换为datetime对象
            start_dt = datetime.strptime(start_date, '%Y%m%d')
            end_dt = datetime.strptime(end_date, '%Y%m%d')
            
            # 转换为时间戳范围
            start_timestamp = int(start_dt.timestamp() * 1000)
            end_timestamp = int(end_dt.timestamp() * 1000) + 24*60*60*1000  # 加上一天
            
            # 删除数据
            query = f"""
            DELETE FROM stock_quotes 
            WHERE fund_code = '{fund_code}' 
            AND data_level = '{data_level}'
            AND time >= {start_timestamp}
            AND time <= {end_timestamp}
            """
            
            affected_rows = self.db.execute_update(query)
            QMessageBox.information(self, "删除成功", f"成功删除 {affected_rows} 条记录。")
            
        except Exception as e:
            QMessageBox.warning(self, "删除错误", f"删除数据时出错: {str(e)}")
        
    def stop_fetch(self):
        """停止获取数据"""
        if self.data_fetcher and self.data_fetcher.isRunning():
            self.data_fetcher.terminate()
            self.data_fetcher.wait()
            
        self.update_status("数据获取已停止")
        self.reset_ui()
        
    def update_status(self, message):
        """更新状态信息"""
        self.status_label.setText(message)
        
    def on_fetch_finished(self, success):
        """数据获取完成的处理"""
        if success:
            QMessageBox.information(self, "完成", "数据获取成功")
        else:
            QMessageBox.warning(self, "错误", "数据获取失败")
            
        self.reset_ui()
        
    def reset_ui(self):
        """重置界面状态"""
        self.fetch_button.setEnabled(True)
        self.delete_button.setEnabled(True)
        self.cancel_button.setText("取消")
        self.cancel_button.clicked.disconnect()
        self.cancel_button.clicked.connect(self.reject)
        
    def closeEvent(self, event):
        """关闭窗口时的处理"""
        if self.data_fetcher and self.data_fetcher.isRunning():
            self.data_fetcher.terminate()
            self.data_fetcher.wait()
        event.accept() 