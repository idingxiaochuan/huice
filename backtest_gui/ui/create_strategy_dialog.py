from PyQt5.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit,
                           QPushButton, QComboBox, QTextEdit, QFormLayout,
                           QMessageBox, QGroupBox, QSpinBox, QDoubleSpinBox)
from PyQt5.QtCore import Qt
from ..db.database import StockDatabase
import json

class CreateStrategyDialog(QDialog):
    """创建波段策略对话框"""
    
    def __init__(self, fund_code, data_level, parent=None, strategy_id=None):
        super().__init__(parent)
        self.fund_code = fund_code
        self.data_level = data_level
        self.strategy_id = strategy_id  # 如果有ID，则是编辑模式
        self.db = StockDatabase()
        
        self.setWindowTitle("创建波段策略" if strategy_id is None else "编辑波段策略")
        self.resize(600, 500)
        self.init_ui()
        
        # 如果是编辑模式，加载策略数据
        if self.strategy_id:
            self.load_strategy()
        
    def init_ui(self):
        """初始化界面"""
        main_layout = QVBoxLayout(self)
        
        # 基本信息
        basic_group = QGroupBox("基本信息")
        basic_layout = QFormLayout(basic_group)
        
        # 基金代码
        self.fund_code_label = QLabel(self.fund_code)
        basic_layout.addRow("基金代码:", self.fund_code_label)
        
        # 数据级别
        self.data_level_label = QLabel(self.data_level)
        basic_layout.addRow("数据级别:", self.data_level_label)
        
        # 策略名称
        self.name_edit = QLineEdit()
        basic_layout.addRow("策略名称:", self.name_edit)
        
        # 策略描述
        self.description_edit = QTextEdit()
        self.description_edit.setMaximumHeight(100)
        basic_layout.addRow("策略描述:", self.description_edit)
        
        main_layout.addWidget(basic_group)
        
        # 策略参数
        params_group = QGroupBox("策略参数")
        params_layout = QFormLayout(params_group)
        
        # 短周期
        self.short_period_spin = QSpinBox()
        self.short_period_spin.setRange(1, 100)
        self.short_period_spin.setValue(5)
        params_layout.addRow("短周期:", self.short_period_spin)
        
        # 长周期
        self.long_period_spin = QSpinBox()
        self.long_period_spin.setRange(5, 200)
        self.long_period_spin.setValue(20)
        params_layout.addRow("长周期:", self.long_period_spin)
        
        # 买入阈值
        self.buy_threshold_spin = QDoubleSpinBox()
        self.buy_threshold_spin.setRange(0, 100)
        self.buy_threshold_spin.setValue(5)
        self.buy_threshold_spin.setSingleStep(0.1)
        params_layout.addRow("买入阈值(%):", self.buy_threshold_spin)
        
        # 卖出阈值
        self.sell_threshold_spin = QDoubleSpinBox()
        self.sell_threshold_spin.setRange(0, 100)
        self.sell_threshold_spin.setValue(3)
        self.sell_threshold_spin.setSingleStep(0.1)
        params_layout.addRow("卖出阈值(%):", self.sell_threshold_spin)
        
        main_layout.addWidget(params_group)
        
        # 按钮区域
        button_layout = QHBoxLayout()
        button_layout.addStretch(1)
        
        self.save_btn = QPushButton("保存")
        self.save_btn.clicked.connect(self.save_strategy)
        
        self.cancel_btn = QPushButton("取消")
        self.cancel_btn.clicked.connect(self.reject)
        
        button_layout.addWidget(self.save_btn)
        button_layout.addWidget(self.cancel_btn)
        
        main_layout.addLayout(button_layout)
        
    def load_strategy(self):
        """加载策略数据"""
        strategy = self.db.get_wave_strategy(self.strategy_id)
        if not strategy:
            QMessageBox.warning(self, "错误", "找不到指定的策略")
            self.reject()
            return
            
        # 设置基本信息
        self.name_edit.setText(strategy['name'])
        self.description_edit.setText(strategy['description'] or "")
        
        # 设置策略参数
        try:
            params = json.loads(strategy['strategy_params'] or "{}")
            
            if 'short_period' in params:
                self.short_period_spin.setValue(params['short_period'])
                
            if 'long_period' in params:
                self.long_period_spin.setValue(params['long_period'])
                
            if 'buy_threshold' in params:
                self.buy_threshold_spin.setValue(params['buy_threshold'])
                
            if 'sell_threshold' in params:
                self.sell_threshold_spin.setValue(params['sell_threshold'])
        except Exception as e:
            print(f"加载策略参数错误: {str(e)}")
        
    def save_strategy(self):
        """保存策略"""
        # 获取输入
        name = self.name_edit.text().strip()
        description = self.description_edit.toPlainText().strip()
        
        # 验证输入
        if not name:
            QMessageBox.warning(self, "输入错误", "请输入策略名称")
            return
            
        # 构建策略参数
        params = {
            'short_period': self.short_period_spin.value(),
            'long_period': self.long_period_spin.value(),
            'buy_threshold': self.buy_threshold_spin.value(),
            'sell_threshold': self.sell_threshold_spin.value()
        }
        
        params_json = json.dumps(params)
        
        try:
            if self.strategy_id:
                # 更新策略
                success = self.db.update_wave_strategy(
                    self.strategy_id, 
                    name=name, 
                    description=description, 
                    strategy_params=params_json
                )
                
                if success:
                    QMessageBox.information(self, "成功", "策略更新成功")
                    self.accept()
                else:
                    QMessageBox.warning(self, "错误", "策略更新失败")
            else:
                # 创建新策略
                strategy_id = self.db.save_wave_strategy(
                    name, 
                    description, 
                    self.fund_code, 
                    self.data_level, 
                    params_json
                )
                
                if strategy_id > 0:
                    QMessageBox.information(self, "成功", "策略创建成功")
                    self.accept()
                else:
                    QMessageBox.warning(self, "错误", "策略创建失败")
        except Exception as e:
            QMessageBox.warning(self, "错误", f"保存策略时发生错误: {str(e)}") 