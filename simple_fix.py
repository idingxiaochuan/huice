#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
简单修复脚本 - 直接修改交易报告窗口的源代码文件
"""
import os
import sys
import traceback
import shutil

def main():
    """主函数"""
    try:
        # 源文件路径
        source_file = "backtest_gui/gui/trade_report_window.py"
        
        # 备份文件路径
        backup_file = "backtest_gui/gui/trade_report_window.py.bak"
        
        # 检查文件是否存在
        if not os.path.exists(source_file):
            print(f"错误: 源文件 {source_file} 不存在")
            return
            
        # 创建备份
        print(f"创建备份文件: {backup_file}")
        shutil.copy2(source_file, backup_file)
        print("备份创建成功")
        
        # 读取源文件
        print(f"读取源文件: {source_file}")
        with open(source_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # 修改内容
        print("修改文件内容...")
        
        # 1. 修改按钮初始化代码
        content = content.replace(
            'self.xirr_button.setEnabled(False)  # 默认禁用',
            'self.xirr_button.setEnabled(True)  # 修改：默认启用'
        )
        
        content = content.replace(
            'self.export_excel_button.setEnabled(False)  # 默认禁用',
            'self.export_excel_button.setEnabled(True)  # 修改：默认启用'
        )
        
        # 2. 修改on_xirr_clicked方法
        content = content.replace(
            'if not self.current_backtest_id:',
            '# 强制启用按钮\nself.xirr_button.setEnabled(True)\nself.export_excel_button.setEnabled(True)\n\nif not self.current_backtest_id:'
        )
        
        # 3. 在on_query_clicked方法末尾添加按钮启用代码
        content = content.replace(
            '        except Exception as e:\n            print(f"查询过程中出错: {str(e)}")\n            traceback.print_exc()\n            QMessageBox.warning(self, "查询错误", f"查询过程中发生错误: {str(e)}")',
            '        except Exception as e:\n            print(f"查询过程中出错: {str(e)}")\n            traceback.print_exc()\n            QMessageBox.warning(self, "查询错误", f"查询过程中发生错误: {str(e)}")\n        \n        # 强制启用按钮\n        self.xirr_button.setEnabled(True)\n        self.export_excel_button.setEnabled(True)'
        )
        
        # 写入修改后的内容
        print(f"写入修改后的内容到: {source_file}")
        with open(source_file, 'w', encoding='utf-8') as f:
            f.write(content)
            
        print("修改成功！")
        print(f"如需恢复原始文件，请将 {backup_file} 复制回 {source_file}")
        
    except Exception as e:
        print(f"修复过程中出错: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main() 