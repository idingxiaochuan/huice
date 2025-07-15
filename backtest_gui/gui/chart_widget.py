#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
图表控件模块 - 用于显示K线图和交易标记
"""
import matplotlib
matplotlib.use('Qt5Agg')  # 使用Qt5后端

import pandas as pd
import numpy as np
from matplotlib.figure import Figure
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
from PyQt5.QtWidgets import QVBoxLayout, QWidget, QSizePolicy

class ChartWidget(QWidget):
    """K线图表控件"""
    
    def __init__(self):
        """初始化图表控件"""
        super().__init__()
        
        # 初始化数据存储
        self.price_data = pd.DataFrame()
        self.nav_data = pd.DataFrame()  # 净值数据（保留但不使用）
        self.buy_points = []  # 买入点列表
        self.sell_points = []  # 卖出点列表
        
        # 初始化UI组件
        self.figure = None
        self.canvas = None
        self.toolbar = None
        self.price_ax = None
        
        # 设置显示模式
        self.show_price = True  # 是否显示价格曲线
        
        # 设置UI
        self._setup_ui()
        
    def _setup_ui(self):
        """设置用户界面"""
        # 创建图表和画布
        self.figure = Figure(figsize=(8, 6), dpi=100)
        self.canvas = FigureCanvas(self.figure)
        
        # 创建工具栏但默认不显示
        self.toolbar = NavigationToolbar(self.canvas, self)
        self.toolbar.setVisible(False)  # 默认隐藏工具栏
        
        # 创建布局
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)  # 移除边距
        layout.addWidget(self.toolbar)
        layout.addWidget(self.canvas)
        
        # 设置尺寸策略，使图表可以水平拉伸
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.canvas.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        
        # 初始化图表 - 只创建价格图表
        self.price_ax = self.figure.add_subplot(111)  # 只有一个子图
        
        # 去掉标题
        # self.price_ax.set_title('价格走势图')
        
        # 移除边框
        for spine in self.price_ax.spines.values():
            spine.set_visible(False)
        
        self.figure.tight_layout()
        
        # 连接画布大小变化信号
        self.canvas.mpl_connect('resize_event', self.on_resize)
        
    def on_resize(self, event):
        """处理画布大小变化事件"""
        # 当画布大小变化时，调整图表布局
        if self.figure:
            self.figure.tight_layout()
            
    def update_chart(self, new_data):
        """更新价格图表数据
        
        Args:
            new_data: 新的价格数据，包含时间、开盘价、最高价、最低价、收盘价
        """
        if not self.show_price:
            return
            
        # 更新数据
        if self.price_data.empty:
            self.price_data = new_data
        else:
            # 避免重复数据
            new_indices = ~new_data.index.isin(self.price_data.index)
            if any(new_indices):
                new_unique_data = new_data[new_indices]
                self.price_data = pd.concat([self.price_data, new_unique_data])
            
        # 清空当前图表
        self.price_ax.clear()
        
        # 如果没有数据，只显示空图表
        if self.price_data.empty or len(self.price_data) == 0:
            # 不使用set_title，避免小黑框
            self.price_ax.text(0.5, 0.98, '价格走势图', 
                             transform=self.price_ax.transAxes,
                             ha='center', va='top',
                             fontsize=10, color='black')
            self.price_ax.set_xlabel('时间')
            self.price_ax.set_ylabel('价格')
            self.price_ax.grid(True)
            self.figure.tight_layout()
            self.canvas.draw()
            return
        
        # 绘制K线图
        times = self.price_data.index
        closes = self.price_data['close'].values
        
        # 绘制收盘价曲线
        self.price_ax.plot(range(len(times)), closes, 'b-', linewidth=1.5, label='收盘价')
        
        # 绘制买入点和卖出点
        for point in self.buy_points:
            if point['time'] in self.price_data.index:
                idx = self.price_data.index.get_loc(point['time'])
                self.price_ax.plot(idx, point['price'], '^', color='red', markersize=8)
        
        for point in self.sell_points:
            if point['time'] in self.price_data.index:
                idx = self.price_data.index.get_loc(point['time'])
                self.price_ax.plot(idx, point['price'], 'v', color='green', markersize=8)
        
        # 设置图表属性 - 不使用set_title，避免小黑框
        # 清除所有现有的文本对象，避免小黑框
        for text in self.price_ax.texts:
            text.remove()
            
        # 移除所有图例对象，避免小黑框
        if self.price_ax.get_legend():
            self.price_ax.get_legend().remove()
        
        # 移除所有注释对象，避免小黑框
        for child in self.price_ax.get_children():
            if hasattr(child, 'get_boxstyle'):
                child.remove()
            
        # 添加自定义标题文本
        self.price_ax.text(0.5, 0.98, '价格走势图', 
                         transform=self.price_ax.transAxes,
                         ha='center', va='top',
                         fontsize=10, color='black')
        self.price_ax.set_ylabel('价格')
        self.price_ax.set_xlabel('时间')
        self.price_ax.grid(True)
        
        # 创建自定义图例而不是使用legend()方法
        self.price_ax.text(0.98, 0.98, '收盘价', 
                         transform=self.price_ax.transAxes,
                         ha='right', va='top',
                         fontsize=9, color='blue')
        
        # 设置x轴刻度
        if len(times) > 0:
            # 只设置三个刻度：左边界、中间和右边界
            tick_indices = [0]  # 左边界
            
            # 添加中间点
            if len(times) > 2:
                middle_idx = len(times) // 2
                tick_indices.append(middle_idx)
            
            # 添加右边界点
            tick_indices.append(len(times) - 1)
            
            self.price_ax.set_xticks(tick_indices)
            
            # 准备日期标签
            date_labels = [times[i].strftime('%Y-%m-%d') for i in tick_indices]
            self.price_ax.set_xticklabels(date_labels)
            
            # 设置标签对齐方式：左对齐、居中、右对齐
            for i, label in enumerate(self.price_ax.get_xticklabels()):
                if i == 0:
                    label.set_ha('left')  # 左对齐
                elif i == len(date_labels) - 1:
                    label.set_ha('right')  # 右对齐
                else:
                    label.set_ha('center')  # 居中
        
        self.figure.tight_layout()
        self.canvas.draw()
        
    def update_nav_chart(self, nav_data):
        """更新净值曲线图表（保留方法但不执行绘图）
        
        Args:
            nav_data: 净值数据，包含时间索引和nav列
        """
        # 保存数据但不显示
        self.nav_data = nav_data
        
    def add_buy_point(self, time, price):
        """添加买入点
        
        Args:
            time: 买入时间
            price: 买入价格
        """
        self.buy_points.append({'time': time, 'price': price})
        
    def add_sell_point(self, time, price):
        """添加卖出点
        
        Args:
            time: 卖出时间
            price: 卖出价格
        """
        self.sell_points.append({'time': time, 'price': price})
        
    def toggle_price_display(self, show):
        """切换是否显示价格曲线
        
        Args:
            show: 是否显示
        """
        self.show_price = show
        if show:
            if self.price_ax is None:
                self.price_ax = self.figure.add_subplot(111)
            self.update_chart(self.price_data)
        else:
            if self.price_ax:
                self.figure.delaxes(self.price_ax)
                self.price_ax = None
        
        self.figure.tight_layout()
        self.canvas.draw()
        
    def clear(self):
        """清空图表数据"""
        self.price_data = pd.DataFrame()
        self.nav_data = pd.DataFrame()
        self.buy_points = []
        self.sell_points = []
        
        if self.price_ax:
            self.price_ax.clear()
            
            # 清除所有现有的文本对象，避免小黑框
            for text in self.price_ax.texts:
                text.remove()
            
            # 不使用set_title，避免小黑框
            self.price_ax.text(0.5, 0.98, '价格走势图', 
                             transform=self.price_ax.transAxes,
                             ha='center', va='top',
                             fontsize=10, color='black')
            
            self.price_ax.set_xlabel('时间')
            self.price_ax.set_ylabel('价格')
            self.price_ax.grid(True)
            
            # 设置默认的三个刻度位置
            self.price_ax.set_xticks([0.0, 0.5, 1.0])
            self.price_ax.set_xticklabels(["", "", ""])
            
            # 设置标签对齐方式
            for i, label in enumerate(self.price_ax.get_xticklabels()):
                if i == 0:
                    label.set_ha('left')  # 左对齐
                elif i == 2:
                    label.set_ha('right')  # 右对齐
                else:
                    label.set_ha('center')  # 居中
            
        if self.figure and self.canvas:
            self.figure.tight_layout()
            self.canvas.draw()
        
    def set_title(self, title):
        """设置图表标题
        
        Args:
            title: 标题文本
        """
        if self.price_ax:
            # 移除旧的标题
            self.price_ax.set_title('')
            
            # 清除所有现有的文本对象
            for text in self.price_ax.texts:
                text.remove()
                
            # 使用自定义方式添加标题，避免使用可能产生小黑框的方法
            self.price_ax.text(0.5, 0.98, title, 
                             transform=self.price_ax.transAxes,
                             ha='center', va='top',
                             fontsize=10, color='black')
            
            # 保持刻度标签的对齐方式
            if hasattr(self.price_ax, 'get_xticks') and len(self.price_ax.get_xticks()) > 0:
                labels = self.price_ax.get_xticklabels()
                if len(labels) >= 3:
                    labels[0].set_ha('left')      # 左对齐
                    labels[-1].set_ha('right')    # 右对齐
                    for i in range(1, len(labels) - 1):
                        labels[i].set_ha('center')  # 中间标签居中
            
            self.figure.tight_layout()
            self.canvas.draw()
            
    def hideToolbar(self):
        """隐藏导航工具栏"""
        if self.toolbar and self.toolbar.isVisible():
            self.toolbar.setVisible(False)
            
    def showToolbar(self):
        """显示导航工具栏"""
        if self.toolbar and not self.toolbar.isVisible():
            self.toolbar.setVisible(True)
            
    def setFigure(self, fig):
        """设置自定义的matplotlib图表
        
        Args:
            fig: matplotlib Figure对象
        """
        # 移除旧的图表和画布
        layout = self.layout()
        if self.canvas:
            layout.removeWidget(self.canvas)
            self.canvas.deleteLater()
        
        # 设置新的图表和画布
        self.figure = fig
        self.canvas = FigureCanvas(self.figure)
        
        # 设置尺寸策略，使图表可以水平拉伸
        self.canvas.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        
        # 更新工具栏但保持隐藏
        if self.toolbar:
            layout.removeWidget(self.toolbar)
            self.toolbar.deleteLater()
            self.toolbar = NavigationToolbar(self.canvas, self)
            self.toolbar.setVisible(False)  # 确保工具栏隐藏
            layout.addWidget(self.toolbar)
        
        # 添加新的画布到布局
        layout.addWidget(self.canvas)
        
        # 更新引用
        if hasattr(fig, 'axes') and len(fig.axes) > 0:
            self.price_ax = fig.axes[0]
            
            # 确保没有小黑框
            if self.price_ax:
                # 移除标题，避免顶部的小黑框
                title = self.price_ax.get_title()
                self.price_ax.set_title('')
                
                # 清除所有现有的文本对象，避免小黑框
                for text in self.price_ax.texts:
                    text.remove()
                
                # 移除所有图例对象，避免小黑框
                if self.price_ax.get_legend():
                    self.price_ax.get_legend().remove()
                
                # 移除所有注释对象，避免小黑框
                for child in self.price_ax.get_children():
                    if hasattr(child, 'get_boxstyle'):
                        child.remove()
                
                # 使用自定义方式添加标题，避免使用可能产生小黑框的方法
                if title:
                    self.price_ax.text(0.5, 0.98, title, 
                                     transform=self.price_ax.transAxes,
                                     ha='center', va='top',
                                     fontsize=10, color='black')
                
                # 确保日期标签与图表边缘对齐
                if hasattr(self.price_ax, 'get_xticks') and len(self.price_ax.get_xticks()) > 0:
                    # 获取当前刻度和标签
                    ticks = list(self.price_ax.get_xticks())
                    labels = [item.get_text() for item in self.price_ax.get_xticklabels()]
                    
                    # 如果有足够的标签，确保首尾对齐
                    if len(ticks) > 1 and len(labels) > 1:
                        xlim = self.price_ax.get_xlim()
                        
                        # 创建新的刻度位置：左边界、中间和右边界
                        new_ticks = [xlim[0]]  # 左边界
                        
                        # 添加中间点
                        if len(ticks) > 2:
                            middle_point = (xlim[0] + xlim[1]) / 2
                            new_ticks.append(middle_point)
                        
                        # 添加右边界点
                        new_ticks.append(xlim[1])
                        
                        # 设置新的刻度位置
                        self.price_ax.set_xticks(new_ticks)
                        
                        # 获取日期标签
                        date_labels = []
                        if len(labels) > 0:
                            # 获取第一个日期标签
                            if labels[0]:
                                date_labels.append(labels[0])
                            else:
                                date_labels.append("")
                            
                            # 获取中间日期标签
                            if len(labels) > 2:
                                middle_idx = len(labels) // 2
                                date_labels.append(labels[middle_idx])
                            else:
                                date_labels.append("")
                            
                            # 获取最后一个日期标签
                            if labels[-1]:
                                date_labels.append(labels[-1])
                            else:
                                date_labels.append("")
                        
                        # 设置日期标签对齐方式
                        self.price_ax.set_xticklabels(date_labels)
                        
                        # 设置标签对齐方式：左对齐、居中、右对齐
                        for i, label in enumerate(self.price_ax.get_xticklabels()):
                            if i == 0:
                                label.set_ha('left')  # 左对齐
                            elif i == len(date_labels) - 1:
                                label.set_ha('right')  # 右对齐
                            else:
                                label.set_ha('center')  # 居中
        
        # 连接画布大小变化信号
        self.canvas.mpl_connect('resize_event', self.on_resize)
        
        # 刷新显示
        self.canvas.draw()
        
    def plot(self, x, y, color='blue', fill=False, fill_color='lightblue'):
        """简单绘图方法
        
        Args:
            x: x轴数据
            y: y轴数据
            color: 线条颜色
            fill: 是否填充曲线下方区域
            fill_color: 填充颜色
        """
        if self.price_ax:
            self.price_ax.plot(x, y, color=color)
            
            if fill:
                self.price_ax.fill_between(x, y, min(y), color=fill_color, alpha=0.3)
                
            self.figure.tight_layout()
            self.canvas.draw() 