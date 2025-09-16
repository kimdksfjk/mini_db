#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
表格弹窗展示工具：
提供 show_table_popup(table_json, title) 函数，接收JSON（或dict）数据并以图形化弹窗展示表格。

支持的数据格式：
1) {
     "columns": ["col1", "col2", ...],
     "rows": [[v11, v12, ...], [v21, v22, ...], ...]
   }
2) {
     "columns": ["col1", "col2", ...],
     "rows": [{"col1": v11, "col2": v12, ...}, {"col1": v21, ...}, ...]
   }
"""

from typing import Any, Dict, List, Sequence, Tuple, Union, Optional
import json
import os
from datetime import datetime


def _normalize_table_data(table: Dict[str, Any]) -> Tuple[List[str], List[List[Any]]]:
    """标准化表格数据，确保格式一致性"""
    if not isinstance(table, dict):
        raise ValueError("table_json必须是object/dict，或可解析为dict的JSON字符串")

    if 'columns' not in table or 'rows' not in table:
        raise ValueError("table_json格式错误：需要包含'columns'与'rows'")

    columns = list(table['columns'])
    raw_rows = table['rows']

    rows: List[List[Any]] = []
    if not isinstance(raw_rows, Sequence):
        raise ValueError("rows必须是序列")

    for i, r in enumerate(raw_rows):
        # 支持每行是dict或list的混合
        if isinstance(r, dict):
            rows.append([r.get(c, None) for c in columns])
        elif isinstance(r, Sequence):
            r_list = list(r)
            if len(r_list) != len(columns):
                raise ValueError(f"行 {i + 1} 长度({len(r_list)})与列数({len(columns)})不一致")
            rows.append(r_list)
        else:
            raise ValueError(f"rows中的第 {i + 1} 行应为列表或字典，实际为 {type(r).__name__}")

    return columns, rows


def _export_to_csv(path: str, cols: List[str], rs: List[List[Any]]) -> None:
    """导出数据为CSV文件"""
    import csv
    with open(path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        for r in rs:
            writer.writerow(["" if v is None else str(v) for v in r])


def _export_to_sql(path: str, cols: List[str], rs: List[List[Any]], table_name: str = "exported_table") -> None:
    """导出数据为SQL文件

    Args:
        path: 输出文件路径
        cols: 列名列表
        rs: 数据行列表
        table_name: 生成的表名
    """
    with open(path, 'w', encoding='utf-8') as f:
        # 写入文件头注释
        f.write("-- 自动生成的SQL文件\n")
        f.write(f"-- 表名: {table_name}\n")
        f.write(f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"-- 数据行数: {len(rs)}\n\n")

        # 生成CREATE TABLE语句
        f.write(f"DROP TABLE IF EXISTS `{table_name}`;\n")
        f.write(f"CREATE TABLE `{table_name}` (\n")

        # 分析列类型并生成列定义
        column_definitions = []
        for col in cols:
            # 简单类型推断：检查数据内容
            col_data = [row[i] for i, c in enumerate(cols) if c == col for row in rs if row[i] is not None]

            if not col_data:
                # 如果没有数据，默认为VARCHAR
                col_type = "VARCHAR(255)"
            else:
                # 类型推断逻辑
                is_numeric = True
                is_integer = True

                for val in col_data:
                    try:
                        # 尝试转换为数字
                        float_val = float(str(val))
                        if not float_val.is_integer():
                            is_integer = False
                    except (ValueError, TypeError):
                        is_numeric = False
                        break

                if is_numeric:
                    if is_integer:
                        col_type = "INT"
                    else:
                        col_type = "DECIMAL(10,2)"
                else:
                    # 计算最大字符串长度
                    max_len = max(len(str(val)) for val in col_data)
                    col_type = f"VARCHAR({max(50, min(max_len * 2, 500))})"

            column_definitions.append(f"  `{col}` {col_type}")

        f.write(",\n".join(column_definitions))
        f.write("\n);\n\n")

        # 生成INSERT语句
        if rs:
            f.write(f"INSERT INTO `{table_name}` (`{'`, `'.join(cols)}`) VALUES\n")

            insert_values = []
            for row in rs:
                # 处理每行的值
                formatted_values = []
                for val in row:
                    if val is None:
                        formatted_values.append("NULL")
                    elif isinstance(val, (int, float)):
                        formatted_values.append(str(val))
                    else:
                        # 转义单引号并添加引号
                        escaped_val = str(val).replace("'", "''")
                        formatted_values.append(f"'{escaped_val}'")

                insert_values.append(f"({', '.join(formatted_values)})")

            # 分批写入INSERT语句（每批1000行）
            batch_size = 1000
            for i in range(0, len(insert_values), batch_size):
                batch = insert_values[i:i + batch_size]
                f.write(",\n".join(batch))
                if i + batch_size < len(insert_values):
                    f.write(";\n\nINSERT INTO `{table_name}` (`{'`, `'.join(cols)}`) VALUES\n")
                else:
                    f.write(";\n")

        f.write("\n-- SQL文件生成完成\n")


class TableViewer:
    """表格查看器类，封装UI逻辑"""

    def __init__(self, table_data: Dict[str, Any], title: str = "查询结果"):
        self.table_data = table_data
        self.title = title
        self.columns, self.rows = _normalize_table_data(table_data)
        self.window = None
        self.tree = None

    def create_ui(self):
        """创建用户界面"""
        import tkinter as tk
        from tkinter import ttk

        # 获取或创建根窗口
        existing_root = getattr(tk, "_default_root", None)
        if existing_root is not None:
            root = existing_root
            self.created_root = False
        else:
            root = tk.Tk()
            self.created_root = True
            root.withdraw()

        # 创建主窗口
        self.window = tk.Toplevel(root)
        self.window.title(self.title)
        self.window.geometry("800x400")
        self.window.minsize(300, 300)

        # 设置窗口图标（如果有）
        try:
            self.window.iconbitmap(self._get_icon_path())
        except:
            pass

        # 仅当复用外部root时才设置为transient
        if not self.created_root:
            try:
                self.window.transient(root)
            except Exception:
                pass

        # 设置窗口置顶和焦点
        try:
            self.window.lift()
            self.window.focus_force()
        except Exception:
            pass

        # 设置窗口关闭事件
        self.window.protocol("WM_DELETE_WINDOW", self._on_close)

        # 创建主框架
        main_frame = ttk.Frame(self.window)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # 创建标题栏
        self._create_title_bar(main_frame)

        # 创建表格框架
        table_frame = ttk.Frame(main_frame)
        table_frame.pack(fill=tk.BOTH, expand=True, pady=(10, 0))

        # 创建表格
        self._create_table(table_frame)

        # 创建状态栏
        self._create_status_bar(main_frame)

        # 居中窗口
        self._center_window()

        return self.window

    def _create_title_bar(self, parent):
        """创建标题栏"""
        import tkinter as tk
        from tkinter import ttk

        title_frame = ttk.Frame(parent)
        title_frame.pack(fill=tk.X, pady=(0, 10))

        # # 标题
        # title_label = ttk.Label(
        #     title_frame,
        #     text=self.title,
        #     font=("Segoe UI", 12, "bold")
        # )
        # title_label.pack(side=tk.LEFT)

        # 导出按钮
        export_btn = ttk.Button(
            title_frame,
            text="导出数据",
            command=self._export_data,
            width=12
        )
        export_btn.pack(side=tk.RIGHT, padx=(5, 0))

        # 关闭按钮
        close_btn = ttk.Button(
            title_frame,
            text="关闭",
            command=self._on_close,
            width=8
        )
        close_btn.pack(side=tk.RIGHT)

    def _create_table(self, parent):
        """创建表格"""
        import tkinter as tk
        from tkinter import ttk

        # 设置样式
        style = ttk.Style()
        style.configure("Treeview",
                        font=("Segoe UI", 10),
                        rowheight=28,
                        borderwidth=1,
                        relief="solid")
        style.configure("Treeview.Heading",
                        font=("Segoe UI", 10, "bold"),
                        background="#f0f0f0")
        style.map("Treeview.Heading", background=[("active", "#e0e0e0")])

        # 创建滚动条
        v_scrollbar = ttk.Scrollbar(parent, orient=tk.VERTICAL)
        h_scrollbar = ttk.Scrollbar(parent, orient=tk.HORIZONTAL)

        # 创建表格
        self.tree = ttk.Treeview(
            parent,
            columns=self.columns,
            show='headings',
            displaycolumns=self.columns,
            yscrollcommand=v_scrollbar.set,
            xscrollcommand=h_scrollbar.set,
            selectmode='extended'
        )

        # 配置滚动条
        v_scrollbar.config(command=self.tree.yview)
        h_scrollbar.config(command=self.tree.xview)

        # 布局
        self.tree.grid(row=0, column=0, sticky='nsew')
        v_scrollbar.grid(row=0, column=1, sticky='ns')
        h_scrollbar.grid(row=1, column=0, sticky='ew')

        # 配置网格权重
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(0, weight=1)

        # 设置列标题和属性
        col_max_len = {c: len(str(c)) for c in self.columns}
        for r in self.rows:
            for c, v in zip(self.columns, r):
                l = len(str(v)) if v is not None else 0
                if l > col_max_len[c]:
                    col_max_len[c] = l

        for col in self.columns:
            self.tree.heading(col, text=col, anchor='center')
            est = col_max_len[col]
            width = max(80, min(300, est * 9))
            self.tree.column(col, width=width, anchor='center', stretch=True)

        # 插入数据
        self.tree.tag_configure('odd', background='#f8f9fa')
        self.tree.tag_configure('even', background='#ffffff')

        for idx, row in enumerate(self.rows):
            display_row = ["" if v is None else str(v) for v in row]
            tag = 'odd' if idx % 2 else 'even'
            self.tree.insert('', 'end', values=display_row, tags=(tag,))

        # 绑定事件
        self.tree.bind('<Double-1>', self._on_cell_double_click)

    def _create_status_bar(self, parent):
        """创建状态栏"""
        import tkinter as tk
        from tkinter import ttk

        status_frame = ttk.Frame(parent)
        status_frame.pack(fill=tk.X, pady=(10, 0))

        # 行数信息
        row_count = len(self.rows)
        col_count = len(self.columns)
        status_text = f"共 {row_count} 行, {col_count} 列"

        status_label = ttk.Label(
            status_frame,
            text=status_text,
            font=("Segoe UI", 9),
            foreground="#666666"
        )
        status_label.pack(side=tk.LEFT)

        # 添加更新时间
        time_label = ttk.Label(
            status_frame,
            text=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            font=("Segoe UI", 9),
            foreground="#999999"
        )
        time_label.pack(side=tk.RIGHT)

    def _center_window(self):
        """居中窗口"""
        self.window.update_idletasks()
        width = self.window.winfo_width()
        height = self.window.winfo_height()

        screen_width = self.window.winfo_screenwidth()
        screen_height = self.window.winfo_screenheight()

        x = (screen_width - width) // 2
        y = (screen_height - height) // 2

        self.window.geometry(f"{width}x{height}+{x}+{y}")

    def _get_icon_path(self):
        """获取图标路径（如果有）"""
        # 这里可以添加图标文件的路径
        # 例如：return "icon.ico"
        return None

    def _export_data(self):
        """导出数据"""
        from tkinter import filedialog, messagebox

        default_name = f"{self.title or '数据导出'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        file_path = filedialog.asksaveasfilename(
            parent=self.window,
            defaultextension=".xlsx",
            initialfile=default_name,
            filetypes=[
                ("Excel 工作簿", ".xlsx"),
                ("CSV 文件", ".csv"),
                ("SQL 文件", ".sql"),
                ("所有文件", "*.*")
            ]
        )

        if not file_path:
            return

        try:
            if file_path.lower().endswith('.xlsx'):
                # 尝试使用openpyxl导出
                try:
                    from openpyxl import Workbook
                    wb = Workbook()
                    ws = wb.active
                    ws.title = "数据导出"

                    # 添加列标题
                    ws.append(self.columns)

                    # 添加数据行
                    for row in self.rows:
                        ws.append(["" if v is None else v for v in row])

                    # 自动调整列宽
                    for column in ws.columns:
                        max_length = 0
                        column_letter = column[0].column_letter
                        for cell in column:
                            try:
                                if len(str(cell.value)) > max_length:
                                    max_length = len(str(cell.value))
                            except:
                                pass
                        adjusted_width = min(max_length + 2, 50)
                        ws.column_dimensions[column_letter].width = adjusted_width

                    wb.save(file_path)
                    messagebox.showinfo("导出成功", f"数据已成功导出到:\n{file_path}", parent=self.window)

                except ImportError:
                    # 回退到CSV
                    csv_path = file_path[:-5] + '.csv'
                    _export_to_csv(csv_path, self.columns, self.rows)
                    messagebox.showinfo("导出完成",
                                        f"未检测到openpyxl库，已导出为CSV格式:\n{csv_path}\n\n如需导出Excel格式，请安装openpyxl: pip install openpyxl",
                                        parent=self.window)
            elif file_path.lower().endswith('.sql'):
                # 导出SQL
                # 清理表名：移除特殊字符，替换空格为下划线，确保符合SQL标识符规范
                if self.title and self.title != "查询结果":
                    # 移除特殊字符，只保留字母、数字、下划线
                    import re
                    clean_title = re.sub(r'[^\w\u4e00-\u9fff]', '_', self.title)
                    # 移除连续的下划线
                    clean_title = re.sub(r'_+', '_', clean_title)
                    # 移除开头和结尾的下划线
                    clean_title = clean_title.strip('_')
                    table_name = clean_title if clean_title else 'exported_table'
                else:
                    table_name = 'exported_table'
                _export_to_sql(file_path, self.columns, self.rows, table_name)
                messagebox.showinfo("导出成功", f"SQL文件已成功导出到:\n{file_path}\n\n表名: {table_name}",
                                    parent=self.window)
            else:
                # 导出CSV
                _export_to_csv(file_path, self.columns, self.rows)
                messagebox.showinfo("导出成功", f"数据已成功导出到:\n{file_path}", parent=self.window)

        except Exception as e:
            messagebox.showerror("导出失败", f"导出过程中发生错误:\n{str(e)}", parent=self.window)

    def _on_cell_double_click(self, event):
        """双击单元格事件"""
        import tkinter as tk
        from tkinter import simpledialog

        item = self.tree.selection()[0] if self.tree.selection() else None
        if not item:
            return

        column = self.tree.identify_column(event.x)
        col_index = int(column.replace('#', '')) - 1

        if col_index < 0 or col_index >= len(self.columns):
            return

        current_value = self.tree.item(item, 'values')[col_index]

        # 弹出编辑对话框
        new_value = simpledialog.askstring(
            "编辑单元格",
            f"编辑 {self.columns[col_index]} 的值:",
            initialvalue=current_value,
            parent=self.window
        )

        if new_value is not None and new_value != current_value:
            # 更新显示的值
            values = list(self.tree.item(item, 'values'))
            values[col_index] = new_value
            self.tree.item(item, values=values)

    def _on_close(self):
        """关闭窗口"""
        if self.window:
            self.window.destroy()

        # 如果是我们创建的根窗口，也需要退出
        if hasattr(self, 'created_root') and self.created_root:
            import tkinter as tk
            root = tk._default_root
            if root:
                try:
                    root.quit()
                    root.destroy()
                except:
                    pass


def show_table_popup(table_json: Union[str, Dict[str, Any]], title: str = "查询结果", blocking: bool = True) -> None:
    """
    显示表格弹窗

    Args:
        table_json: 表格数据，可以是JSON字符串或字典
        title: 窗口标题
        blocking: 是否阻塞直到窗口关闭
    """
    try:
        # 解析表格数据
        table = json.loads(table_json) if isinstance(table_json, str) else table_json

        # 创建查看器实例
        viewer = TableViewer(table, title)
        window = viewer.create_ui()

        # 管理事件循环
        if blocking:
            if viewer.created_root:
                try:
                    # 新建的根窗口，启动事件循环
                    window.grab_set()
                    window.mainloop()
                except Exception as e:
                    print(f"窗口事件循环错误: {e}")
            else:
                # 已有事件循环环境，等待窗口关闭
                try:
                    import tkinter as tk
                    root = tk._default_root
                    if root:
                        root.wait_window(window)
                except Exception as e:
                    print(f"等待窗口错误: {e}")
        else:
            # 非阻塞模式
            if viewer.created_root:
                # 启动事件循环但不阻塞
                try:
                    import tkinter as tk
                    root = tk._default_root

                    def keep_alive():
                        try:
                            root.update()
                            root.after(100, keep_alive)
                        except:
                            pass

                    root.after(100, keep_alive)
                except Exception as e:
                    print(f"非阻塞模式错误: {e}")

    except Exception as e:
        # 错误处理
        import tkinter as tk
        from tkinter import messagebox

        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("数据错误", f"无法解析或显示表格数据：{e}")
        root.destroy()


def export_table_to_sql(data: Dict[str, Any], file_path: str = None, directory: str = None,
                        table_name: str = "exported_table") -> str:
    """
    独立导出表格数据为SQL文件

    Args:
        data: 表格数据，格式为 {'columns': [...], 'rows': [...]}
        file_path: 输出文件路径，如果为None则自动生成
        directory: 输出目录，如果指定则在此目录下生成文件
        table_name: 生成的表名

    Returns:
        实际保存的文件路径
    """
    import os
    from datetime import datetime

    # 数据标准化
    columns, rows = _normalize_table_data(data)

    # 处理目录参数
    if directory is not None:
        # 确保目录存在
        os.makedirs(directory, exist_ok=True)

        # 如果指定了目录但没有文件名，生成默认文件名
        if file_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = f"table_export_{timestamp}.sql"

        # 如果file_path只是文件名，则与directory组合
        if not os.path.dirname(file_path):
            file_path = os.path.join(directory, file_path)
    else:
        # 生成默认文件名（当前目录）
        if file_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = f"table_export_{timestamp}.sql"

    try:
        _export_to_sql(file_path, columns, rows, table_name)
        return file_path
    except Exception as e:
        raise Exception(f"导出SQL失败: {e}")


def export_table_to_excel(data: Dict[str, Any], file_path: str = None, directory: str = None) -> str:
    """
    独立导出表格数据为Excel文件

    Args:
        data: 表格数据，格式为 {'columns': [...], 'rows': [...]}
        file_path: 输出文件路径，如果为None则自动生成
        directory: 输出目录，如果指定则在此目录下生成文件

    Returns:
        实际保存的文件路径
    """
    import os
    from datetime import datetime

    # 数据标准化
    columns, rows = _normalize_table_data(data)

    # 处理目录参数
    if directory is not None:
        # 确保目录存在
        os.makedirs(directory, exist_ok=True)

        # 如果指定了目录但没有文件名，生成默认文件名
        if file_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = f"table_export_{timestamp}.xlsx"

        # 如果file_path只是文件名，则与directory组合
        if not os.path.dirname(file_path):
            file_path = os.path.join(directory, file_path)
    else:
        # 生成默认文件名（当前目录）
        if file_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = f"table_export_{timestamp}.xlsx"

    try:
        # 尝试使用openpyxl导出为xlsx
        if file_path.lower().endswith('.xlsx'):
            try:
                from openpyxl import Workbook
                wb = Workbook()
                ws = wb.active
                ws.title = "数据导出"

                # 添加列标题
                ws.append(columns)

                # 添加数据行
                for r in rows:
                    ws.append(["" if v is None else v for v in r])

                # 自动调整列宽
                for column in ws.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    ws.column_dimensions[column_letter].width = adjusted_width

                wb.save(file_path)
                return file_path
            except ImportError:
                # 回退为CSV
                csv_path = file_path[:-5] + '.csv'
                _export_to_csv(csv_path, columns, rows)
                return csv_path
        else:
            # 其他扩展名按CSV导出
            _export_to_csv(file_path, columns, rows)
            return file_path
    except Exception as e:
        raise Exception(f"导出失败: {e}")


if __name__ == "__main__":
    # 示例数据
    demo = {
        "columns": ["ID", "姓名", "年龄", "成绩", "备注"],
        "rows": [
            [1, "张三", 20, "A", "优秀学生"],
            [2, "李四", 21, "B+", "表现良好"],
            [3, "王五", 19, "A-", "进步明显"],
            [4, "赵六", 22, "C", "需要加强学习"],
            {"ID": 5, "姓名": "钱七", "年龄": 20, "成绩": "B", "备注": "态度认真"}
        ]
    }

    # 显示表格
    show_table_popup(demo, title="学生成绩表", blocking=True)