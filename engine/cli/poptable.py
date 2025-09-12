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

from typing import Any, Dict, List, Sequence, Tuple, Union
import json


def _normalize_table_data(table: Dict[str, Any]) -> Tuple[List[str], List[List[Any]]]:
    if not isinstance(table, dict):
        raise ValueError("table_json必须是object/dict，或可解析为dict的JSON字符串")
    if 'columns' not in table or 'rows' not in table:
        raise ValueError("table_json格式错误：需要包含'columns'与'rows'")
    columns = list(table['columns'])
    raw_rows = table['rows']

    rows: List[List[Any]] = []
    if not isinstance(raw_rows, Sequence):
        raise ValueError("rows必须是序列")

    for r in raw_rows:
        # 支持每行是dict或list的混合
        if isinstance(r, dict):
            rows.append([r.get(c, None) for c in columns])
        elif isinstance(r, Sequence):
            r_list = list(r)
            if len(r_list) != len(columns):
                raise ValueError(f"行长度({len(r_list)})与列数({len(columns)})不一致")
            rows.append(r_list)
        else:
            raise ValueError("rows中的每一行应为列表或字典")

    return columns, rows


def _export_to_csv(path: str, cols: List[str], rs: List[List[Any]]) -> None:
    """导出数据为CSV文件"""
    import csv
    with open(path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        for r in rs:
            writer.writerow(["" if v is None else v for v in r])


def show_table_popup(table_json: Union[str, Dict[str, Any]], title: str = "查询结果", blocking: bool = True) -> None:
    try:
        table = json.loads(table_json) if isinstance(table_json, str) else table_json
        columns, rows = _normalize_table_data(table)
    except Exception as e:
        # 若解析失败，用一个简易弹窗提示
        import tkinter as tk
        from tkinter import messagebox
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("数据错误", f"无法解析表格数据：{e}")
        root.destroy()
        return

    import tkinter as tk
    from tkinter import ttk

    # 复用已有root（若存在），否则创建
    existing_root = getattr(tk, "_default_root", None)
    if existing_root is not None:
        root = existing_root
        created_root = False
    else:
        root = tk.Tk()
        created_root = True
        root.withdraw()

    # HiDPI/清晰度调整（尽量避免模糊）
    try:
        root.tk.call('tk', 'scaling', 1.25)
    except Exception:
        pass

    win = tk.Toplevel(root)
    win.title(title)
    win.geometry("900x500")
    win.minsize(500, 300)
    # 仅当复用外部root时才设置为transient，避免root隐藏导致窗口不可见
    if not created_root:
        try:
            win.transient(root)
        except Exception:
            pass
    try:
        win.lift()
        win.focus_force()
    except Exception:
        pass

    # 统一风格与清晰度
    try:
        style = ttk.Style(win)
        style.configure('Treeview', font=('Segoe UI', 11), rowheight=28)
        style.configure('Treeview.Heading', font=('Segoe UI', 11, 'bold'))
    except Exception:
        pass

    container = ttk.Frame(win)
    container.pack(fill=tk.BOTH, expand=True)

    # 滚动条
    xscroll = ttk.Scrollbar(container, orient=tk.HORIZONTAL)
    yscroll = ttk.Scrollbar(container, orient=tk.VERTICAL)

    tree = ttk.Treeview(
        container,
        columns=columns,
        show='headings',
        displaycolumns=columns,
        xscrollcommand=xscroll.set,
        yscrollcommand=yscroll.set
    )

    xscroll.config(command=tree.xview)
    yscroll.config(command=tree.yview)

    # 列头与初始宽度/对齐
    # 先统计每列的最大文本长度（用于估算宽度）
    col_max_len = {c: len(str(c)) for c in columns}
    for r in rows:
        for c, v in zip(columns, r):
            l = len(str(v)) if v is not None else 0
            if l > col_max_len[c]:
                col_max_len[c] = l
    for col in columns:
        tree.heading(col, text=col, anchor='center')
        est = col_max_len[col]
        width = max(80, min(260, est * 10))
        tree.column(col, width=width, anchor='center', stretch=True)

    # 插入数据
    # 斑马纹与插入
    tree.tag_configure('odd', background='#fafafa')
    tree.tag_configure('even', background='#ffffff')
    for idx, row in enumerate(rows):
        display_row = ["" if v is None else str(v) for v in row]
        tag = 'odd' if idx % 2 else 'even'
        tree.insert('', 'end', values=display_row, tags=(tag,))

    # 布局
    tree.grid(row=0, column=0, sticky='nsew')
    yscroll.grid(row=0, column=1, sticky='ns')
    xscroll.grid(row=1, column=0, sticky='ew')

    container.rowconfigure(0, weight=1)
    container.columnconfigure(0, weight=1)

    # 关闭按钮
    btn_frame = ttk.Frame(win)
    btn_frame.pack(fill=tk.X)

    # 导出为Excel（.xlsx，若无openpyxl则回退为CSV）
    def export_to_excel() -> None:
        from tkinter import filedialog, messagebox
        default_name = f"{title or '导出'}.xlsx"
        file_path = filedialog.asksaveasfilename(
            parent=win,
            defaultextension=".xlsx",
            initialfile=default_name,
            filetypes=[("Excel 工作簿", ".xlsx"), ("CSV", ".csv"), ("所有文件", "*.*")]
        )
        if not file_path:
            return
        try:
            # 首选 openpyxl 输出 .xlsx
            if file_path.lower().endswith('.xlsx'):
                try:
                    from openpyxl import Workbook  # type: ignore
                except Exception:
                    # 回退为CSV
                    csv_path = file_path[:-5] + '.csv'
                    _export_to_csv(csv_path, columns, rows)
                    messagebox.showinfo("导出完成", f"未检测到openpyxl，已回退为CSV:\n{csv_path}")
                    return
                wb = Workbook()
                ws = wb.active
                ws.append(columns)
                for r in rows:
                    ws.append(["" if v is None else v for v in r])
                wb.save(file_path)
                messagebox.showinfo("导出完成", f"已导出：\n{file_path}")
            else:
                # 其他扩展名按CSV导出
                _export_to_csv(file_path, columns, rows)
                messagebox.showinfo("导出完成", f"已导出CSV：\n{file_path}")
        except Exception as e:
            from tkinter import messagebox
            messagebox.showerror("导出失败", f"导出时发生错误：{e}")

    export_btn = ttk.Button(btn_frame, text="导出为Excel", command=export_to_excel)
    export_btn.pack(side='right', padx=8, pady=6)

    close_btn = ttk.Button(btn_frame, text="关闭", command=win.destroy)
    close_btn.pack(side='right', padx=8, pady=6)

    # 居中窗口
    win.update_idletasks()
    w = win.winfo_width()
    h = win.winfo_height()
    sw = win.winfo_screenwidth()
    sh = win.winfo_screenheight()
    win.geometry(f"{w}x{h}+{(sw - w) // 2}+{(sh - h) // 2}")

    # 管理事件循环：
    if blocking:
        # 阻塞模式：等待窗口关闭
        if created_root:
            # 新建的根窗口，启动事件循环
            try:
                win.grab_set()
                root.mainloop()
            except Exception:
                pass
        else:
            # 已有事件循环环境，等待窗口关闭
            try:
                root.wait_window(win)
            except Exception:
                pass
    else:
        # 非阻塞模式：立即返回，窗口在后台运行
        if created_root:
            # 启动事件循环但不阻塞
            try:
                win.grab_set()

                # 启动一个定时器来保持事件循环运行
                def keep_alive():
                    try:
                        root.update()
                        root.after(100, keep_alive)  # 每100ms更新一次
                    except:
                        pass

                root.after(100, keep_alive)
            except Exception:
                pass
        else:
            # 在已有root的情况下，也要保持窗口活跃
            try:
                def keep_alive():
                    try:
                        root.update()
                        root.after(100, keep_alive)
                    except:
                        pass

                root.after(100, keep_alive)
            except Exception:
                pass


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
                ws.append(columns)
                for r in rows:
                    ws.append(["" if v is None else v for v in r])
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
    demo = {
        "columns": ["id", "name", "age", "grade"],
        "rows": [
            [1, "Alice", 20, "A"],
            [2, "Bob", 20, "B"],
            {"id": 3, "name": "Carol", "age": 21, "grade": "A"},
        ]
    }
    show_table_popup(demo, title="示例数据")


