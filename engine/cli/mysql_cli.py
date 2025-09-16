# engine/cli/mysql_cli.py
from __future__ import annotations
import argparse, os, sys, json, time

# Windows 没有内置 readline，可选导入避免报错
try:
    import readline  # type: ignore
except Exception:
    readline = None

from typing import Optional, Iterable, Dict, Any, List
from engine.executor import Executor

# 你的 SQL 编译器（小L 的）应位于 sql/sql_compiler.py
try:
    from sql.sql_compiler import SQLCompiler  # type: ignore
except Exception as e:
    print("[致命错误] 无法导入 sql/sql_compiler.SQLCompiler：", e)
    sys.exit(2)

# 弹窗/导出桥（非阻塞弹窗在子进程中）
try:
    from .poptable_bridge import (
        set_last_result,
        show_last_popup,
        export_last_to_excel,
    )  # type: ignore
except Exception:
    set_last_result = None
    show_last_popup = None
    export_last_to_excel = None

BANNER = """mini-db 教学版客户端
说明：
  - 输入 SQL，以分号 ';' 结尾回车执行
  - \\dt                          显示当前所有表
  - \\create_index 表 列          建立索引
  - \\list_indexes 表             显示表
  - \\drop_index 表 索引名        删除索引
  - \\popup                       弹窗显示最近一次查询结果
  - \\export 路径                 导出最近一次查询结果（xlsx/缺库回退csv）
  - \\q                           退出
"""

def read_statement(prompt: str = "mini-db> ") -> Optional[str]:
    r"""多行输入：以分号结束；以 '\' 开头的元命令（\q, \dt, \popup, \export）直接返回。"""
    buf: List[str] = []
    while True:
        try:
            line = input(prompt if not buf else "......> ")
        except EOFError:
            print()
            return None
        if not line:
            continue
        s = line.strip()
        # 元命令：直接返回，不要求以 ';' 结尾
        if s.startswith("\\"):
            return s
        buf.append(line)
        # SQL：以 ';' 结束
        if s.endswith(";"):
            return "\n".join(buf)

def _print_rows(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        print("(空集)")
        return
    cols = list(rows[0].keys())
    # 计算每列宽度（包含表头）
    widths = []
    header_row = {c: c for c in cols}
    temp_rows = rows + [header_row]
    def _fmt(v):
        return "NULL" if v is None else str(v)
    for c in cols:
        widths.append(max(len(_fmt(r.get(c, ""))) for r in temp_rows))
    # 表头
    header = " | ".join(c.ljust(w) for c, w in zip(cols, widths))
    print(header)
    print("-+-".join("-"*w for w in widths))
    # 数据
    for r in rows:
        print(" | ".join(_fmt(r.get(c, "")).ljust(w) for c, w in zip(cols, widths)))
    print(f"(共 {len(rows)} 行)")

def _coerce_tables_to_items(exe: Executor, tables_obj: Any) -> List[tuple[str, Dict[str, Any]]]:
    """
    兼容三种返回：
      1) dict: {name -> meta}
      2) list[str]: ["student", "course"]
      3) list[dict]: [{"name":..., "columns":[...]}, ...]
    统一转成 [(name, meta_dict)]
    """
    items: List[tuple[str, Dict[str, Any]]] = []
    if isinstance(tables_obj, dict):
        items = list(tables_obj.items())
    elif isinstance(tables_obj, list):
        if not tables_obj:
            items = []
        else:
            first = tables_obj[0]
            if isinstance(first, str):
                # 仅名字列表，逐个取 meta
                for name in tables_obj:
                    try:
                        meta = exe.catalog.get_table(name) or {}
                    except Exception:
                        meta = {}
                    items.append((name, meta))
            elif isinstance(first, dict) and "name" in first:
                for t in tables_obj:
                    name = t.get("name")
                    if not name:
                        continue
                    items.append((name, t))
            else:
                # 无法识别，直接字符串化输出
                for idx, t in enumerate(tables_obj):
                    items.append((f"table_{idx}", {"raw": t}))
    else:
        # 其它类型，直接空
        items = []
    return items

def main(argv=None):
    ap = argparse.ArgumentParser(description="mini-db 中文命令行")
    ap.add_argument("--data", default="data", help="数据目录（表文件与目录信息将保存在此处）")
    ap.add_argument("--debug", action="store_true", help="显示详细报错堆栈")
    args = ap.parse_args(argv)
    DEBUG = args.debug

    print(BANNER)
    executor = Executor(args.data)
    compiler = SQLCompiler()

    while True:
        sql = read_statement()
        if sql is None:
            break
        sql_stripped = sql.strip()

        # ---------- 索引元命令 ----------
        if sql_stripped.startswith("\\create_index"):
            parts = sql_stripped.split()
            if len(parts) < 3:
                print("用法: \\create_index <table> <column> [index_name]")
            else:
                _, t, c, *rest = parts
                iname = rest[0] if rest else f"idx_{c}"
                plan = {"type": "CreateIndex", "table_name": t, "column": c, "index_name": iname}
                start = time.perf_counter()
                out = executor.execute_plan(plan)
                elapsed = time.perf_counter() - start
                print(out.get("message") or out)
                print(f"（耗时 {elapsed:.6f} s）")
            continue

        if sql_stripped.startswith("\\list_indexes"):
            parts = sql_stripped.split()
            t = parts[1] if len(parts) > 1 else None
            idxs = executor.indexes.list_indexes(t)  # type: ignore
            if not idxs:
                print("(无索引)")
            else:
                if t:
                    for name, meta in idxs.items():
                        print(f"{t}.{name} -> {meta.get('type')} ({meta.get('column')})")
                else:
                    for tt, mm in idxs.items():
                        for name, meta in mm.items():
                            print(f"{tt}.{name} -> {meta.get('type')} ({meta.get('column')})")
            continue

        if sql_stripped.startswith("\\drop_index"):
            parts = sql_stripped.split()
            if len(parts) != 3:
                print("用法: \\drop_index <table> <index_name>")
            else:
                _, t, iname = parts
                executor.indexes.drop_index(t, iname)  # type: ignore
                print(f"Index {t}.{iname} dropped from registry.")
            continue

        # ---------- 弹窗/导出 ----------
        if sql_stripped in ("\\popup", "\\popup;"):
            if show_last_popup is None:
                print("该功能依赖 engine/cli/poptable_bridge.py（以及 poptable.py）。")
            else:
                show_last_popup("查询结果")  # 非阻塞
            continue

        if sql_stripped.startswith("\\export"):
            if export_last_to_excel is None:
                print("该功能依赖 engine/cli/poptable_bridge.py（以及 poptable.py）。")
            else:
                import os, datetime
                args_str = sql_stripped[len("\\export"):].strip()
                path = None
                directory = None
                if not args_str:
                    # 无参数：当前目录自动命名
                    path = None
                    directory = None
                else:
                    s = args_str
                    # 容错：去掉 ["..."] / '...' / "..."
                    if (s.startswith("[") and s.endswith("]")) or (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
                        try:
                            parsed = json.loads(s)
                            if isinstance(parsed, list) and parsed:
                                s = str(parsed[0])
                            elif isinstance(parsed, str):
                                s = parsed
                        except Exception:
                            s = s.strip("[]'\" ")
                    s = s.strip(" '\"")
                    if os.path.isdir(s) or s.endswith(os.sep) or s.endswith("/") or s.endswith("\\"):
                        directory = s
                        path = None
                    else:
                        base, ext = os.path.splitext(s)
                        path = s if ext else (s + ".xlsx")
                saved = export_last_to_excel(file_path=path, directory=directory)  # type: ignore
                if saved:
                    print(f"已导出到: {saved}")
            continue

        # ---------- 基本元命令 ----------
        if sql_stripped in ("\\q;", "\\q"):
            print("再见！")
            break

        if sql_stripped in ("\\dt;", "\\dt"):
            start = time.perf_counter()
            tables_obj = executor.catalog.list_tables()
            items = _coerce_tables_to_items(executor, tables_obj)
            if not items:
                print("(当前没有表)")
            else:
                for name, meta in items:
                    cols_meta = meta.get('columns', [])
                    if isinstance(cols_meta, list) and cols_meta and isinstance(cols_meta[0], dict):
                        cols = ", ".join([f"{c.get('name','?')} {c.get('type','')}" for c in cols_meta])
                        print(f"{name} ({cols})")
                    else:
                        print(name)
            elapsed = time.perf_counter() - start
            print(f"（耗时 {elapsed:.6f} s）")
            continue

        # ---------- 从编译到执行，统一计时 ----------
        start_all = time.perf_counter()
        result = compiler.compile(sql)

        if not result.get("success"):
            et = result.get("error_type", "错误")
            msg = result.get("message") or result.get("semantic_result", {}).get("error", "")
            print(f"[{et}] {msg}")
            if et == "SYNTAX_ERROR" and result.get("line_text"):
                print(result["line_text"])
                print(result.get("pointer", ""))
            elapsed = time.perf_counter() - start_all
            print(f"（耗时 {elapsed:.6f} s）")
            continue

        plan = result.get("execution_plan") or {}
        try:
            out = executor.execute_plan(plan)
        except KeyError as e:
            # 典型：table 'xxx' not found
            msg = str(e)
            if (msg.startswith('"') and msg.endswith('"')) or (msg.startswith("'") and msg.endswith("'")):
                msg = msg[1:-1]  # 去掉多余引号
            print(f"[Runtime error] {msg}")
            if DEBUG:
                import traceback;
                traceback.print_exc()
            elapsed = time.perf_counter() - start_all
            print(f"（耗时 {elapsed:.6f} s）")
            continue
        except Exception as e:
            # 其他未知错误也给简洁提示；需要再查看堆栈时加 --debug
            print(f"[Runtime error] {e}")
            if DEBUG:
                import traceback;
                traceback.print_exc()
            elapsed = time.perf_counter() - start_all
            print(f"（耗时 {elapsed:.6f} s）")
            continue

        # ---------- 打印结果 ----------
        if out.get("ok") and "rows" in out:
            rows = out["rows"]
            if set_last_result is not None:
                # 记住最近一次查询结果，供 \popup / \export 使用
                try:
                    set_last_result(rows)  # type: ignore
                except Exception:
                    pass
            _print_rows(rows)
        else:
            print(out.get("message") or out.get("error") or out)

        elapsed = time.perf_counter() - start_all
        print(f"（耗时 {elapsed:.6f} s）")

if __name__ == "__main__":
    main()
