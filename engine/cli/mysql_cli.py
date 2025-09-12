# engine/cli/mysql_cli.py
from __future__ import annotations
import argparse, os, sys, json, time

# Windows 上没有内置 readline，这里做成可选导入，避免报错
try:
    import readline  # type: ignore
except Exception:
    readline = None

from typing import Optional
from engine.executor import Executor

# 与弹窗/导出桥接（保持 poptable.py 不改）
try:
    from engine.cli.poptable_bridge import (
        set_last_result, show_last_popup, export_last_to_excel
    )
except Exception:
    # 桥接层不存在也不阻塞 CLI 运行，仅在使用相关命令时提示
    set_last_result = None  # type: ignore
    show_last_popup = None  # type: ignore
    export_last_to_excel = None  # type: ignore

# 你的 SQL 编译器（小L 的）应位于 sql/sql_compiler.py
try:
    from sql.sql_compiler import SQLCompiler  # type: ignore
except Exception as e:
    print("[致命错误] 无法导入 sql/sql_compiler.SQLCompiler：", e)
    sys.exit(2)

BANNER = """mini-db 教学版客户端
说明：
  - 输入 SQL，以分号 ';' 结尾回车执行
  - \\dt           显示当前所有表
  - \\create_index 表 列 [索引名]
  - \\list_indexes [表]
  - \\drop_index   表 索引名
  - \\popup        弹窗显示最近一次查询结果
  - \\export [路径] 导出最近一次查询结果（xlsx/缺库回退csv）
  - \\q            退出
"""

def read_statement(prompt: str = "mini-db> ") -> Optional[str]:
    r"""多行输入：以分号结束；以 '\' 开头的元命令（\q, \dt 等）直接返回。"""
    buf = []
    while True:
        try:
            line = input(prompt if not buf else "......> ")
        except EOFError:
            print()
            return None
        if not line:
            continue
        s = line.strip()
        # ✅ 元命令：直接返回，不要求以 ';' 结尾
        if s.startswith("\\"):
            return s
        buf.append(line)
        # SQL：以 ';' 结束
        if s.endswith(";"):
            return "\n".join(buf)

def _store_popup_result(rows):
    """把 rows[list[dict]] 变成 {columns, rows} 结构并缓存到弹窗桥接层。"""
    if set_last_result is None:
        return
    try:
        cols = list(rows[0].keys()) if rows else []
        table_for_popup = {"columns": cols, "rows": [[r.get(c) for c in cols] for r in rows]}
        set_last_result(table_for_popup)  # type: ignore
    except Exception:
        pass

def main(argv=None):
    ap = argparse.ArgumentParser(description="mini-db 中文命令行")
    ap.add_argument("--data", default="data", help="数据与目录（表文件与目录信息将保存在此处）")
    args = ap.parse_args(argv)

    print(BANNER)
    executor = Executor(args.data)
    compiler = SQLCompiler()

    while True:
        sql = read_statement()
        if sql is None:
            break
        sql_stripped = sql.strip()

        # ---------- 索引相关元命令 ----------
        # \create_index 表 列 [索引名]
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

        # \list_indexes [表]
        if sql_stripped.startswith("\\list_indexes"):
            parts = sql_stripped.split()
            t = parts[1] if len(parts) > 1 else None
            idxs = executor.indexes.list_indexes(t)
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

        # \drop_index 表 索引名
        if sql_stripped.startswith("\\drop_index"):
            parts = sql_stripped.split()
            if len(parts) != 3:
                print("用法: \\drop_index <table> <index_name>")
            else:
                _, t, iname = parts
                executor.indexes.drop_index(t, iname)
                print(f"Index {t}.{iname} dropped from registry.")
            continue

        # ---------- 弹窗/导出元命令 ----------
        if sql_stripped == "\\popup":
            if show_last_popup is None:
                print("该功能依赖 engine/cli/poptable_bridge.py（以及 poptable.py）。")
            else:
                show_last_popup("查询结果")  # type: ignore
            continue

        if sql_stripped.startswith("\\export"):
            if export_last_to_excel is None:
                print("该功能依赖 engine/cli/poptable_bridge.py（以及 poptable.py）。")
            else:
                parts = sql_stripped.split(maxsplit=1)
                path = parts[1] if len(parts) > 1 else None
                saved = export_last_to_excel(file_path=path)  # type: ignore
                if saved:
                    print(f"已导出到: {saved}")
            continue

        # ---------- 基本元命令 ----------
        if sql_stripped in ("\\q;", "\\q"):
            print("再见！")
            break
        if sql_stripped in ("\\dt;", "\\dt"):
            start = time.perf_counter()
            tables = executor.catalog.list_tables()
            if not tables:
                print("(当前没有表)")
            else:
                for name, meta in tables.items():
                    cols = ", ".join([f"{c['name']} {c.get('type','')}" for c in meta.get('columns', [])])
                    print(f"{name} ({cols})")
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
        except Exception as e:
            import traceback
            print("运行时错误：", f"{type(e).__name__}: {e!s}")
            traceback.print_exc()
            elapsed = time.perf_counter() - start_all
            print(f"（耗时 {elapsed:.6f} s）")
            continue

        # ---------- 输出结果（行集或消息），并显示耗时 ----------
        if out.get("ok") and "rows" in out:
            rows = out["rows"]
            if not rows:
                print("(空集)")
            else:
                cols = list(rows[0].keys())
                # 计算每列宽度（包含表头）
                widths = []
                header_row = {c: c for c in cols}
                temp_rows = rows + [header_row]
                for c in cols:
                    widths.append(max(len(str(r.get(c, ""))) for r in temp_rows))
                # 表头
                header = " | ".join(c.ljust(w) for c, w in zip(cols, widths))
                print(header)
                print("-+-".join("-"*w for w in widths))
                # 数据
                for r in rows:
                    print(" | ".join(str(r.get(c, "")).ljust(w) for c, w in zip(cols, widths)))
                print(f"(共 {len(rows)} 行)")
                # 缓存到弹窗桥接层（若可用）
                _store_popup_result(rows)
        else:
            # 非查询语句，打印返回消息或错误
            print(out.get("message") or out.get("error") or out)

        elapsed = time.perf_counter() - start_all
        print(f"（耗时 {elapsed:.6f} s）")

if __name__ == "__main__":
    main()
