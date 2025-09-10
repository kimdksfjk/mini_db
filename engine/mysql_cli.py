# mini_db/engine/mysql_cli.py
from __future__ import annotations
import argparse, sys, json, os, time, re, importlib, inspect
from typing import Any, Dict, List, Optional

# Engine parts
try:
    from .executor import Executor
    from .storage_iface import JsonlStorage
    from .catalog import Catalog
except ImportError:
    from engine.executor import Executor
    from engine.storage_iface import JsonlStorage
    from engine.catalog import Catalog

# ===== 中文化提示 =====
BANNER = (
    "欢迎使用 mini_db 客户端。\n"
    "语句以 ; 或 \\g 结束。输入 \\h 或 \\? 查看帮助；输入 \\q 退出。\n"
)

HELP = r"""
类 MySQL 命令：
  SHOW TABLES;
  DESCRIBE <表名>;         -- 或：DESC <表名>;
  \q, quit, exit           -- 退出客户端
  \h, \?                   -- 查看帮助
"""

PROMPT = "mini_db> "
CONT_PROMPT = "    -> "

def print_table(rows: List[Dict[str, Any]], cols: Optional[List[str]] = None, elapsed: float = 0.0):
    if not rows:
        print(f"空集（{elapsed:.2f} 秒）")
        return
    if cols is None or cols == ["*"]:
        keys = []
        for r in rows:
            for k in r.keys():
                if k not in keys:
                    keys.append(k)
        cols = keys
    widths = [len(c) for c in cols]
    for r in rows:
        for i, c in enumerate(cols):
            val = r.get(c, "NULL")
            s = "NULL" if val is None else str(val)
            widths[i] = max(widths[i], len(s))
    def line():
        return "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    print(line())
    print("| " + " | ".join(c.ljust(widths[i]) for i, c in enumerate(cols)) + " |")
    print(line())
    for r in rows:
        print("| " + " | ".join((("NULL" if r.get(c) is None else str(r.get(c))).ljust(widths[i])) for i, c in enumerate(cols)) + " |")
    print(line())
    print(f"{len(rows)} 行记录（{elapsed:.2f} 秒）")

def ok(msg: str, elapsed: float = 0.0):
    # 把英文提示改为中文
    # 兼容传入英文的场景：若上层传英文，这里不强制替换，只统一加上耗时
    print(f"{msg}（{elapsed:.2f} 秒）")

def read_statement() -> Optional[str]:
    r"""读取一条语句（以 ';' 或 '\g' 结束；引号内的分号不算结束）。"""
    buf = ""
    in_str = False
    quote = None
    first = True
    while True:
        try:
            line = input(PROMPT if first else CONT_PROMPT)
        except EOFError:
            return None
        if not line and first:
            continue
        first = False
        buf += (("" if not buf else "\n") + line)
        stripped = line.strip()
        if stripped in ("\\q", "quit", "exit"):
            return None
        if stripped in ("\\h", "\\?"):
            print(HELP.strip()); buf = ""; first = True; continue

        i = 0
        while i < len(buf):
            ch = buf[i]
            if in_str:
                # 跳过转义字符（如 \" 或 \')
                if ch == '\\' and i + 1 < len(buf):
                    i += 2; continue
                if ch == quote:
                    in_str = False
                i += 1; continue

            if ch in ("'", '"'):
                in_str = True; quote = ch; i += 1; continue

            if ch == ';':
                return buf.strip()

            # 支持 \g 作为结束
            if ch == '\\' and i + 1 < len(buf) and buf[i+1] == 'g':
                return buf[:i].strip()

            i += 1

# --------- SHOW/DESC helpers ---------
def show_tables(catalog: Catalog):
    # 为了兼容 MySQL 的显示风格，列名仍保留原风格；如需中文表头可改成 "表名"
    rows = [{"Tables_in_mini_db": name} for name in catalog.list_tables()]
    return rows, ["Tables_in_mini_db"]

def describe_table(catalog: Catalog, table: str):
    cols = catalog.schema(table)
    rows = []
    for c in cols:
        rows.append({
            "Field": c.get("name"),
            "Type": c.get("type"),
            "Null": "YES",
            "Key": "",
            "Default": None,
            "Extra": "",
        })
    return rows, ["Field","Type","Null","Key","Default","Extra"]

# --------- Compiler loader (supports --compiler) ---------
class _CompilerWrapper:
    """包装编译器（类：有 .compile(sql)，或函数：返回计划/结果的 dict）。"""
    def __init__(self, obj: Any):
        self.obj = obj
        if callable(obj) and not hasattr(obj, "compile"):
            self.kind = "func"
        else:
            self.kind = "class"
            if inspect.isclass(obj):
                self.obj = obj()

    def compile(self, sql: str) -> Dict[str, Any]:
        if self.kind == "class":
            return self.obj.compile(sql)
        res = self.obj(sql)
        if isinstance(res, dict) and "type" in res:
            return {"success": True, "execution_plan": res}
        if isinstance(res, dict):
            return res
        raise ValueError("编译器函数必须返回 dict。")

def load_compiler(spec: Optional[str]) -> _CompilerWrapper:
    """spec 形式：'sql.sql_compiler:SQLCompiler' 或 'pkg.mod:compile_to_plan'。"""
    tried = []
    if spec:
        if ":" not in spec:
            raise SystemExit("参数 --compiler 的格式应为：'<模块>:<符号>'")
        mod, sym = spec.split(":", 1)
        try:
            m = importlib.import_module(mod)
            return _CompilerWrapper(getattr(m, sym))
        except Exception as e:
            raise SystemExit(f"导入编译器 '{spec}' 失败：{e}")
    # 默认尝试位置（按你们的文件名）
    defaults = [
        ("sql.sql_compiler", "SQLCompiler"),
        ("mini_db.sql.sql_compiler", "SQLCompiler"),
        ("mini_db.sql.compiler", "SQLCompiler"),
        ("sql.compiler", "SQLCompiler"),
    ]
    for mod, sym in defaults:
        try:
            m = importlib.import_module(mod)
            return _CompilerWrapper(getattr(m, sym))
        except Exception as e:
            tried.append(f"{mod}:{sym} -> {e}")
    raise SystemExit("未找到 SQL 编译器。请通过 --compiler '<模块>:<符号>' 指定。已尝试："
                     + "; ".join(tried))

def main(argv=None):
    ap = argparse.ArgumentParser(prog="mini_db_mysql", description="类 MySQL 的 mini_db 交互客户端")
    ap.add_argument("--data", default="data", help="数据目录（默认：data）")
    ap.add_argument("--compiler", default=None, help="SQL 编译器路径，例如：sql.sql_compiler:SQLCompiler")
    args = ap.parse_args(argv)

    storage = JsonlStorage(data_dir=args.data)
    catalog = Catalog(data_dir=args.data)
    execu = Executor(storage=storage, catalog=catalog)
    compiler = load_compiler(args.compiler)

    print(BANNER)
    while True:
        stmt = read_statement()
        if stmt is None:
            print("再见")
            return
        if not stmt.strip():
            continue
        # SHOW/DESC
        if re.match(r"^SHOW\s+TABLES\s*;?$", stmt, flags=re.IGNORECASE):
            rows, cols = show_tables(catalog)
            print_table(rows, cols, elapsed=0.0); continue
        m = re.match(r"^(?:DESCRIBE|DESC)\s+([A-Za-z_][A-Za-z0-9_]*)\s*;?$", stmt, flags=re.IGNORECASE)
        if m:
            try:
                rows, cols = describe_table(catalog, m.group(1))
                print_table(rows, cols, elapsed=0.0)
            except Exception as e:
                print(f"错误 {type(e).__name__}: {e}")
            continue
        # 编译并执行
        try:
            t0 = time.perf_counter()
            res = compiler.compile(stmt)
            if not isinstance(res, dict):
                raise ValueError("编译器返回值必须为 dict")
            plan = res.get("execution_plan") if "execution_plan" in res else res
            if "success" in res and not res.get("success"):
                raise ValueError(res.get("error"))
            if not isinstance(plan, dict) or "type" not in plan:
                raise ValueError("编译器未返回包含 'type' 的有效执行计划。")
            out = execu.execute_sql_plan(plan)
            elapsed = time.perf_counter() - t0
            if plan.get("type") in ("Select","ExtendedSelect"):
                print_table(out, cols=plan.get("columns"), elapsed=elapsed)
            elif plan.get("type") == "Insert":
                affected = sum(int(x.get("affected",0)) for x in out)
                ok(f"执行成功，影响 {affected} 行", elapsed)
            elif plan.get("type") in ("CreateTable", "Delete", "Update"):
                affected = 0
                if plan.get("type") in ("Delete","Update"):
                    affected = sum(int(x.get("affected",0)) for x in out)
                ok(f"执行成功，影响 {affected} 行", elapsed)
            else:
                ok("执行成功", elapsed)
        except NotImplementedError as e:
            print(f"错误 NotImplemented: {e}")
        except Exception as e:
            print(f"错误 {type(e).__name__}: {e}")

if __name__ == "__main__":
    main()
