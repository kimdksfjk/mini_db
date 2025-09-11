# engine/mysql_cli.py
from __future__ import annotations
import argparse, sys, json, time, re, importlib, inspect
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

BANNER = (
    "欢迎使用 mini_db 客户端。\n"
    "语句以 ; 或 \\g 结束。输入 \\h 或 \\? 查看帮助；输入 \\q 退出。\n"
)

HELP = (
    "MySQL 风格命令：\n"
    "  SHOW TABLES;\n"
    "  DESCRIBE <table>;        -- 或：DESC <table>;\n"
    "  \\q, quit, exit           -- 退出\n"
    "  \\h, \\?                   -- 帮助\n"
)

PROMPT = "mini_db> "
CONT_PROMPT = "    -> "

def print_table(rows: List[Dict[str, Any]], cols: Optional[List[str]] = None, elapsed: float = 0.0):
    import re
    AS_SPLIT = re.compile(r"\s+AS\s+", flags=re.IGNORECASE)

    def pretty(col: str) -> str:
        # 优先显示别名：COUNT(*) AS cnt -> cnt
        parts = AS_SPLIT.split(col, maxsplit=1)
        if len(parts) == 2 and parts[1]:
            return parts[1]
        # 没有别名则去掉表前缀：s.name -> name
        if "." in col:
            return col.split(".")[-1]
        return col

    if not rows:
        print(f"空集（{elapsed:.2f} 秒）")
        return

    if cols is None or cols == ["*"]:
        keys: List[str] = []
        for r in rows:
            for k in r.keys():
                if k not in keys:
                    keys.append(k)
        cols = keys

    headers = [pretty(c) for c in cols]
    widths = [len(h) for h in headers]
    for r in rows:
        for i, c in enumerate(cols):
            val = r.get(c, "NULL")
            s = "NULL" if val is None else str(val)
            widths[i] = max(widths[i], len(s))

    def line():
        return "+" + "+".join("-" * (w + 2) for w in widths) + "+"

    print(line())
    print("| " + " | ".join(headers[i].ljust(widths[i]) for i in range(len(cols))) + " |")
    print(line())
    for r in rows:
        print("| " + " | ".join((("NULL" if r.get(c) is None else str(r.get(c))).ljust(widths[i])) for i, c in enumerate(cols)) + " |")
    print(line())
    print(f"{len(rows)} 行记录（{elapsed:.2f} 秒）")

def ok(msg: str, elapsed: float = 0.0):
    print(f"{msg}（{elapsed:.2f} 秒）")

def read_statement() -> Optional[str]:
    """逐行读取，直到遇到分号 ; 或 \\g（不在引号内）为止。"""
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
                if ch == '\\' and i + 1 < len(buf):
                    i += 2; continue
                if ch == quote:
                    in_str = False
                i += 1; continue

            if ch in ("'", '"'):
                in_str = True; quote = ch; i += 1; continue

            if ch == ';':
                return buf.strip()

            if ch == '\\' and i + 1 < len(buf) and buf[i+1] == 'g':
                return buf[:i].strip()

            i += 1

# --------- SHOW/DESC helpers ---------
def show_tables(catalog: Catalog):
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
    """封装类或函数风格的编译器（需具备 compile(sql) 或返回 dict 的可调用对象）。"""
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
    """spec 如 'sql.sql_compiler:SQLCompiler' 或 'pkg.mod:compile_to_plan'。"""
    tried = []
    if spec:
        if ":" not in spec:
            raise SystemExit("参数 --compiler 需要形如 '<module>:<symbol>'")
        mod, sym = spec.split(":", 1)
        try:
            m = importlib.import_module(mod)
            return _CompilerWrapper(getattr(m, sym))
        except Exception as e:
            raise SystemExit(f"导入编译器 '{spec}' 失败：{e}")
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
    raise SystemExit("未找到 SQL 编译器。请通过 --compiler 指定。Tried: " + "; ".join(tried))

# --------- New: 直接转发编译器的错误 ---------
def print_compiler_error(res: Dict[str, Any]) -> None:
    """
    将 SQLCompiler.compile 返回的错误结构原样美化输出。
    兼容：
      - {'success': False, 'error_type': 'SYNTAX_ERROR', 'message': '...', 'line': 1, 'column': 5, 'line_text': '...', 'pointer': '    ^'}
      - {'success': False, 'error_type': 'SEMANTIC_ERROR', 'message': '...'}
      - {'success': False, 'error': '...'}
    """
    et = str(res.get("error_type", "")).upper()
    if et == "SYNTAX_ERROR":
        line = res.get("line", "?")
        col = res.get("column", "?")
        msg = res.get("message", "")
        print(f"✗ 语法错误: 行{line} 列{col} - {msg}")
        lt = res.get("line_text")
        if lt is not None:
            print(lt)
        ptr = res.get("pointer")
        if ptr is not None:
            print(ptr)
        return
    if et == "SEMANTIC_ERROR":
        print(f"✗ 语义错误: {res.get('message','')}")
        return
    if et:  # 其他类型
        print(f"✗ {et}: {res.get('message','')}")
        return
    # 兼容旧版只返回 error 的情况
    if "error" in res:
        print(f"✗ 编译失败: {res.get('error')}")
    else:
        print("✗ 编译失败")

def main(argv=None):
    ap = argparse.ArgumentParser(prog="mini_db_mysql", description="mini_db 的 MySQL 风格交互客户端")
    ap.add_argument("--data", default="data", help="数据目录（默认：data）")
    ap.add_argument("--compiler", default=None, help="SQL 编译器，如 'sql.sql_compiler:SQLCompiler'")
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

        # 内置命令：SHOW/DESC
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

        # 编译 & 执行
        try:
            t0 = time.perf_counter()
            res = compiler.compile(stmt)
            if not isinstance(res, dict):
                print("✗ 编译失败：编译器未返回 dict"); continue

            # 如果编译失败，直接转发编译器的错误并继续下一条
            if res.get("success") is False:
                print_compiler_error(res)
                continue

            plan = res.get("execution_plan") if "execution_plan" in res else res
            if not isinstance(plan, dict) or "type" not in plan:
                print("✗ 编译失败：编译器未返回有效的执行计划（缺少 'type'）")
                continue

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
            print(f"错误 未实现: {e}")
        except Exception as e:
            # 这里是执行阶段或我们这边的异常（非编译器错误）
            print(f"错误 {type(e).__name__}: {e}")

if __name__ == "__main__":
    main()
