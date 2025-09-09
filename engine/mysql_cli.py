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

BANNER = (
    "Welcome to the mini_db monitor.\n"
    "Commands end with ;  or \\g.  Type \\h or \\? for help.  Type \\q to quit.\n"
)

HELP = r"""
MySQL-ish commands:
  SHOW TABLES;
  DESCRIBE <table>;        -- alias: DESC <table>;
  \q, quit, exit           -- quit the client
  \h, \?                   -- this help
"""

PROMPT = "mini_db> "
CONT_PROMPT = "    -> "

def print_table(rows: List[Dict[str, Any]], cols: Optional[List[str]] = None, elapsed: float = 0.0):
    if not rows:
        print(f"Empty set ({elapsed:.2f} sec)")
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
    print(f"{len(rows)} row{'s' if len(rows)!=1 else ''} in set ({elapsed:.2f} sec)")

def ok(msg: str, elapsed: float = 0.0):
    print(f"{msg} ({elapsed:.2f} sec)")

def read_statement() -> Optional[str]:
    """Read one statement terminated by ';' or '\\g' (not inside quotes)."""
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
                # skip escape like \" or \'
                if ch == '\\' and i + 1 < len(buf):
                    i += 2; continue
                if ch == quote:
                    in_str = False
                i += 1; continue

            if ch in ("'", '"'):
                in_str = True; quote = ch; i += 1; continue

            if ch == ';':
                return buf.strip()

            # support \g terminator
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
    """Wrap either a class with .compile(sql) or a function returning a plan/result."""
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
        raise ValueError("Compiler function must return a dict.")

def load_compiler(spec: Optional[str]) -> _CompilerWrapper:
    """spec like 'sql.sql_compiler:SQLCompiler' or 'pkg.mod:compile_to_plan'."""
    tried = []
    if spec:
        if ":" not in spec:
            raise SystemExit("Value of --compiler must be '<module>:<symbol>'")
        mod, sym = spec.split(":", 1)
        try:
            m = importlib.import_module(mod)
            return _CompilerWrapper(getattr(m, sym))
        except Exception as e:
            raise SystemExit(f"Failed to import compiler '{spec}': {e}")
    # defaults: try local/sibling first
    defaults = [
        ("sql.sql_compiler", "SQLCompiler"),          # ← 你的文件名
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
    raise SystemExit("No SQL compiler found. Provide --compiler '<module>:<symbol>'. Tried: " + "; ".join(tried))

def main(argv=None):
    ap = argparse.ArgumentParser(prog="mini_db_mysql", description="MySQL-like interactive client for mini_db")
    ap.add_argument("--data", default="data", help="data dir (default: data)")
    ap.add_argument("--compiler", default=None, help="SQL compiler path like 'sql.sql_compiler:SQLCompiler'")
    args = ap.parse_args(argv)

    storage = JsonlStorage(data_dir=args.data)
    catalog = Catalog(data_dir=args.data)
    execu = Executor(storage=storage, catalog=catalog)
    compiler = load_compiler(args.compiler)

    print(BANNER)
    while True:
        stmt = read_statement()
        if stmt is None:
            print("Bye")
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
                print(f"ERROR {type(e).__name__}: {e}")
            continue
        # Compile & execute
        try:
            t0 = time.perf_counter()
            res = compiler.compile(stmt)
            if not isinstance(res, dict):
                raise ValueError("Compiler must return dict")
            plan = res.get("execution_plan") if "execution_plan" in res else res
            if "success" in res and not res.get("success"):
                raise ValueError(res.get("error"))
            if not isinstance(plan, dict) or "type" not in plan:
                raise ValueError("Compiler did not return a valid execution plan dict with 'type'.")
            out = execu.execute_sql_plan(plan)
            elapsed = time.perf_counter() - t0
            if plan.get("type") in ("Select","ExtendedSelect"):
                print_table(out, cols=plan.get("columns"), elapsed=elapsed)
            elif plan.get("type") == "Insert":
                affected = sum(int(x.get("affected",0)) for x in out)
                ok(f"Query OK, {affected} rows affected", elapsed)
            elif plan.get("type") in ("CreateTable", "Delete", "Update"):
                affected = 0
                if plan.get("type") in ("Delete","Update"):
                    affected = sum(int(x.get("affected",0)) for x in out)
                ok(f"Query OK, {affected} rows affected", elapsed)
            else:
                ok("Query OK", elapsed)
        except NotImplementedError as e:
            print(f"ERROR NotImplemented: {e}")
        except Exception as e:
            print(f"ERROR {type(e).__name__}: {e}")

if __name__ == "__main__":
    main()
