# engine/cli/poptable_child.py
from __future__ import annotations
import sys, json

def main():
    try:
        title = sys.argv[1] if len(sys.argv) > 1 else "查询结果"
    except Exception:
        title = "查询结果"
    try:
        data_bytes = sys.stdin.buffer.read()
        table = json.loads(data_bytes.decode("utf-8")) if data_bytes else {"columns": [], "rows": []}
    except Exception as e:
        # 读不到数据也给个错误弹窗
        from . import poptable  # type: ignore
        poptable.show_table_popup({"columns": ["error"], "rows": [[str(e)]]}, title="数据错误")
        return
    from . import poptable  # type: ignore
    poptable.show_table_popup(table, title=title)

if __name__ == "__main__":
    main()
