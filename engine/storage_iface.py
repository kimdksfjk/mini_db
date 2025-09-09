from __future__ import annotations
from typing import Any, Dict, Iterable, Callable, List, Optional
import os, json

class Storage:
    def create_table(self, table: str, schema: List[Dict[str, Any]]) -> None: ...
    def drop_table(self, table: str) -> None: ...
    def append_row(self, table: str, row: Dict[str, Any]) -> None: ...
    def scan_rows(self, table: str) -> Iterable[Dict[str, Any]]: ...
    def delete_where(self, table: str, predicate: Callable[[Dict[str, Any]], bool]) -> int: ...
    def update_where(self, table: str, predicate: Callable[[Dict[str, Any]], bool], updater: Callable[[Dict[str, Any]], Dict[str, Any]]) -> int: ...

class JsonlStorage(Storage):
    """Simplest JSONL-backed storage. Each row is a JSON object per line."""
    def __init__(self, data_dir: str = "data") -> None:
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)

    def _path(self, table: str) -> str:
        return os.path.join(self.data_dir, f"{table}.jsonl")

    def create_table(self, table: str, schema: List[Dict[str, Any]]) -> None:
        p = self._path(table)
        if not os.path.exists(p):
            open(p, "w", encoding="utf-8").close()

    def drop_table(self, table: str) -> None:
        p = self._path(table)
        if os.path.exists(p):
            os.remove(p)

    def append_row(self, table: str, row: Dict[str, Any]) -> None:
        p = self._path(table)
        with open(p, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def scan_rows(self, table: str):
        p = self._path(table)
        if not os.path.exists(p):
            return
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: 
                    continue
                yield json.loads(line)

    def _rewrite(self, table: str, keep_or_update: Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]) -> int:
        p = self._path(table)
        if not os.path.exists(p):
            return 0
        tmp = p + ".tmp"
        count = 0
        with open(p, "r", encoding="utf-8") as src, open(tmp, "w", encoding="utf-8") as dst:
            for line in src:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                new_row = keep_or_update(row)
                if new_row is None:
                    count += 1  # deleted
                    continue
                dst.write(json.dumps(new_row, ensure_ascii=False) + "\n")
        os.replace(tmp, p)
        return count

    def delete_where(self, table: str, predicate, /) -> int:
        return self._rewrite(table, lambda r: None if predicate(r) else r)

    def update_where(self, table: str, predicate, updater) -> int:
        changed = 0
        def fn(r):
            nonlocal changed
            if predicate(r):
                changed += 1
                return updater(r)
            return r
        self._rewrite(table, fn)
        return changed
