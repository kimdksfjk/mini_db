# engine/storage_iface.py
from __future__ import annotations
import os, json, importlib, sys
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Callable, Optional

# ============ 抽象接口（保持不变） ============
class Storage:
    def create_table(self, table: str, columns: List[Dict[str, Any]]) -> None: ...
    def drop_table(self, table: str) -> None: ...
    def scan(self, table: str) -> Iterable[Dict[str, Any]]: ...
    def insert(self, table: str, row: Dict[str, Any]) -> int: ...
    def delete_where(self, table: str, pred: Callable[[Dict[str,Any]], bool]) -> int: ...
    def update_where(self, table: str, pred: Callable[[Dict[str,Any]], bool], setter: Callable[[Dict[str,Any]], Dict[str,Any]]) -> int: ...


# ============ 现有：Json 行存储（保留） ============
class JsonlStorage(Storage):
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)

    def _path(self, table: str) -> str:
        return os.path.join(self.data_dir, f"{table}.jsonl")

    def create_table(self, table: str, columns: List[Dict[str, Any]]) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        if not os.path.exists(self._path(table)):
            open(self._path(table), "w", encoding="utf-8").close()

    def drop_table(self, table: str) -> None:
        p = self._path(table)
        if os.path.exists(p):
            os.remove(p)

    def scan(self, table: str) -> Iterable[Dict[str, Any]]:
        p = self._path(table)
        if not os.path.exists(p):
            return
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)

    def insert(self, table: str, row: Dict[str, Any]) -> int:
        with open(self._path(table), "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
        return 1

    def delete_where(self, table: str, pred, *_args) -> int:
        p = self._path(table)
        if not os.path.exists(p): return 0
        tmp = p + ".tmp"; cnt = 0
        with open(p, "r", encoding="utf-8") as r, open(tmp, "w", encoding="utf-8") as w:
            for line in r:
                if not line.strip(): continue
                row = json.loads(line)
                if pred(row):
                    cnt += 1
                else:
                    w.write(json.dumps(row, ensure_ascii=False) + "\n")
        os.replace(tmp, p)
        return cnt

    def update_where(self, table: str, pred, setter) -> int:
        p = self._path(table)
        if not os.path.exists(p): return 0
        tmp = p + ".tmp"; cnt = 0
        with open(p, "r", encoding="utf-8") as r, open(tmp, "w", encoding="utf-8") as w:
            for line in r:
                if not line.strip(): continue
                row = json.loads(line)
                if pred(row):
                    row = setter(row); cnt += 1
                w.write(json.dumps(row, ensure_ascii=False) + "\n")
        os.replace(tmp, p)
        return cnt


# ============ 新增：HeapStorage 适配你同学的堆表实现 ============
@dataclass
class _Handle:
    pager: Any
    bp: Any
    meta: Any   # storage.table_heap.TableMeta
    heap: Any   # storage.table_heap.TableHeap

class HeapStorage(Storage):
    """
    对接 storage/ 下的：pager.py, buffer_pool.py, data_page.py, table_heap.py
    - 每张表：<data_dir>/<table>.mdb  数据文件
             <data_dir>/<table>.meta.json  表内页元数据（data_pids / fsm）
    - 行以 JSON 序列化为 bytes 存取
    """
    def __init__(self, data_dir="data", page_size: int = 4096, bp_capacity: int = 128):
        self.data_dir = data_dir
        self.page_size = page_size
        self.bp_capacity = bp_capacity
        os.makedirs(self.data_dir, exist_ok=True)

        # 兼容你们存储源码里的绝对导入（from pager import ...）
        m_pager = importlib.import_module("storage.pager")
        sys.modules.setdefault("pager", m_pager)
        m_bp    = importlib.import_module("storage.buffer_pool")
        sys.modules.setdefault("buffer_pool", m_bp)
        m_dp    = importlib.import_module("storage.data_page")
        sys.modules.setdefault("data_page", m_dp)
        m_th    = importlib.import_module("storage.table_heap")

        self.Pager = m_pager.Pager
        self.BufferPool = m_bp.BufferPool
        self.TableMeta = m_th.TableMeta
        self.TableHeap = m_th.TableHeap

        self._handles: Dict[str, _Handle] = {}

    # ---------- 路径 ----------
    def _db_path(self, table: str) -> str:
        return os.path.join(self.data_dir, f"{table}.mdb")

    def _meta_path(self, table: str) -> str:
        return os.path.join(self.data_dir, f"{table}.meta.json")

    # ---------- 编解码 ----------
    def _encode(self, row: Dict[str, Any]) -> bytes:
        return json.dumps(row, ensure_ascii=False).encode("utf-8")

    def _decode(self, data: bytes) -> Dict[str, Any]:
        return json.loads(data.decode("utf-8"))

    # ---------- 元数据 ----------
    def _load_meta(self, table: str) -> Any:
        mp = self._meta_path(table)
        if os.path.exists(mp):
            with open(mp, "r", encoding="utf-8") as f:
                obj = json.load(f)
            return self.TableMeta(
                table_id=int(obj.get("table_id", 1)),
                name=obj.get("name", table),
                data_pids=list(obj.get("data_pids", [])),
                fsm=dict((int(k), int(v)) for k, v in (obj.get("fsm", {}) or {}).items()),
            )
        else:
            return self.TableMeta(table_id=1, name=table, data_pids=[], fsm={})

    def _save_meta(self, table: str, meta: Any) -> None:
        mp = self._meta_path(table)
        obj = {
            "table_id": int(getattr(meta, "table_id", 1)),
            "name": getattr(meta, "name", table),
            "data_pids": list(getattr(meta, "data_pids", [])),
            "fsm": {str(k): int(v) for k, v in (getattr(meta, "fsm", {}) or {}).items()},
        }
        with open(mp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False)

    # ---------- 句柄缓存 ----------
    def _open(self, table: str) -> _Handle:
        h = self._handles.get(table)
        if h: return h
        pager = self.Pager(self._db_path(table), page_size=self.page_size)
        bp    = self.BufferPool(pager, capacity=self.bp_capacity, policy="LRU")
        meta  = self._load_meta(table)
        heap  = self.TableHeap(pager, bp, meta)
        h = _Handle(pager=pager, bp=bp, meta=meta, heap=heap)
        self._handles[table] = h
        return h

    def _close_table(self, table: str) -> None:
        h = self._handles.pop(table, None)
        if not h: return
        try:
            self._save_meta(table, h.meta)
            h.bp.flush_all()
            h.pager.close()
        except Exception:
            pass

    # ========== Storage 接口实现 ==========
    def create_table(self, table: str, columns: List[Dict[str, Any]]) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        # 创建空文件并写入 meta page
        if not os.path.exists(self._db_path(table)):
            self.Pager(self._db_path(table), page_size=self.page_size).close()
        # 初始化表元数据
        if not os.path.exists(self._meta_path(table)):
            self._save_meta(table, self.TableMeta(table_id=1, name=table, data_pids=[], fsm={}))

    def drop_table(self, table: str) -> None:
        # 关闭句柄 -> 删文件
        self._close_table(table)
        for p in (self._db_path(table), self._meta_path(table)):
            if os.path.exists(p):
                os.remove(p)

    def scan(self, table: str) -> Iterable[Dict[str, Any]]:
        if not os.path.exists(self._db_path(table)):
            return
        h = self._open(table)
        # table_heap.scan() 产出 (RID, bytes)
        for rid, payload in h.heap.scan():
            yield self._decode(payload)

    def insert(self, table: str, row: Dict[str, Any]) -> int:
        h = self._open(table)
        h.heap.insert(self._encode(row))
        self._save_meta(table, h.meta)
        h.bp.flush_all()
        return 1

    def delete_where(self, table: str, pred: Callable[[Dict[str,Any]], bool]) -> int:
        h = self._open(table)
        # 两遍更稳妥：先选中 RID，再执行删除，避免边扫描边修改
        victims: List[Any] = []
        for rid, payload in h.heap.scan():
            row = self._decode(payload)
            if pred(row):
                victims.append(rid)
        for rid in victims:
            h.heap.delete(rid)
        self._save_meta(table, h.meta)
        h.bp.flush_all()
        return len(victims)

    def update_where(self, table: str, pred: Callable[[Dict[str,Any]], bool], setter: Callable[[Dict[str,Any]], Dict[str,Any]]) -> int:
        h = self._open(table)
        updates: List[tuple] = []
        for rid, payload in h.heap.scan():
            row = self._decode(payload)
            if pred(row):
                new_row = setter(row)
                updates.append((rid, self._encode(new_row)))
        for rid, new_payload in updates:
            h.heap.update(rid, new_payload)
        self._save_meta(table, h.meta)
        h.bp.flush_all()
        return len(updates)
