# engine/storage_adapter.py
from __future__ import annotations
import os, json
from typing import Any, Dict, Iterable, Optional

# 强制使用 storage/ 下的页式存储；若导入失败，直接报错，绝不回退 JSONL。
try:
    from storage.pager import Pager  # type: ignore
    from storage.buffer_pool import BufferPool  # type: ignore
    try:
        # TableHeap/Meta 的命名在不同作业实现可能略不同，这里尽量兼容
        from storage.table_heap import TableHeap, TableMeta  # type: ignore
        _HAS_TABLE_META = True
    except Exception:
        from storage.table_heap import TableHeap  # type: ignore
        TableMeta = None  # type: ignore
        _HAS_TABLE_META = False
    # DataPageView 并不直接在这里使用，但确保实现存在
    from storage.data_page import DataPageView  # noqa: F401
except Exception as e:
    raise ImportError("无法导入 storage 模块（pager/buffer_pool/table_heap/data_page）。请确认项目结构与模块名无误。") from e

class StorageAdapter:
    """
    页式存储适配：
      - 每张表一个目录 <data_dir>/<table>/
      - 主数据文件 <table>.mdb
      - 元信息文件 meta.json （用于保存 TableMeta 的必要字段，供下次打开时恢复）
    注意：不提供 JSONL 回退；如果 storage 包缺失会直接抛错。
    """
    def __init__(self, data_dir: str) -> None:
        self.data_dir = os.path.abspath(data_dir)
        os.makedirs(self.data_dir, exist_ok=True)

    # --- helpers ---
    def _table_dir(self, table: str) -> str:
        d = os.path.join(self.data_dir, table)
        os.makedirs(d, exist_ok=True)
        return d

    def _table_paths(self, table: str) -> Dict[str, str]:
        d = self._table_dir(table)
        return {
            "mdb": os.path.join(d, f"{table}.mdb"),
            "meta": os.path.join(d, "meta.json"),
        }

    def _try_build_heap(self, pager: Pager, bp: BufferPool, table: str, meta: Optional[Any]) -> Any:
        """兼容不同 TableHeap 构造函数签名。"""
        errors = []
        if meta is not None:
            try:
                return TableHeap(pager, bp, meta)  # type: ignore
            except Exception as e:
                errors.append(("TableHeap(pager,bp,meta)", e))
        # 尝试 (pager,bp,table_name)
        try:
            return TableHeap(pager, bp, table)  # type: ignore
        except Exception as e:
            errors.append(("TableHeap(pager,bp,table)", e))
        # 尝试 (pager,bp)
        try:
            return TableHeap(pager, bp)  # type: ignore
        except Exception as e:
            errors.append(("TableHeap(pager,bp)", e))
        # 尝试 (pager,)
        try:
            return TableHeap(pager)  # type: ignore
        except Exception as e:
            errors.append(("TableHeap(pager)", e))
        # 如果都失败，抛出详细错误
        msg = "无法构造 TableHeap，尝试的签名：\n" + "\n".join([f" - {sig}: {err}" for sig, err in errors])
        raise RuntimeError(msg)

    # --- catalog-facing APIs ---
    def create_table(self, table: str, columns: list[dict]) -> Dict[str, Any]:
        paths = self._table_paths(table)
        mdb_path, meta_path = paths["mdb"], paths["meta"]

        pager = Pager(mdb_path, page_size=4096)  # type: ignore
        bp = BufferPool(pager, capacity=64, policy="LRU")  # type: ignore

        # 构造 Meta（若实现中存在）
        meta = None
        if _HAS_TABLE_META and TableMeta is not None:  # type: ignore
            try:
                meta = TableMeta(table_id=1, name=table)  # type: ignore
            except Exception:
                # 有的实现用不同参数名
                try:
                    meta = TableMeta(1, table)  # type: ignore
                except Exception:
                    meta = None

        # 先构建堆，促使底层初始化必要的结构
        heap = self._try_build_heap(pager, bp, table, meta)

        # 将列信息与可能的 meta 字段落到 meta.json（便于下次 open）
        meta_obj: Dict[str, Any] = {"table": table, "columns": columns}
        if meta is not None:
            for k in ("table_id", "name", "data_pids", "fsm"):
                if hasattr(meta, k):
                    meta_obj[k] = getattr(meta, k)
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta_obj, f, ensure_ascii=False, indent=2)

        # 尝试刷盘
        try:
            bp.flush_all()
        except Exception:
            pass
        try:
            pager.sync()
        except Exception:
            pass

        return {"kind": "page", "path": mdb_path, "meta": meta_path}

    def open_table(self, table: str, storage_desc: Dict[str, Any]):
        if storage_desc.get("kind") != "page":
            raise ValueError("存储描述与页式存储不匹配（kind!=page）。")

        mdb_path = storage_desc["path"]
        meta_path = storage_desc.get("meta")

        pager = Pager(mdb_path, page_size=4096)  # type: ignore
        bp = BufferPool(pager, capacity=64, policy="LRU")  # type: ignore

        meta = None
        if _HAS_TABLE_META and TableMeta is not None:  # type: ignore
            # 尽力从 meta.json 恢复
            if isinstance(meta_path, str) and os.path.exists(meta_path):
                try:
                    with open(meta_path, "r", encoding="utf-8") as f:
                        m = json.load(f)
                    # 最小必要字段
                    table_id = m.get("table_id", 1)
                    name = m.get("name", table)
                    try:
                        meta = TableMeta(table_id=table_id, name=name)  # type: ignore
                    except Exception:
                        meta = TableMeta(table_id, name)  # type: ignore
                    # 尝试回填扩展字段
                    if hasattr(meta, "data_pids") and isinstance(m.get("data_pids"), list):
                        meta.data_pids = list(m["data_pids"])
                    if hasattr(meta, "fsm") and isinstance(m.get("fsm"), dict):
                        meta.fsm = dict(m["fsm"])
                except Exception:
                    # meta.json损坏也不致命，meta=None 走兼容构造
                    meta = None

        heap = self._try_build_heap(pager, bp, table, meta)
        return ("page", heap, bp, pager, meta, meta_path)

    # --- row ops ---
    def insert_row(self, open_obj, row: Dict[str, Any]) -> None:
        _, heap, bp, pager, meta, meta_path = open_obj
        payload = json.dumps(row, ensure_ascii=False).encode("utf-8")
        rid = heap.insert(payload)  # type: ignore  # (page_id, slot_id) 之类，具体实现不依赖这里

        # 持久化 meta（若有）
        try:
            if meta_path:
                meta_obj: Dict[str, Any] = {"table": getattr(meta, "name", None) or "unknown"}
                # 如果 catalog 那边有列，可以不在这里写；为了稳妥，尽量保留原 meta.json 内容
                if os.path.exists(meta_path):
                    try:
                        with open(meta_path, "r", encoding="utf-8") as f:
                            old = json.load(f)
                        if isinstance(old, dict):
                            meta_obj.update(old)
                    except Exception:
                        pass
                # 回填 meta 的运行时字段
                if meta is not None:
                    for k in ("table_id", "name", "data_pids", "fsm"):
                        if hasattr(meta, k):
                            meta_obj[k] = getattr(meta, k)
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(meta_obj, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

        try:
            bp.flush_all()
        except Exception:
            pass
        try:
            pager.sync()
        except Exception:
            pass

    def scan_rows(self, open_obj) -> Iterable[Dict[str, Any]]:
        _, heap, bp, pager, meta, meta_path = open_obj
        for (_rid, data) in heap.scan():  # type: ignore
            try:
                yield json.loads(data.decode("utf-8"))
            except Exception:
                continue

    def clear_table(self, open_obj) -> None:
        """简单实现：重建 mdb 文件。更细粒度的删除/回收请在 TableHeap 内实现。"""
        _, heap, bp, pager, meta, meta_path = open_obj
        try:
            pager.close()
        except Exception:
            pass
        file_path = getattr(pager, "path", None) or getattr(pager, "file_path", None)  # 兼容不同命名
        if isinstance(file_path, str):
            try:
                os.remove(file_path)
            except Exception:
                pass
            # 重新创建空表文件
            self.create_table(getattr(meta, "name", None) or "unknown", [])
