# engine/storage_adapter.py
from __future__ import annotations
import os, json
from typing import Any, Dict, Iterable, Optional

# 只使用项目里的页式存储；缺失即报错
try:
    from storage.pager import Pager  # type: ignore
    from storage.buffer_pool import BufferPool  # type: ignore
    try:
        from storage.table_heap import TableHeap, TableMeta  # type: ignore
        _HAS_TABLE_META = True
    except Exception:
        from storage.table_heap import TableHeap  # type: ignore
        TableMeta = None  # type: ignore
        _HAS_TABLE_META = False
    from storage.data_page import DataPageView  # noqa: F401
except Exception as e:
    raise ImportError(
        "无法导入 storage 模块（pager/buffer_pool/table_heap/data_page）。请确认项目结构与模块名无误。"
    ) from e


class StorageAdapter:
    """
    纯页式存储适配器：
      - 每张表一个目录 <data_dir>/<table>/
      - 主数据文件 <table>.mdb
      - 不读不写 meta.json（目录信息交由系统表管理）
    """

    def __init__(self, data_dir: str) -> None:
        self.data_dir = os.path.abspath(data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        self.default_page_size = 4096

    # ---------------- helpers ----------------
    def _table_dir(self, table: str) -> str:
        d = os.path.join(self.data_dir, table)
        os.makedirs(d, exist_ok=True)
        return d

    def _table_paths(self, table: str) -> Dict[str, str]:
        d = self._table_dir(table)
        return {"mdb": os.path.join(d, f"{table}.mdb")}

    def _resolve_page_size(self, pager) -> int:
        """兼容 pager.page_size 为属性或方法；都没有则回退默认值。"""
        try:
            ps = getattr(pager, "page_size", None)
            if callable(ps):
                return int(ps())
            if isinstance(ps, (int, float)):
                return int(ps)
        except Exception:
            pass
        return int(self.default_page_size)

    def _resolve_num_pages(self, pager, file_path: Optional[str], page_size: int) -> int:
        """优先用文件大小推断；不可靠时尝试 pager.num_pages（属性或方法）。"""
        n_pages = 0
        if isinstance(file_path, str) and os.path.exists(file_path):
            try:
                n_pages = os.path.getsize(file_path) // int(page_size)
            except Exception:
                n_pages = 0
        if n_pages <= 1:
            try:
                np = getattr(pager, "num_pages", None)
                if callable(np):
                    n_pages = int(np())
                elif isinstance(np, (int, float)):
                    n_pages = int(np)
            except Exception:
                pass
        return max(1, int(n_pages))

    def _make_meta(self, table: str, pager: Pager, mdb_path: str):
        """
        构造一个最小可用的 TableMeta：
          - 设置 table_id/name；
          - 用文件大小推导 data_pids = [1..N-1]（假定 0 页为元页）。
        若工程里 TableMeta 不需要，可返回 None。
        """
        if not _HAS_TABLE_META or TableMeta is None:
            return None
        # 实例化
        meta = None
        try:
            meta = TableMeta(table_id=1, name=table)  # type: ignore
        except Exception:
            try:
                meta = TableMeta(1, table)            # type: ignore
            except Exception:
                return None
        # 回填 data_pids
        try:
            page_size = self._resolve_page_size(pager)
            file_size = os.path.getsize(mdb_path)
            n_pages = max(0, file_size // int(page_size))
            if hasattr(meta, "data_pids"):
                setattr(meta, "data_pids", list(range(1, n_pages)))
        except Exception:
            pass
        return meta

    def _try_build_heap(self, pager: Pager, bp: BufferPool, table: str, meta: Optional[Any]) -> Any:
        """
        按更安全的顺序尝试不同构造签名，尽量避免把字符串误当 meta：
          1) (pager, bp, meta)
          2) (pager, bp)
          3) (pager,)
          4) (pager, bp, table)   <-- 放最后
        """
        errors = []
        if meta is not None:
            try:
                return TableHeap(pager, bp, meta)  # type: ignore
            except Exception as e:
                errors.append(("TableHeap(pager,bp,meta)", e))
        try:
            return TableHeap(pager, bp)  # type: ignore
        except Exception as e:
            errors.append(("TableHeap(pager,bp)", e))
        try:
            return TableHeap(pager)  # type: ignore
        except Exception as e:
            errors.append(("TableHeap(pager)", e))
        try:
            return TableHeap(pager, bp, table)  # type: ignore
        except Exception as e:
            errors.append(("TableHeap(pager,bp,table)", e))
        msg = "无法构造 TableHeap，尝试的签名：\n" + "\n".join([f" - {sig}: {err}" for sig, err in errors])
        raise RuntimeError(msg)

    # --------------- catalog-facing APIs ---------------
    def create_table(self, table: str, columns: list[dict]) -> Dict[str, Any]:
        """
        创建页式堆文件并返回存储描述（不写 meta.json）。
        注意：不要先手动写空文件，直接让 Pager 创建并初始化元页。
        """
        mdb_path = self._table_paths(table)["mdb"]

        pager = Pager(mdb_path, page_size=self.default_page_size)  # type: ignore
        bp = BufferPool(pager, capacity=64, policy="LRU")          # type: ignore

        meta = self._make_meta(table, pager, mdb_path)
        _ = self._try_build_heap(pager, bp, table, meta)

        try:
            bp.flush_all()
        except Exception:
            pass
        try:
            pager.sync()
        except Exception:
            pass

        return {"kind": "page", "path": mdb_path}

    def open_table(self, table: str, storage_desc: Dict[str, Any]):
        """
        打开表并返回一个 6 元组：
          ("page", heap, bp, pager, meta, meta_path)
        其中 meta_path 恒为 None（不再使用 meta.json）。
        """
        if storage_desc.get("kind") != "page":
            raise ValueError("存储描述与页式存储不匹配（kind!=page）。")

        mdb_path = storage_desc["path"]
        pager = Pager(mdb_path, page_size=self.default_page_size)  # type: ignore
        bp = BufferPool(pager, capacity=64, policy="LRU")          # type: ignore

        meta = self._make_meta(table, pager, mdb_path)
        heap = self._try_build_heap(pager, bp, table, meta)
        meta_path = None
        return ("page", heap, bp, pager, meta, meta_path)

    # ---------------- row ops ----------------
    def insert_row(self, open_obj, row: Dict[str, Any]) -> Any:
        """
        将行对象编码为 JSON -> bytes，调用底层堆 insert，随后 flush+sync。
        返回底层提供的 RID（若有）。
        """
        _, heap, bp, pager, meta, meta_path = open_obj
        payload = json.dumps(row, ensure_ascii=False).encode("utf-8")
        rid = heap.insert(payload)  # type: ignore

        try:
            bp.flush_all()
        except Exception:
            pass
        try:
            pager.sync()
        except Exception:
            pass
        return rid

    def scan_rows(self, open_obj) -> Iterable[Dict[str, Any]]:
        """
        优先使用 TableHeap.scan()；若其实现依赖 meta.data_pids 而返回空/报错，
        自动回退到“原始页扫描”：用 Pager 逐页读取 + DataPageView 逐槽解析。
        """
        _, heap, bp, pager, meta, meta_path = open_obj

        # 1) 优先尝试 heap.scan()
        try:
            it = heap.scan()  # 预期 yield (rid, bytes)
            got_any = False
            for (_rid, data) in it:           # type: ignore
                got_any = True
                try:
                    yield json.loads(data.decode("utf-8"))
                except Exception:
                    continue
            if got_any:
                return
        except Exception:
            pass

        # 2) 兜底：按页扫描（跳过 0 号元页）
        page_size = self._resolve_page_size(pager)
        file_path = getattr(pager, "path", None) or getattr(pager, "file_path", None)
        n_pages = self._resolve_num_pages(pager, file_path, page_size)

        for pid in range(1, n_pages):
            buf = None
            try:
                if hasattr(pager, "read_page"):
                    buf = pager.read_page(pid)                # bytes/bytearray/memoryview
                elif hasattr(bp, "get_page"):
                    buf = bp.get_page(pid)                    # 部分实现从缓冲池取页
            except Exception:
                continue
            if buf is None:
                continue
            try:
                mv = memoryview(buf)
                if mv.readonly:
                    mv = memoryview(bytearray(mv))            # DataPageView 需要可写 mv
                page = DataPageView(mv)
                for sid in page.iter_slots():
                    try:
                        payload = page.read_record(sid)
                        obj = json.loads(payload.decode("utf-8"))
                        yield obj
                    except Exception:
                        continue
            except Exception:
                continue

    def clear_table(self, open_obj) -> None:
        """
        清空表：删除 .mdb 文件（不重建空文件）。
        注意：该操作会使当前 open_obj 失效；调用处应在需要时重新 create/open。
        """
        _, heap, bp, pager, meta, meta_path = open_obj
        file_path = getattr(pager, "path", None) or getattr(pager, "file_path", None)

        try:
            bp.flush_all()
        except Exception:
            pass
        try:
            pager.close()
        except Exception:
            pass

        if isinstance(file_path, str):
            try:
                os.remove(file_path)
            except Exception:
                pass
