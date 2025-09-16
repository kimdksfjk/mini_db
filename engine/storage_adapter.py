# engine/storage_adapter.py
from __future__ import annotations
import os, json, atexit
from typing import Any, Dict, Iterable, Optional

# 仅使用项目里的页式存储；缺失即报错
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


# ========= 句柄池（全进程复用） =========
# key = 绝对路径 .mdb
# value = {"pager": Pager, "bp": BufferPool, "ref": int}
_HANDLE_POOL: Dict[str, Dict[str, Any]] = {}


def _acquire_handles(mdb_path: str, page_size: int, capacity: int = 256, policy: str = "LRU") -> tuple[Pager, BufferPool]:
    """获取/复用给定 .mdb 的 Pager/BufferPool，并增加引用计数。"""
    abspath = os.path.abspath(mdb_path)
    ent = _HANDLE_POOL.get(abspath)
    if ent is None:
        pager = Pager(abspath, page_size=page_size)  # type: ignore
        bp = BufferPool(pager, capacity=capacity, policy=policy)  # type: ignore
        _HANDLE_POOL[abspath] = {"pager": pager, "bp": bp, "ref": 1}
        return pager, bp
    ent["ref"] += 1
    return ent["pager"], ent["bp"]


def _release_handles(mdb_path: str, force: bool = False) -> None:
    """
    释放一次引用；当 ref<=0 或 force=True 时，flush/sync/close 并从池中移除。
    Windows 删除文件前务必 force=True，避免句柄占用。
    """
    abspath = os.path.abspath(mdb_path)
    ent = _HANDLE_POOL.get(abspath)
    if ent is None:
        return
    if not force:
        ent["ref"] -= 1
        if ent["ref"] > 0:
            return
    try:
        try:
            ent["bp"].flush_all()
        except Exception:
            pass
        try:
            ent["pager"].sync()
        except Exception:
            pass
        try:
            ent["pager"].close()
        except Exception:
            pass
    finally:
        _HANDLE_POOL.pop(abspath, None)


def _cleanup_pool() -> None:
    """进程退出前的兜底清理。"""
    for path in list(_HANDLE_POOL.keys()):
        _release_handles(path, force=True)

atexit.register(_cleanup_pool)


class StorageAdapter:
    """
    纯页式存储适配器（使用句柄池复用）：
      - 每张表一个目录 <data_dir>/<table>/ ，主文件 <table>.mdb
      - 不读不写 meta.json（系统表负责表级元信息）
      - open_table() 返回的是句柄池中的 pager/buffer_pool，跨语句复用缓存
    """

    def __init__(self, data_dir: str) -> None:
        self.data_dir = os.path.abspath(data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        self.default_page_size = 4096
        self.default_bp_capacity = 256  # 可按需调大以提升命中

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
        meta = None
        try:
            meta = TableMeta(table_id=1, name=table)  # type: ignore
        except Exception:
            try:
                meta = TableMeta(1, table)            # type: ignore
            except Exception:
                return None
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
        依次尝试不同构造签名，尽量规避把字符串误当 meta：
          1) (pager, bp, meta)
          2) (pager, bp)
          3) (pager,)
          4) (pager, bp, table)
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
        步骤：通过句柄池创建一次 .mdb（写入元页），随后立即释放引用。
        """
        mdb_path = self._table_paths(table)["mdb"]
        pager, bp = _acquire_handles(mdb_path, page_size=self.default_page_size, capacity=self.default_bp_capacity)
        try:
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
        finally:
            _release_handles(mdb_path)
        return {"kind": "page", "path": mdb_path}

    def open_table(self, table: str, storage_desc: Dict[str, Any]):
        """
        打开表并返回一个 6 元组：
          ("page", heap, bp, pager, meta, meta_path)
        其中 meta_path 恒为 None（不再使用 meta.json）。
        句柄来自句柄池，可跨语句复用缓存。
        """
        if storage_desc.get("kind") != "page":
            raise ValueError("存储描述与页式存储不匹配（kind!=page）。")
        mdb_path = storage_desc["path"]
        pager, bp = _acquire_handles(mdb_path, page_size=self.default_page_size, capacity=self.default_bp_capacity)
        meta = self._make_meta(table, pager, mdb_path)
        heap = self._try_build_heap(pager, bp, table, meta)
        meta_path = None
        return ("page", heap, bp, pager, meta, meta_path)

    # ---------------- row ops ----------------
    def insert_row(self, open_obj, row: Dict[str, Any]) -> Any:
        """
        将行对象编码为 JSON -> bytes，调用底层堆 insert。
        出于简洁与安全，仍在每次插入后 flush+sync；如需更高吞吐，可在上层批量控制。
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
        自动回退到“原始页扫描”：Pager 逐页 + DataPageView 逐槽解析。
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
                    buf = pager.read_page(pid)
                elif hasattr(bp, "get_page"):
                    buf = bp.get_page(pid)  # 从缓冲池取页
            except Exception:
                continue
            if buf is None:
                continue
            try:
                mv = memoryview(buf)
                if mv.readonly:
                    mv = memoryview(bytearray(mv))  # DataPageView 需要可写 mv
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
        为兼容 Windows，先强制释放句柄池中的资源，再删除文件。
        """
        _, heap, bp, pager, meta, meta_path = open_obj
        file_path = getattr(pager, "path", None) or getattr(pager, "file_path", None)

        if isinstance(file_path, str):
            _release_handles(file_path, force=True)

        if isinstance(file_path, str) and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception:
                pass

    # ---------------- 缓冲池统计（含命中率） ----------------
    def buffer_pool_global_stats(self) -> Dict[str, Any]:
        """
        返回全局统计（优先调用 BufferPool.global_stats；若无则聚合实例）。
        统一计算命中率：hit / (hit + miss)。
        """
        # 1) 优先使用新版 BufferPool 的类方法
        gs = getattr(BufferPool, "global_stats", None)
        if callable(gs):
            data = gs()  # type: ignore
            # 兼容不同键名
            hits = data.get("hits") or data.get("hit") or 0
            misses = data.get("misses") or data.get("miss") or 0
            total = hits + misses
            data["hit_rate"] = (hits / total) if total else 0.0
            return data

        # 2) 聚合所有实例
        agg = {"hit": 0, "miss": 0, "evict": 0}
        for ent in _HANDLE_POOL.values():
            bp = ent.get("bp")
            if bp is None:
                continue
            # stats 属性（旧版）
            st = getattr(bp, "stats", None)
            if isinstance(st, dict):
                agg["hit"] += int(st.get("hit", 0))
                agg["miss"] += int(st.get("miss", 0))
                agg["evict"] += int(st.get("evict", 0))
            else:
                agg["hit"] += int(getattr(bp, "hit", 0))
                agg["miss"] += int(getattr(bp, "miss", 0))
                agg["evict"] += int(getattr(bp, "evict", 0))
        total = agg["hit"] + agg["miss"]
        return {
            "hits": agg["hit"],
            "misses": agg["miss"],
            "evict": agg["evict"],
            "hit_rate": (agg["hit"] / total) if total else 0.0,
        }

    def buffer_pool_instance_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        返回每个 .mdb 的实例统计；尽力兼容不同 BufferPool 实现。
        """
        out: Dict[str, Dict[str, Any]] = {}
        for path, ent in _HANDLE_POOL.items():
            bp = ent.get("bp")
            if bp is None:
                continue
            snap = {}
            # 新版：stats_snapshot()
            ss = getattr(bp, "stats_snapshot", None)
            if callable(ss):
                try:
                    snap = ss()
                except Exception:
                    snap = {}
            # 旧版：stats 属性 / 命中计数
            if not snap:
                st = getattr(bp, "stats", None)
                if isinstance(st, dict):
                    snap = dict(st)
                else:
                    snap = {
                        "hit": int(getattr(bp, "hit", 0)),
                        "miss": int(getattr(bp, "miss", 0)),
                        "evict": int(getattr(bp, "evict", 0)),
                    }
            # 命中率统一补充
            h = snap.get("hits") or snap.get("hit") or 0
            m = snap.get("misses") or snap.get("miss") or 0
            t = h + m
            snap["hit_rate"] = (h / t) if t else 0.0
            out[path] = snap
        return out

    def buffer_pool_hit_rate(self) -> float:
        """
        直接返回全局命中率（方便上层快速展示）。
        """
        s = self.buffer_pool_global_stats()
        return float(s.get("hit_rate", 0.0))
