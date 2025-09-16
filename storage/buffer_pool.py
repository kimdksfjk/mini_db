# buffer_pool.py
from __future__ import annotations

import os
import time
import logging
import threading
from dataclasses import dataclass, asdict
from collections import OrderedDict, deque
from typing import Optional, Dict, Deque, Literal

from .pager import Pager  # 引用已有的 Pager

DEBUG_EVICT = False


@dataclass
class Frame:
    """缓冲池槽位：保存一页数据与控制信息"""
    page_id: int                 # 页号
    data: bytearray              # 页数据
    pin_count: int = 0           # 固定计数（>0 时不可被替换）
    dirty: bool = False          # 脏页标记（需写回）


@dataclass
class BPStats:
    """
    缓冲池统计信息（实例级）
    - hits/misses：命中与未命中次数
    - reads/writes：磁盘读/写次数
    - evict_clean/evict_dirty：被替换的干净/脏页计数
    - pins/unpins：pin/unpin 次数
    - current_resident/max_resident：当前/峰值驻留页数
    - capacity：缓冲池容量
    - start_ts：统计起始时间
    """
    hits: int = 0
    misses: int = 0
    reads: int = 0
    writes: int = 0
    evict_clean: int = 0
    evict_dirty: int = 0
    pins: int = 0
    unpins: int = 0
    current_resident: int = 0
    max_resident: int = 0
    capacity: int = 0
    start_ts: float = 0.0


class _BPDiag:
    """缓冲池全局诊断（跨实例聚合统计 + 可选替换日志）"""
    _global_lock = threading.Lock()
    _global = BPStats(start_ts=time.time())
    _logger: logging.Logger | None = None
    _log_handler: logging.Handler | None = None

    @classmethod
    def add(cls, **delta) -> None:
        with cls._global_lock:
            g = cls._global
            for k, v in delta.items():
                if hasattr(g, k):
                    setattr(g, k, getattr(g, k) + int(v))

    @classmethod
    def snapshot(cls) -> dict:
        with cls._global_lock:
            return asdict(cls._global)

    @classmethod
    def reset(cls) -> None:
        with cls._global_lock:
            cap = cls._global.capacity
            cls._global = BPStats(capacity=cap, start_ts=time.time())

    @classmethod
    def enable_log(cls, path: str | None = None) -> None:
        if cls._logger:
            return
        logger = logging.getLogger("buffer_pool")
        logger.setLevel(logging.INFO)
        if path is None:
            os.makedirs("__logs__", exist_ok=True)
            path = os.path.join("__logs__", "buffer_pool.log")
        handler = logging.FileHandler(path, encoding="utf-8")
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        handler.setFormatter(fmt)
        logger.addHandler(handler)
        cls._logger = logger
        cls._log_handler = handler

    @classmethod
    def disable_log(cls) -> None:
        if cls._logger and cls._log_handler:
            cls._logger.removeHandler(cls._log_handler)
        cls._logger = None
        cls._log_handler = None

    @classmethod
    def log(cls, msg: str) -> None:
        if cls._logger:
            cls._logger.info(msg)


class _LRUPolicy:
    """LRU 候选集合：仅跟踪可替换页（pin==0），命中触发 move_to_end"""
    def __init__(self) -> None:
        self._lru: "OrderedDict[int, None]" = OrderedDict()

    def touch(self, pid: int) -> None:
        self._lru.pop(pid, None)
        self._lru[pid] = None

    def remove(self, pid: int) -> None:
        self._lru.pop(pid, None)

    def victim(self) -> Optional[int]:
        if not self._lru:
            return None
        pid, _ = self._lru.popitem(last=False)
        return pid


class _FIFOPolicy:
    """FIFO 候选集合：仅跟踪可替换页（pin==0）"""
    def __init__(self) -> None:
        self._q: Deque[int] = deque()
        self._in_q: set[int] = set()

    def touch(self, pid: int) -> None:
        if pid not in self._in_q:
            self._q.append(pid)
            self._in_q.add(pid)

    def remove(self, pid: int) -> None:
        if pid in self._in_q:
            self._in_q.remove(pid)

    def victim(self) -> Optional[int]:
        while self._q:
            pid = self._q[0]
            if pid in self._in_q:
                self._q.popleft()
                self._in_q.remove(pid)
                return pid
            self._q.pop()
        return None


class BufferPool:
    """
    页缓冲池
    - 固定容量（按页）
    - get_page：命中返回；未命中读盘；满则按策略淘汰未 pin 的页
    - unpin(dirty)：释放并可选标脏
    - flush_page/flush_all：写回磁盘
    - 统计/日志：实例级与全局级统计，支持替换日志到文件
    """
    def __init__(self,
                 pager: Pager,
                 capacity: int = 128,
                 policy: Literal["LRU", "FIFO"] = "LRU") -> None:
        assert capacity > 0
        self.pager = pager
        self.capacity = capacity
        self.frames: Dict[int, Frame] = {}   # page_id -> Frame
        self.page_table: Dict[int, int] = {} # 便于排错/扩展
        self.hit = 0
        self.miss = 0
        self.evict = 0

        if policy.upper() == "LRU":
            self._policy = _LRUPolicy()
        elif policy.upper() == "FIFO":
            self._policy = _FIFOPolicy()
        else:
            raise ValueError("policy must be 'LRU' or 'FIFO'")

        # 实例统计
        self._stats = BPStats(capacity=capacity, start_ts=time.time())
        # 全局记录容量峰值
        with _BPDiag._global_lock:
            _BPDiag._global.capacity = max(_BPDiag._global.capacity, capacity)

    # -------------------- 对外 API --------------------

    def get_page(self, page_id: int) -> memoryview:
        """
        获取指定页的可写 memoryview；调用者使用完须调用 unpin。
        命中：仅调整计数；未命中：如有必要先淘汰，再从磁盘读取。
        """
        fr = self.frames.get(page_id)
        if fr is not None:
            # 命中
            self.hit += 1
            self._stats.hits += 1
            self._stats.pins += 1
            _BPDiag.add(hits=1, pins=1)
            fr.pin_count += 1
            return memoryview(fr.data)

        # 未命中
        self.miss += 1
        self._stats.misses += 1
        _BPDiag.add(misses=1)

        if len(self.frames) >= self.capacity:
            self._evict_for(page_id)

        raw = self.pager.read_page(page_id)
        self._stats.reads += 1
        _BPDiag.add(reads=1)

        fr = Frame(page_id=page_id, data=bytearray(raw), pin_count=1, dirty=False)
        self.frames[page_id] = fr
        self.page_table[page_id] = page_id

        # 新页驻留
        self._stats.current_resident += 1
        if self._stats.current_resident > self._stats.max_resident:
            self._stats.max_resident = self._stats.current_resident
        self._stats.pins += 1
        _BPDiag.add(pins=1)

        return memoryview(fr.data)

    def unpin(self, page_id: int, dirty: bool = False) -> None:
        """
        解除固定；dirty=True 表示本次修改了页。
        当 pin_count 降为 0，页进入可替换候选集合。
        """
        fr = self._require_frame(page_id)
        if fr.pin_count == 0:
            return
        fr.pin_count -= 1
        self._stats.unpins += 1
        _BPDiag.add(unpins=1)
        if dirty:
            fr.dirty = True
        if fr.pin_count == 0:
            self._policy.touch(page_id)

    def flush_page(self, page_id: int) -> None:
        """写回单页（仅脏页）"""
        fr = self.frames.get(page_id)
        if fr and fr.dirty:
            self.pager.write_page(page_id, bytes(fr.data))
            fr.dirty = False
            self._stats.writes += 1
            _BPDiag.add(writes=1)

    def flush_all(self) -> None:
        """写回所有脏页，并进行磁盘同步"""
        for pid, fr in list(self.frames.items()):
            if fr.dirty:
                self.pager.write_page(pid, bytes(fr.data))
                fr.dirty = False
                self._stats.writes += 1
                _BPDiag.add(writes=1)
        self.pager.sync()

    @property
    def stats(self) -> dict:
        """
        兼容旧接口的简要统计：
        - hit/miss/evict 与命中率
        """
        total = self.hit + self.miss
        return {
            "capacity": self.capacity,
            "cached": len(self.frames),
            "hit": self.hit,
            "miss": self.miss,
            "evict": self.evict,
            "hit_rate": (self.hit / total) if total else 0.0,
        }

    def stats_snapshot(self) -> dict:
        """返回实例级详细统计（BPStats 全量字段）"""
        return asdict(self._stats)

    @staticmethod
    def global_stats() -> dict:
        """返回全局聚合统计"""
        return _BPDiag.snapshot()

    @staticmethod
    def reset_global_stats() -> None:
        """重置全局统计"""
        _BPDiag.reset()

    @staticmethod
    def enable_global_log(path: str | None = None) -> None:
        """开启替换日志（可指定文件路径）"""
        _BPDiag.enable_log(path)

    @staticmethod
    def disable_global_log() -> None:
        """关闭替换日志"""
        _BPDiag.disable_log()

    def reset_stats(self) -> None:
        """重置旧版简要统计"""
        self.hit = 0
        self.miss = 0
        self.evict = 0

    def report_stats(self) -> None:
        """打印旧版简要统计"""
        s = self.stats
        print(f"[STATS] cap={s['capacity']} cached={s['cached']} "
              f"hit={s['hit']} miss={s['miss']} evict={s['evict']} "
              f"hit_rate={s['hit_rate']:.2%}")

    # -------------------- 内部方法 --------------------

    def _evict_for(self, incoming_pid: int) -> None:
        """
        淘汰一个可替换页以给 incoming_pid 腾位置：
          - 若脏页则先写回并计数
          - 记录替换统计与可选日志
        """
        while True:
            victim_pid = self._policy.victim()
            if victim_pid is None:
                raise RuntimeError("BufferPool is full and all pages are pinned; cannot evict")

            fr = self.frames.get(victim_pid)
            if fr is None:
                continue
            if fr.pin_count > 0:
                continue

            if fr.dirty:
                if DEBUG_EVICT:
                    print(f"[EVICT] pid={victim_pid} dirty=True → writeback; replace with pid={incoming_pid}")
                _BPDiag.log(f"EVICT pid={victim_pid} dirty=True -> writeback; replace with pid={incoming_pid}")
                self.pager.write_page(victim_pid, bytes(fr.data))
                self._stats.evict_dirty += 1
                self._stats.writes += 1
                _BPDiag.add(evict_dirty=1, writes=1)
            else:
                if DEBUG_EVICT:
                    print(f"[EVICT] pid={victim_pid} dirty=False")
                _BPDiag.log(f"EVICT pid={victim_pid} dirty=False")
                self._stats.evict_clean += 1
                _BPDiag.add(evict_clean=1)

            self.frames.pop(victim_pid, None)
            self.page_table.pop(victim_pid, None)
            self._stats.current_resident = max(0, self._stats.current_resident - 1)
            self.evict += 1
            return

    def _require_frame(self, page_id: int) -> Frame:
        fr = self.frames.get(page_id)
        if fr is None:
            raise KeyError(f"page {page_id} not in buffer pool (did you forget get_page?)")
        return fr
