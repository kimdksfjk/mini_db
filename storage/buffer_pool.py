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

# 将 True 改为打印到 stdout 的淘汰日志（适合本地快速观察）
DEBUG_EVICT = False


# --------------------------- 数据结构与统计 ---------------------------

@dataclass
class Frame:
    """
    缓冲池槽位（一个 frame 对应磁盘上的一页）：
    - page_id: 逻辑页号（与 Pager 的页号一致）
    - data: 该页的内存副本（可写）
    - pin_count: 引用计数；>0 表示“被固定”，不可被淘汰
    - dirty: 是否为脏页；True 表示内存数据较磁盘更新，淘汰时必须写回
    """
    page_id: int
    data: bytearray
    pin_count: int = 0
    dirty: bool = False


@dataclass
class BPStats:
    """
    实例级详细统计（一个 BufferPool 对应一份）：
    - hits / misses: get_page 命中/未命中次数
    - reads / writes: 磁盘读/写次数（通过 Pager）
    - evict_clean / evict_dirty: 淘汰的干净/脏页数量
    - pins / unpins: get_page 成功后的固定次数 / 释放次数
    - current_resident: 当前驻留在缓冲池的页数
    - max_resident: 运行期内出现过的最大驻留页数（容量利用峰值）
    - capacity: 容量（帧数）
    - start_ts: 统计起始时间（用于观测窗口）
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
    """
    全局诊断器（跨实例聚合统计 + 可选写文件日志）：
    - 通过类变量维护一个全局 BPStats，并加锁保证线程安全
    - 可开启文件日志：把替换事件写入磁盘，便于长期分析
    """
    _global_lock = threading.Lock()
    _global = BPStats(start_ts=time.time())
    _logger: logging.Logger | None = None
    _log_handler: logging.Handler | None = None

    @classmethod
    def add(cls, **delta) -> None:
        """对全局统计做增量加和（需持有锁）"""
        with cls._global_lock:
            g = cls._global
            for k, v in delta.items():
                if hasattr(g, k):
                    setattr(g, k, getattr(g, k) + int(v))

    @classmethod
    def snapshot(cls) -> dict:
        """获取全局统计快照（字典）"""
        with cls._global_lock:
            return asdict(cls._global)

    @classmethod
    def reset(cls) -> None:
        """重置全局统计（保留历史最大容量）"""
        with cls._global_lock:
            cap = cls._global.capacity
            cls._global = BPStats(capacity=cap, start_ts=time.time())

    @classmethod
    def enable_log(cls, path: str | None = None) -> None:
        """
        开启文件日志（仅初始化一次）：
        - 默认写入 __logs__/buffer_pool.log
        - 主要记录淘汰事件（EVICT）
        """
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
        """关闭文件日志（移除 handler）"""
        if cls._logger and cls._log_handler:
            cls._logger.removeHandler(cls._log_handler)
        cls._logger = None
        cls._log_handler = None

    @classmethod
    def log(cls, msg: str) -> None:
        """写入一条 INFO 日志到文件（若已启用）"""
        if cls._logger:
            cls._logger.info(msg)


# --------------------------- 替换策略（LRU / FIFO） ---------------------------

class _LRUPolicy:
    """
    LRU 候选集合（仅跟踪 pin==0 的可替换页）：
    - touch(pid): 把 pid 放到队尾（最近使用）
    - victim(): 弹出队首（最久未使用）
    """
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
    """
    FIFO 候选集合（仅跟踪 pin==0 的可替换页）：
    - touch(pid): 可替换时入队
    - victim(): 按进入顺序淘汰
    """
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
            # 僵尸元素（之前 remove 过）：丢弃并继续
            self._q.pop()
        return None


# --------------------------- 缓冲池主体 ---------------------------

class BufferPool:
    """
    页缓冲池：
    - 容量固定（capacity 表示最多缓存多少页）
    - get_page: 先查缓存，未命中时读盘；满了则按策略淘汰
    - unpin(dirty): 释放引用，可选标脏；pin==0 时进入候选集合
    - flush_page / flush_all: 脏页写回
    - stats / stats_snapshot: 简要/详细统计
    - global_stats: 跨实例聚合统计（调试/观测）
    - enable_global_log: 选配的替换日志落盘（生产排障）
    """
    def __init__(self,
                 pager: Pager,
                 capacity: int = 128,
                 policy: Literal["LRU", "FIFO"] = "LRU") -> None:
        assert capacity > 0
        self.pager = pager
        self.capacity = capacity
        self.frames: Dict[int, Frame] = {}    # page_id -> Frame
        self.page_table: Dict[int, int] = {}  # 目前等值映射，保留扩展可能
        # 兼容旧接口的三项简要统计
        self.hit = 0
        self.miss = 0
        self.evict = 0

        # 选择替换策略实现
        if policy.upper() == "LRU":
            self._policy = _LRUPolicy()
        elif policy.upper() == "FIFO":
            self._policy = _FIFOPolicy()
        else:
            raise ValueError("policy must be 'LRU' or 'FIFO'")

        # 实例级详细统计
        self._stats = BPStats(capacity=capacity, start_ts=time.time())
        # 记录全局最大容量（便于横向观察）
        with _BPDiag._global_lock:
            _BPDiag._global.capacity = max(_BPDiag._global.capacity, capacity)

    # -------------------- 对外 API --------------------

    def get_page(self, page_id: int) -> memoryview:
        """
        取得指定页的可写 memoryview：
        - 命中：仅增加 hit / pins，直接返回
        - 未命中：若满则淘汰；然后 read_page 读盘、计数 reads；把新页放入缓存并 pin
        - 返回值必须配对调用 unpin(page_id, dirty=...)
        """
        # 命中路径
        fr = self.frames.get(page_id)
        if fr is not None:
            self.hit += 1
            self._stats.hits += 1
            self._stats.pins += 1
            _BPDiag.add(hits=1, pins=1)
            fr.pin_count += 1
            return memoryview(fr.data)

        # 未命中路径
        self.miss += 1
        self._stats.misses += 1
        _BPDiag.add(misses=1)

        # 缓存满则先淘汰
        if len(self.frames) >= self.capacity:
            self._evict_for(page_id)

        # 从磁盘读入该页
        raw = self.pager.read_page(page_id)
        self._stats.reads += 1
        _BPDiag.add(reads=1)

        # 放入缓存并置为 pinned
        fr = Frame(page_id=page_id, data=bytearray(raw), pin_count=1, dirty=False)
        self.frames[page_id] = fr
        self.page_table[page_id] = page_id

        # 维护驻留页统计
        self._stats.current_resident += 1
        if self._stats.current_resident > self._stats.max_resident:
            self._stats.max_resident = self._stats.current_resident
        self._stats.pins += 1
        _BPDiag.add(pins=1)

        return memoryview(fr.data)

    def unpin(self, page_id: int, dirty: bool = False) -> None:
        """
        用完页后必须调用：
        - pin_count 减 1
        - 若 dirty=True，标记该页为脏；写回由 flush/淘汰时统一进行（延迟写回）
        - 当 pin_count==0 时，加入替换候选集合（LRU/FIFO）
        """
        fr = self._require_frame(page_id)
        if fr.pin_count == 0:
            # 容错：重复 unpin 时不降为负数
            return
        fr.pin_count -= 1
        self._stats.unpins += 1
        _BPDiag.add(unpins=1)
        if dirty:
            fr.dirty = True
        if fr.pin_count == 0:
            self._policy.touch(page_id)  # 进入候选集合

    def flush_page(self, page_id: int) -> None:
        """
        写回单个脏页（若非脏页则忽略）：
        - 采用 write-behind 策略，只有显式 flush 或淘汰时才写盘
        """
        fr = self.frames.get(page_id)
        if fr and fr.dirty:
            self.pager.write_page(page_id, bytes(fr.data))
            fr.dirty = False
            self._stats.writes += 1
            _BPDiag.add(writes=1)

    def flush_all(self) -> None:
        """
        写回所有脏页，并请求 Pager 同步（fsync）：
        - 用于事务结束/安全落盘/关闭前
        """
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
        兼容旧接口的“简表”：
        - hit/miss/evict 及命中率
        - cached = 当前缓存中的 frame 数
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
        """返回实例级详细统计（BPStats -> dict）"""
        return asdict(self._stats)

    @staticmethod
    def global_stats() -> dict:
        """返回跨实例聚合统计（适合多表/多 BP 的总览）"""
        return _BPDiag.snapshot()

    @staticmethod
    def reset_global_stats() -> None:
        """重置跨实例聚合统计"""
        _BPDiag.reset()

    @staticmethod
    def enable_global_log(path: str | None = None) -> None:
        """开启替换日志落盘（默认 __logs__/buffer_pool.log）"""
        _BPDiag.enable_log(path)

    @staticmethod
    def disable_global_log() -> None:
        """关闭替换日志落盘"""
        _BPDiag.disable_log()

    def reset_stats(self) -> None:
        """重置旧版三项简要统计（便于 A/B 实验或阶段评估）"""
        self.hit = 0
        self.miss = 0
        self.evict = 0

    def report_stats(self) -> None:
        """
        以固定格式打印简要统计（直接 print，便于 CLI 无 debug 情况下查看）：
        - 和 logging 版互补：不依赖 logging 配置
        """
        s = self.stats
        print(f"[STATS] cap={s['capacity']} cached={s['cached']} "
              f"hit={s['hit']} miss={s['miss']} evict={s['evict']} "
              f"hit_rate={s['hit_rate']:.2%}")

    # -------------------- 内部方法 --------------------

    def _evict_for(self, incoming_pid: int) -> None:
        """
        为 incoming_pid 腾出一个槽位：
        - 从策略集合选择 victim（仅 pin==0 的页）
        - 脏页先 writeback 再移除；干净页直接移除
        - 维护实例/全局统计与驻留计数
        """
        while True:
            victim_pid = self._policy.victim()
            if victim_pid is None:
                # 候选为空：说明所有页都被 pin 住了（上层忘记 unpin 的常见症状）
                raise RuntimeError("BufferPool is full and all pages are pinned; cannot evict")

            fr = self.frames.get(victim_pid)
            if fr is None:
                # 可能是 FIFO 的“僵尸条目”，跳过重试
                continue
            if fr.pin_count > 0:
                # 再保险：策略层只应放 pin==0 的；遇到竞态也要跳过
                continue

            if fr.dirty:
                # 淘汰脏页：打印/记录日志后写回磁盘
                if DEBUG_EVICT:
                    print(f"[EVICT] pid={victim_pid} dirty=True → writeback; replace with pid={incoming_pid}")
                _BPDiag.log(f"EVICT pid={victim_pid} dirty=True -> writeback; replace with pid={incoming_pid}")
                self.pager.write_page(victim_pid, bytes(fr.data))
                self._stats.evict_dirty += 1
                self._stats.writes += 1
                _BPDiag.add(evict_dirty=1, writes=1)
            else:
                # 淘汰干净页：仅记录事件
                if DEBUG_EVICT:
                    print(f"[EVICT] pid={victim_pid} dirty=False")
                _BPDiag.log(f"EVICT pid={victim_pid} dirty=False")
                self._stats.evict_clean += 1
                _BPDiag.add(evict_clean=1)

            # 真正移除 victim
            self.frames.pop(victim_pid, None)
            self.page_table.pop(victim_pid, None)
            # 当前驻留页 -1（不要小于 0）
            self._stats.current_resident = max(0, self._stats.current_resident - 1)
            # 兼容旧接口的淘汰计数
            self.evict += 1
            return

    def _require_frame(self, page_id: int) -> Frame:
        """确保页在缓存中；否则抛错提示使用者忘记 get_page"""
        fr = self.frames.get(page_id)
        if fr is None:
            raise KeyError(f"page {page_id} not in buffer pool (did you forget get_page?)")
        return fr
