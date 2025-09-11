# buffer_pool.py
from __future__ import annotations

from dataclasses import dataclass
from collections import OrderedDict, deque
from typing import Optional, Dict, Deque, Literal

from pager import Pager  # 引用已有的 Pager
"""
定义缓冲池槽位，一个槽位存放一页数据以及管理信息
"""
@dataclass
class Frame:
    page_id: int  # 页号，与磁盘的物理页对应
    data: bytearray  # 页的实际内容
    pin_count: int = 0  # 页的固定计数
    dirty: bool = False  # 脏页标记：脏页表示在内存中被修改过还没写回磁盘，要淘汰这样的页必须调用flush_page写回磁盘以免数据丢失


class _LRUPolicy:
    """
    LRU 候选集合（只跟踪可替换的页：pin_count==0）。
    使用 OrderedDict 实现：最近访问 move_to_end。
    """
    def __init__(self) -> None:
        self._lru: "OrderedDict[int, None]" = OrderedDict()

    def touch(self, pid: int) -> None:
        """当一个未 pin 的页被 get 命中或刚刚 unpin 到 0 时，放/挪到尾部（最近用过）"""
        self._lru.pop(pid, None)
        self._lru[pid] = None

    def remove(self, pid: int) -> None:
        self._lru.pop(pid, None)

    def victim(self) -> Optional[int]:
        """弹出最旧的未 pin 页；没有则返回 None"""
        if not self._lru:
            return None
        # popitem(last=False) → 最旧
        pid, _ = self._lru.popitem(last=False)
        return pid


class _FIFOPolicy:
    """
    FIFO 候选集合（只跟踪可替换的页：pin_count==0）。
    """
    def __init__(self) -> None:
        self._q: Deque[int] = deque()
        self._in_q: set[int] = set()

    def touch(self, pid: int) -> None:
        """一个页成为可替换（pin 变 0）时加入队列；命中不改变顺序。"""
        if pid not in self._in_q:
            self._q.append(pid)
            self._in_q.add(pid)

    def remove(self, pid: int) -> None:
        if pid in self._in_q:
            # 惰性删除：标记移除，必要时清理队首“僵尸”
            self._in_q.remove(pid)

    def victim(self) -> Optional[int]:
        while self._q:
            pid = self._q[0]
            if pid in self._in_q:
                self._q.popleft()
                self._in_q.remove(pid)
                return pid
            # 僵尸（已被 remove），丢弃
            self._q.popleft()
        return None


class BufferPool:
    """
    页缓存：
      - 固定容量（帧数）
      - get_page：命中直接返回；未命中读盘；满了采用策略淘汰未 pin 的页
      - unpin(dirty)：释放并标脏
      - flush_page / flush_all：写回磁盘
    """
    def __init__(self,
                 pager: Pager,
                 capacity: int = 128,
                 policy: Literal["LRU", "FIFO"] = "LRU") -> None:
        assert capacity > 0
        self.pager = pager
        self.capacity = capacity
        self.frames: Dict[int, Frame] = {}     # page_id -> Frame
        self.page_table: Dict[int, int] = {}   # 语义上可以省略；保留便于排错/扩展
        self.hit = 0
        self.miss = 0
        self.evict = 0

        if policy.upper() == "LRU":
            self._policy = _LRUPolicy()
        elif policy.upper() == "FIFO":
            self._policy = _FIFOPolicy()
        else:
            raise ValueError("policy must be 'LRU' or 'FIFO'")

    # -------------------- 对外 API --------------------

    def get_page(self, page_id: int) -> memoryview:
        """
        取得页的可写视图（memoryview）；调用者用完必须 unpin。
        """
        # 命中
        fr = self.frames.get(page_id)
        if fr is not None:
            self.hit += 1
            fr.pin_count += 1
            # 命中后，只有在该页 pin 归零时才进入候选；LRU 可根据需要在 unpin 时 touch
            # 这里不 touch，因为 pinned
            return memoryview(fr.data)

        # 未命中
        self.miss += 1
        if len(self.frames) >= self.capacity:
            self._evict_for(page_id)  # 传入"即将载入"的页号，便于日志输出

        # 读盘加载
        raw = self.pager.read_page(page_id)
        fr = Frame(page_id=page_id, data=bytearray(raw), pin_count=1, dirty=False)
        self.frames[page_id] = fr
        self.page_table[page_id] = page_id  # 简单等值映射，便于断言/调试
        # 新加载的页是 pinned（正在使用），不进候选集合
        return memoryview(fr.data)

    def unpin(self, page_id: int, dirty: bool = False) -> None:
        """
        用完页后必须调用；dirty=True 表示本次使用修改了该页。
        当 pin_count 归零后，页变成可替换 → 进入策略候选集合。
        """
        fr = self._require_frame(page_id)
        if fr.pin_count == 0:
            # 允许重复 unpin 但不降到负值；也可选择抛错
            return
        fr.pin_count -= 1
        if dirty:
            fr.dirty = True
        if fr.pin_count == 0:
            # 进入候选集合
            self._policy.touch(page_id)

    def flush_page(self, page_id: int) -> None:
        """将脏页写回磁盘；不是脏页则忽略。"""
        fr = self.frames.get(page_id)
        if fr and fr.dirty:
            self.pager.write_page(page_id, bytes(fr.data))
            fr.dirty = False

    def flush_all(self) -> None:
        """将所有脏页写回磁盘。"""
        for pid, fr in list(self.frames.items()):
            if fr.dirty:
                self.pager.write_page(pid, bytes(fr.data))
                fr.dirty = False
        self.pager.sync()

    @property
    def stats(self) -> dict:
        total = self.hit + self.miss
        return {
            "capacity": self.capacity,
            "cached": len(self.frames),
            "hit": self.hit,
            "miss": self.miss,
            "evict": self.evict,
            "hit_rate": (self.hit / total) if total else 0.0,
        }

    def reset_stats(self) -> None:  # 方便测试重置
        self.hit = 0
        self.miss = 0
        self.evict = 0

    def report_stats(self) -> None:  # 简单打印
        s = self.stats
        print(f"[STATS] cap={s['capacity']} cached={s['cached']} "
              f"hit={s['hit']} miss={s['miss']} evict={s['evict']} "
              f"hit_rate={s['hit_rate']:.2%}")

    # -------------------- 内部方法 --------------------

    def _evict_for(self, incoming_pid: int) -> None:
        """
        为加载incoming_pid选择并淘汰一个未pin页
        输出结构化替换日志。若页是脏的，先写回。
        """
        while True:
            victim_pid = self._policy.victim()
            if victim_pid is None:
                # 没有可替换候选（都被 pin）→ 无法淘汰
                raise RuntimeError("BufferPool is full and all pages are pinned; cannot evict")

            fr = self.frames.get(victim_pid)
            if fr is None:
                # 可能是僵尸（例如 FIFO 的惰性删除），跳过
                continue

            if fr.pin_count > 0:
                # 保险：策略层已保证候选是 pin==0；若出现并发/时序问题，跳过
                continue

            # 写回脏页
            if fr.dirty:
                print(f"[EVICT] pid={victim_pid} dirty=True → writeback;replace with pid={incoming_pid}")
                self.pager.write_page(victim_pid, bytes(fr.data))
            else:
                print(f"[EVICT] pid={victim_pid} dirty=False")

            # 从缓存剔除
            self.frames.pop(victim_pid, None)
            self.page_table.pop(victim_pid, None)
            # 不需要从策略集合里删除：victim() 已经弹出
            self.evict += 1
            return

    def _require_frame(self, page_id: int) -> Frame:
        fr = self.frames.get(page_id)
        if fr is None:
            raise KeyError(f"page {page_id} not in buffer pool (did you forget get_page?)")
        return fr
