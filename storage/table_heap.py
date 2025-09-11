# table_heap.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

from .buffer_pool import BufferPool
from .pager import Pager
from .data_page import DataPageView

RID = Tuple[int, int]  # (page_id, slot_id)

@dataclass
class TableMeta:
    table_id: int
    name: str
    # 该表的数据页集合
    data_pids: List[int] = field(default_factory=list)
    # Free Space Map：pid -> 可用字节数（粗略估计，按页内 free_space() 更新）
    fsm: Dict[int, int] = field(default_factory=dict)

class TableHeap:
    """
    “堆表”抽象：一张表 = 若干数据页（page_id 的集合）
      - insert(payload) -> RID
      - delete(RID)
      - scan() -> (RID, bytes) 序列
    FSM（简易版）：按页 free_space 粗略选择插入目标页，空间不足就分配新页。
    """
    def __init__(self, pager: Pager, buffer_pool: BufferPool, meta: TableMeta):
        self.pager = pager
        self.bp = buffer_pool
        self.meta = meta

    # ---------- 读取/扫描 ----------
    def scan(self) -> Iterable[Tuple[RID, bytes]]:
        for pid in self.meta.data_pids:
            mv = self.bp.get_page(pid)
            page = DataPageView(mv)
            for slot_id in page.iter_slots():
                yield (pid, slot_id), page.read_record(slot_id)
            self.bp.unpin(pid, dirty=False)

    # ---------- 插入 ----------
    def insert(self, payload: bytes) -> RID:
        need = len(payload)
        pid = self._choose_page_for_insert(need)
        if pid is None:
            pid = self._allocate_data_page()

        mv = self.bp.get_page(pid)
        page = DataPageView(mv)

        # 再次确认剩余空间（并发/估计误差）
        room = page.free_space()
        if room < need + page.slot_overhead():
            # 该页塞不下，换新页
            self.bp.unpin(pid, dirty=False)
            pid = self._allocate_data_page()
            mv = self.bp.get_page(pid)
            page = DataPageView(mv)

        slot_id = page.insert_record(payload)
        # 更新 FSM：用真实 free_space 覆盖
        self.meta.fsm[pid] = page.free_space()
        self.bp.unpin(pid, dirty=True)
        return (pid, slot_id)

    # ---------- 删除 ----------
    def delete(self, rid: RID) -> None:
        pid, sid = rid
        mv = self.bp.get_page(pid)
        page = DataPageView(mv)
        page.delete_record(sid)
        self.meta.fsm[pid] = page.free_space()  # 粗略回升
        self.bp.unpin(pid, dirty=True)

    # ---------- 更新（原位 or 重插） ----------
    def update(self, rid: RID, new_payload: bytes) -> RID:
        pid, sid = rid
        mv = self.bp.get_page(pid)
        page = DataPageView(mv)
        ok = page.overwrite_record(sid, new_payload)
        if ok:
            self.meta.fsm[pid] = page.free_space()
            self.bp.unpin(pid, dirty=True)
            return rid
        # 变长重插
        old_len = page.record_length(sid)
        page.delete_record(sid)
        self.meta.fsm[pid] = page.free_space() + (old_len - page.slot_overhead())  # 近似回收
        self.bp.unpin(pid, dirty=True)
        return self.insert(new_payload)

    # ---------- 内部：选择插入页 ----------
    def _choose_page_for_insert(self, need: int) -> Optional[int]:
        overhead = DataPageView(memoryview(bytearray(self.pager.page_size()))).slot_overhead()
        required = need + overhead
        # 简单策略：按 fsm 顺序找第一个能放得下的页
        for pid in self.meta.data_pids:
            free = self.meta.fsm.get(pid, 0)
            if free >= required:
                return pid
        return None

    # ---------- 内部：分配并初始化新数据页 ----------
    def _allocate_data_page(self) -> int:
        pid = self.pager.allocate_page()
        self.meta.data_pids.append(pid)
        # 初始化空页头
        mv = self.bp.get_page(pid)
        page = DataPageView(mv)
        page.format_empty(pid)
        free = page.free_space()
        self.meta.fsm[pid] = free
        self.bp.unpin(pid, dirty=True)
        return pid
