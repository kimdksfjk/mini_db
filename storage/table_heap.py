# table_heap.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

from .buffer_pool import BufferPool
from .pager import Pager
from .data_page import DataPageView
# RID =Record ID,用（page_id,slot_id）唯一标识一条记录
RID = Tuple[int, int]  # (page_id,slot_id)

@dataclass
class TableMeta:
    """
        表的元信息：
        - table_id: 表的唯一标识
        - name: 表名
        - data_pids: 数据页集合（表中的记录实际存储在哪些 page_id）
        - fsm: Free Space Map，记录每个页的剩余可用空间
        """
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
        """
        pager:页管理器：负责磁盘文件的页分配/读写
        buffer_pool:缓冲池：负责页缓存和替换策略
        meta: 表的元信息（记录属于哪些页）
        """
        self.pager = pager
        self.bp = buffer_pool
        self.meta = meta

    # ---------- 读取/扫描 ----------
    def scan(self) -> Iterable[Tuple[RID, bytes]]:
        """
              全表扫描：遍历 data_pids 集合里的所有页，
              依次返回 (RID, record_bytes)。
              """
        for pid in self.meta.data_pids:
            mv = self.bp.get_page(pid)  # 从缓冲池获取页
            page = DataPageView(mv)     # 页视图，提供slot操作
            for slot_id in page.iter_slots():  # 遍历该页的所有有效slot
                yield (pid, slot_id), page.read_record(slot_id)
            self.bp.unpin(pid, dirty=False)  # 用完释放（未修改）

    # ---------- 插入 ----------
    def insert(self, payload: bytes) -> RID:
        """
                插入记录：
                - 优先找一个有足够剩余空间的页
                - 如果没有，则分配一个新页
                - 插入后更新 FSM，并返回该记录的 RID
                """
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
        """
               删除一条记录：
               - 找到对应页和 slot
               - 删除后更新 FSM
               """
        pid, sid = rid
        mv = self.bp.get_page(pid)
        page = DataPageView(mv)
        page.delete_record(sid)
        self.meta.fsm[pid] = page.free_space()  # 粗略回升
        self.bp.unpin(pid, dirty=True)

    # ---------- 更新（原位 or 重插） ----------
    def update(self, rid: RID, new_payload: bytes) -> RID:
        """
              更新一条记录：
              - 如果新数据和原数据大小一致，可原地覆盖
              - 否则：删除旧记录，再重新插入（可能换页）
              """
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
        """
               根据 FSM 找一个有足够剩余空间的页；
               如果没有，返回 None。
               """
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
        """
                分配一个新的数据页：
                - 调用 Pager.allocate_page() 得到新页 id
                - 初始化该页为空页
                - 加入 data_pids & 更新 FSM
                """
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
