# data_page.py
from __future__ import annotations
import struct
from typing import Iterable

# ---------------- 页面内部布局定义 ----------------
# Header 格式: page_id(uint32) | free_off(uint16) | slot_count(uint16) | flags(uint16)
# 含义：
#   - page_id     当前页号
#   - free_off    当前页中“数据区”已经使用到的偏移（从头部向上增长）
#   - slot_count  槽目录中已有的槽数量
#   - flags       预留标志位
_HDR_FMT = "<IHHH"
_HDR_SIZE = struct.calcsize(_HDR_FMT)  # 4 + 2 + 2 + 2 = 10 字节

# Slot entry 格式: offset(uint16) | length(uint16) | tombstone(uint8) | pad(uint8)
# 含义：
#   - offset     该记录在页内的起始偏移量
#   - length     记录的长度
#   - tombstone  是否删除标记 (1=删除, 0=有效)
#   - pad        填充对齐用
_SLOT_FMT = "<HHBx"
_SLOT_SIZE = struct.calcsize(_SLOT_FMT)  # 6 字节

class DataPageView:
    """
    针对单个数据页的“视图”对象。
    提供页内数据的插入、读取、删除、更新、遍历等操作。
    特点：直接基于 memoryview 操作，不会复制字节，提高效率。

    页面逻辑布局（固定大小）:
    [ Header | .... Data area (↑向上增长) .... | Slot[n-1] ... Slot[0] ]
    """

    def __init__(self, mv: memoryview):
        # 要求传入可写的 memoryview，否则不能修改页
        assert mv.readonly is False, "DataPageView requires writable memoryview"
        self.mv = mv
        self.page_size = len(mv)

    # ---------- Header 读写 ----------
    def _read_header(self):
        """解析头部四个字段 (page_id, free_off, slot_count, flags)"""
        return struct.unpack_from(_HDR_FMT, self.mv, 0)

    def _write_header(self, page_id: int, free_off: int, slot_cnt: int, flags: int = 0):
        """更新头部四个字段"""
        struct.pack_into(_HDR_FMT, self.mv, 0, page_id, free_off, slot_cnt, flags)

    # ---------- 公共头字段属性 ----------
    @property
    def page_id(self) -> int:
        pid, _, _, _ = self._read_header()
        return pid

    @property
    def free_off(self) -> int:
        _, free_off, _, _ = self._read_header()
        return free_off

    @property
    def slot_count(self) -> int:
        _, _, sc, _ = self._read_header()
        return sc

    # ---------- 初始化空页 ----------
    def format_empty(self, page_id: int) -> None:
        """把整页清零，并写入初始 header"""
        self.mv[:] = b"\x00" * self.page_size
        # 初始 free_off = header 大小；slot_count=0
        self._write_header(page_id, _HDR_SIZE, 0, 0)

    # ---------- 槽目录操作 ----------
    def _slot_pos(self, slot_id: int) -> int:
        """
        根据槽号计算它在页中的字节偏移量。
        注意：槽目录从页尾向前增长，第 0 个槽在最末尾。
        """
        assert 0 <= slot_id < self.slot_count
        return self.page_size - (slot_id + 1) * _SLOT_SIZE

    def _read_slot(self, slot_id: int):
        """读取某个槽的信息 (offset, length, tombstone)"""
        off = self._slot_pos(slot_id)
        return struct.unpack_from(_SLOT_FMT, self.mv, off)

    def _write_slot(self, slot_id: int, offset: int, length: int, tomb: int) -> None:
        """写入槽信息"""
        off = self._slot_pos(slot_id)
        struct.pack_into(_SLOT_FMT, self.mv, off, offset, length, tomb)

    # ---------- 空间管理 ----------
    def free_space(self) -> int:
        """
        计算剩余可用空间：
          = 页总大小 - 已占用数据区大小 - (现有槽数+1)*槽大小
        注：未考虑 tombstone 的回收。
        """
        sc = self.slot_count
        return self.page_size - self.free_off - (sc + 1) * _SLOT_SIZE

    def slot_overhead(self) -> int:
        """每条记录除了 payload 外的额外开销（槽表项大小）"""
        return _SLOT_SIZE

    # ---------- 记录操作 ----------
    def insert_record(self, payload: bytes) -> int:
        """
        插入一条新记录：
          - 检查空间是否足够
          - 把数据写入 free_off 指定位置
          - 增加 slot_count，并分配新的槽位
        返回分配的 slot_id。
        """
        need = len(payload) + _SLOT_SIZE
        if self.free_space() < need:
            raise MemoryError("not enough space in page")

        pid, free_off, sc, flags = self._read_header()
        data_off = free_off

        # 写数据到数据区
        self.mv[data_off : data_off + len(payload)] = payload

        # 更新 header：free_off 前移，slot_count 增加
        self._write_header(pid, free_off + len(payload), sc + 1, flags)

        # 新槽位 ID = 原 slot_count
        slot_id = sc
        self._write_slot(slot_id, data_off, len(payload), 0)
        return slot_id

    def read_record(self, slot_id: int) -> bytes:
        """读取指定槽的记录（若被 tombstone 标记则报错）"""
        off, length, tomb = self._read_slot(slot_id)
        if tomb:
            raise KeyError(f"slot {slot_id} is deleted")
        if length == 0:
            return b""
        return bytes(self.mv[off : off + length])

    def delete_record(self, slot_id: int) -> None:
        """删除记录：只是打 tombstone 标记，不立即回收空间"""
        off, length, tomb = self._read_slot(slot_id)
        if tomb:
            return
        self._write_slot(slot_id, off, length, 1)

    def overwrite_record(self, slot_id: int, payload: bytes) -> bool:
        """
        尝试在原位置覆盖记录：
          - 如果新长度 == 原长度，直接覆盖并返回 True
          - 否则返回 False（需要上层重插）
        """
        off, length, tomb = self._read_slot(slot_id)
        if tomb:
            raise KeyError(f"slot {slot_id} is deleted")
        if len(payload) != length:
            return False
        self.mv[off : off + length] = payload
        return True

    def record_length(self, slot_id: int) -> int:
        """返回某槽记录的长度"""
        _, length, _ = self._read_slot(slot_id)
        return length

    # ---------- 遍历 ----------
    def iter_slots(self) -> Iterable[int]:
        """遍历所有有效记录的 slot_id"""
        for sid in range(self.slot_count):
            _, length, tomb = self._read_slot(sid)
            if not tomb and length > 0:
                yield sid

