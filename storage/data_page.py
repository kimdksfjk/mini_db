# data_page.py
from __future__ import annotations
import struct
from typing import Iterable

# Header: page_id(uint32) | free_off(uint16) | slot_count(uint16) | flags(uint16)
_HDR_FMT = "<IHHH"
_HDR_SIZE = struct.calcsize(_HDR_FMT)  # 4 + 2 + 2 + 2 = 10 bytes

# Slot entry: offset(uint16) | length(uint16) | tombstone(uint8) | pad(uint8)
_SLOT_FMT = "<HHBx"
_SLOT_SIZE = struct.calcsize(_SLot_FMT) if (_SLot_FMT := _SLOT_FMT) else 6  # avoid linter
# _SLOT_SIZE = 6

class DataPageView:
    """
    针对一页的“视图”，直接在 memoryview 上读写，不复制。
    页面布局（固定页大小）:
      [Header | ....... Data area (↑增长) ....... | Slot[n-1] ... Slot[0]]
    """
    def __init__(self, mv: memoryview):
        assert mv.readonly is False, "DataPageView requires writable memoryview"
        self.mv = mv
        self.page_size = len(mv)

    # ---------- header 读写 ----------
    def _read_header(self):
        return struct.unpack_from(_HDR_FMT, self.mv, 0)

    def _write_header(self, page_id: int, free_off: int, slot_cnt: int, flags: int = 0):
        struct.pack_into(_HDR_FMT, self.mv, 0, page_id, free_off, slot_cnt, flags)

    # ---------- 公共头字段 ----------
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
        # 清零整页
        self.mv[:] = b"\x00" * self.page_size
        self._write_header(page_id, _HDR_SIZE, 0, 0)

    # ---------- 槽目录操作 ----------
    def _slot_pos(self, slot_id: int) -> int:
        assert 0 <= slot_id < self.slot_count
        # 第 0 个槽在页尾部，随后逆向排列
        return self.page_size - (slot_id + 1) * _SLOT_SIZE

    def _read_slot(self, slot_id: int):
        off = self._slot_pos(slot_id)
        return struct.unpack_from(_SLOT_FMT, self.mv, off)  # (offset, length, tomb)

    def _write_slot(self, slot_id: int, offset: int, length: int, tomb: int) -> None:
        off = self._slot_pos(slot_id)
        struct.pack_into(_SLOT_FMT, self.mv, off, offset, length, tomb)

    # ---------- 可用空间 ----------
    def free_space(self) -> int:
        """剩余可用空间（未考虑墓碑回收），用于插入可行性判断。"""
        sc = self.slot_count
        return self.page_size - self.free_off - (sc + 1) * _SLOT_SIZE

    def slot_overhead(self) -> int:
        return _SLOT_SIZE

    # ---------- 读/写/删 ----------
    def insert_record(self, payload: bytes) -> int:
        need = len(payload) + _SLOT_SIZE
        if self.free_space() < need:
            raise MemoryError("not enough space in page")

        # 写数据
        pid, free_off, sc, flags = self._read_header()
        data_off = free_off
        self.mv[data_off : data_off + len(payload)] = payload

        # 新槽号 = 旧 slot_count
        slot_id = sc
        # 先写槽，再更新头
        self._write_slot(slot_id, data_off, len(payload), 0)
        self._write_header(pid, free_off + len(payload), sc + 1, flags)
        return slot_id

    def read_record(self, slot_id: int) -> bytes:
        off, length, tomb = self._read_slot(slot_id)
        if tomb:
            raise KeyError(f"slot {slot_id} is deleted")
        if length == 0:
            return b""
        return bytes(self.mv[off : off + length])

    def delete_record(self, slot_id: int) -> None:
        off, length, tomb = self._read_slot(slot_id)
        if tomb:
            return
        # 标记墓碑；本版不即时进行页内压缩
        self._write_slot(slot_id, off, length, 1)

    def overwrite_record(self, slot_id: int, payload: bytes) -> bool:
        """
        尝试原位覆盖；若长度不变则覆写并返回 True；否则返回 False（需上层重插）
        """
        off, length, tomb = self._read_slot(slot_id)
        if tomb:
            raise KeyError(f"slot {slot_id} is deleted")
        if len(payload) != length:
            return False
        self.mv[off : off + length] = payload
        return True

    def record_length(self, slot_id: int) -> int:
        _, length, _ = self._read_slot(slot_id)
        return length

    # ---------- 遍历活跃记录 ----------
    def iter_slots(self) -> Iterable[int]:
        for sid in range(self.slot_count):
            _, length, tomb = self._read_slot(sid)
            if not tomb and length > 0:
                yield sid

