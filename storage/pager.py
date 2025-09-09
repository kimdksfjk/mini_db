# pager.py
from __future__ import annotations
import io
import os
import struct
from dataclasses import dataclass

_META_FMT = "<4sHHii"  # magic(4s) | version(uint16) | page_size(uint16) | page_count(int32) | free_head(int32)
_META_SIZE = struct.calcsize(_META_FMT)
_MAGIC = b"MDB1"
_VERSION = 1

# 空闲页头格式：仅一个 int32 指向下一空闲页；负数表示无
_FREE_HDR_FMT = "<i"
_FREE_HDR_SIZE = struct.calcsize(_FREE_HDR_FMT)


@dataclass
class Meta:
    magic: bytes
    version: int
    page_size: int
    page_count: int
    free_head: int  # -1 表示空

    def pack(self) -> bytes:
        buf = struct.pack(_META_FMT, self.magic, self.version, self.page_size, self.page_count, self.free_head)
        # 余下字节补 0 直至一页大小（真正写盘时会再 pad 到 page_size）
        return buf

    @classmethod
    def unpack_from(cls, data: bytes) -> "Meta":
        magic, version, page_size, page_count, free_head = struct.unpack_from(_META_FMT, data, 0)
        return cls(magic, version, page_size, page_count, free_head)


class Pager:
    """
    单文件页式存储：
      - page 0: META（魔数/版本/页大小/总页数/空闲链表头）
      - page 1..N: 数据页或空闲页（空闲页页头是 int32 的 next_free_page_id）
    """

    def __init__(self, file_path: str, page_size: int = 4096):
        self.path = file_path
        self._f: io.BufferedRandom
        self.meta: Meta
        if os.path.exists(self.path):
            self._f = open(self.path, "r+b", buffering=0)
            self._f.seek(0)
            first_page = self._f.read(page_size)
            if len(first_page) != page_size:
                raise IOError("bad database file: truncated meta page")
            meta = Meta.unpack_from(first_page)
            # 基本校验
            if meta.magic != _MAGIC:
                raise IOError("bad magic; not a mini-db file")
            if meta.page_size != page_size:
                raise IOError(f"page size mismatch: file={meta.page_size}, expected={page_size}")
            self.meta = meta
        else:
            # 新建文件并写入 meta 页
            self._f = open(self.path, "w+b", buffering=0)
            self.meta = Meta(_MAGIC, _VERSION, page_size, page_count=1, free_head=-1)
            self._write_meta()  # 产生 page 0
            # 截断到至少一页
            self._f.truncate(self.meta.page_size)  # page 0 已存在
            self._f.flush()
            os.fsync(self._f.fileno())

    # ------------------------- 公共 API -------------------------

    def page_size(self) -> int:
        return self.meta.page_size

    def read_page(self, page_id: int) -> bytes:
        self._check_pid(page_id)
        self._f.seek(page_id * self.meta.page_size)
        data = self._f.read(self.meta.page_size)
        if len(data) != self.meta.page_size:
            raise IOError("short read (corrupted file?)")
        return data

    def write_page(self, page_id: int, data: bytes) -> None:
        self._check_pid(page_id)
        if len(data) != self.meta.page_size:
            raise ValueError(f"write_page: bad data size {len(data)} != {self.meta.page_size}")
        self._f.seek(page_id * self.meta.page_size)
        self._f.write(data)

    def allocate_page(self) -> int:
        """分配一个新页，返回 page_id。优先使用空闲链；为空则在文件末尾追加一页。"""
        if self.meta.free_head >= 0:
            pid = self.meta.free_head
            # 读出该空闲页的“下一个”
            raw = self._read_exact(pid)
            (next_free,) = struct.unpack_from(_FREE_HDR_FMT, raw, 0)
            self.meta.free_head = next_free
            self._write_meta()
            # 返回一个全 0 的页（调用者可覆盖）
            zero = bytes(self.meta.page_size)
            self.write_page(pid, zero)
            return pid
        else:
            # 追加新页：当前 page_count 是下一个新页的 pid
            pid = self.meta.page_count
            self.meta.page_count += 1
            self._write_meta()
            # 扩容文件
            self._f.seek(pid * self.meta.page_size)
            self._f.write(bytes(self.meta.page_size))
            return pid

    def free_page(self, page_id: int) -> None:
        """释放一个页：将其挂到空闲链头，并在该页页头写入 next_free_page_id。"""
        self._check_pid(page_id)
        if page_id == 0:
            raise ValueError("cannot free meta page 0")
        # 写入空闲页头：next = 当前链表头
        buf = bytearray(self.meta.page_size)
        struct.pack_into(_FREE_HDR_FMT, buf, 0, self.meta.free_head)
        # 其余空间清零（buf 已是 0）
        self.write_page(page_id, bytes(buf))
        # 更新 meta.free_head 指向新释放的页
        self.meta.free_head = page_id
        self._write_meta()

    def page_count(self) -> int:
        return self.meta.page_count

    def sync(self) -> None:
        """确保元信息与页数据落盘。"""
        self._f.flush()
        os.fsync(self._f.fileno())

    def close(self) -> None:
        try:
            self.sync()
        finally:
            self._f.close()

    # ------------------------- 内部方法 -------------------------

    def _check_pid(self, pid: int) -> None:
        if pid < 0 or pid >= self.meta.page_count:
            raise IndexError(f"page_id out of range: {pid} (page_count={self.meta.page_count})")

    def _read_exact(self, page_id: int) -> bytes:
        self._f.seek(page_id * self.meta.page_size)
        data = self._f.read(self.meta.page_size)
        if len(data) != self.meta.page_size:
            raise IOError("short read")
        return data

    def _write_meta(self) -> None:
        # 将 meta 写到 page 0，并把整页填满
        page = bytearray(self.meta.page_size)
        packed = self.meta.pack()
        page[: len(packed)] = packed
        # pad 到整页（page 已经是整页零填充）
        self._f.seek(0)
        self._f.write(page)
        self._f.flush()
        os.fsync(self._f.fileno())

