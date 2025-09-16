# pager.py
from __future__ import annotations
import io
import os
import struct
from dataclasses import dataclass

# ---------------- 元页(META)与空闲页头的二进制布局 ----------------
# 说明：整个数据库文件被切成固定大小的“页”。第 0 页用于存放元信息 (Meta)。
#      其它页(1..N-1)是数据页或空闲页。空闲页通过单链表串起来。
#
# Meta 二进制格式（位于 page 0 开头）：
#   magic(4s) | version(uint16) | page_size(uint16) | page_count(int32) | free_head(int32)
# 含义：
#   - magic:   魔数，用于识别文件类型（例如 b"MDB1"）
#   - version: 文件格式版本
#   - page_size: 页大小（字节，固定）
#   - page_count: 当前已存在的页总数（包含第 0 页）
#   - free_head: 空闲页链表的头部页号（-1 表示无空闲页）
#
# 空闲页头格式（位于空闲页的页首）：
#   next_free_page_id (int32)  指向下一空闲页；-1 表示空
#
_META_FMT = "<4sHHii"  # magic | version | page_size | page_count | free_head
_META_SIZE = struct.calcsize(_META_FMT)
_MAGIC = b"MDB1"
_VERSION = 1

_FREE_HDR_FMT = "<i"   # 仅保存 next_free_page_id
_FREE_HDR_SIZE = struct.calcsize(_FREE_HDR_FMT)


@dataclass
class Meta:
    """
    内存中的元信息对象。用于在读写元页时序列化/反序列化。
    注意：写盘时会把本结构体 pack 到第 0 页的开头，其余空间补 0。
    """
    magic: bytes
    version: int
    page_size: int
    page_count: int
    free_head: int  # -1 表示空闲链表为空

    def pack(self) -> bytes:
        """把 Meta 按 _META_FMT 打包成二进制。"""
        buf = struct.pack(_META_FMT, self.magic, self.version, self.page_size, self.page_count, self.free_head)
        # 注：这里只返回 header 的有效字节；真正写盘时，调用方会把整页填满（零填充）。
        return buf

    @classmethod
    def unpack_from(cls, data: bytes) -> "Meta":
        """从第 0 页字节流中解析出 Meta。"""
        magic, version, page_size, page_count, free_head = struct.unpack_from(_META_FMT, data, 0)
        return cls(magic, version, page_size, page_count, free_head)


class Pager:
    """
    单文件页式存储管理器：
      - 负责打开/新建数据库文件
      - 负责按“页”为单位的读写
      - 负责页的分配与释放（维护空闲页链表）
      - 第 0 页持久化保存全局元信息 Meta
    文件页布局：
      - page 0: META（魔数/版本/页大小/总页数/空闲链表头）
      - page 1..N-1: 数据页或空闲页（空闲页页首保存 next_free_page_id）
    """

    def __init__(self, file_path: str, page_size: int = 4096):
        """
        打开或创建数据库文件：
          - 如果文件已存在：读取第 0 页并校验 magic 与 page_size
          - 如果不存在：新建文件，写入初始 Meta 到第 0 页，并把文件截断到 1 页大小
        """
        self.path = file_path
        self._f: io.BufferedRandom
        self.meta: Meta

        if os.path.exists(self.path):
            # 以读写方式打开已有文件（buffering=0 关闭 Python 级缓冲，便于直接控制）
            self._f = open(self.path, "r+b", buffering=0)
            self._f.seek(0)
            first_page = self._f.read(page_size)
            if len(first_page) != page_size:
                raise IOError("bad database file: truncated meta page")
            meta = Meta.unpack_from(first_page)
            # 基本一致性校验
            if meta.magic != _MAGIC:
                raise IOError("bad magic; not a mini-db file")
            if meta.page_size != page_size:
                # 外部传入的 page_size 必须与文件中记录的一致
                raise IOError(f"page size mismatch: file={meta.page_size}, expected={page_size}")
            self.meta = meta
        else:
            # 创建新文件：初始化 Meta，并把第 0 页写满
            self._f = open(self.path, "w+b", buffering=0)
            # 初始只有 1 页（元页），空闲链表为空
            self.meta = Meta(_MAGIC, _VERSION, page_size, page_count=1, free_head=-1)
            self._write_meta()  # 写入第 0 页（用零填充补齐整页）
            # 确保物理文件至少有 1 页大小（若 _write_meta 未拉伸到整页，这里补齐）
            self._f.truncate(self.meta.page_size)
            self._f.flush()
            os.fsync(self._f.fileno())

    # ------------------------- 公共 API -------------------------

    def page_size(self) -> int:
        """返回固定的页大小（字节）。"""
        return self.meta.page_size

    def read_page(self, page_id: int) -> bytes:
        """
        读取整页数据（bytes）：
          - 检查页号范围
          - 定位到 page_id * page_size 处，读取一整页
          - 若读取长度不足视为损坏
        注：BufferPool 正常情况下应优先从缓存取；直连 read_page 可能绕过缓存。
        """
        self._check_pid(page_id)
        self._f.seek(page_id * self.meta.page_size)
        data = self._f.read(self.meta.page_size)
        if len(data) != self.meta.page_size:
            raise IOError("short read (corrupted file?)")
        return data

    def write_page(self, page_id: int, data: bytes) -> None:
        """
        将一整页写回磁盘：
          - 长度必须等于 page_size
          - 直接覆盖该页位置
        """
        self._check_pid(page_id)
        if len(data) != self.meta.page_size:
            raise ValueError(f"write_page: bad data size {len(data)} != {self.meta.page_size}")
        self._f.seek(page_id * self.meta.page_size)
        self._f.write(data)

    def allocate_page(self) -> int:
        """
        分配一个新页，返回其 page_id。
        策略：
          - 若空闲链表非空：弹出 free_head 指向的页作为结果，并把链表头更新为它的 next
          - 若空闲链表为空：在文件末尾“追加”一个新页（全 0），并递增 page_count
        无论从空闲链取还是追加，返回前都会把该页内容清零（写一页 0）。
        """
        if self.meta.free_head >= 0:
            # 1) 从空闲链表取一个
            pid = self.meta.free_head
            raw = self._read_exact(pid)
            (next_free,) = struct.unpack_from(_FREE_HDR_FMT, raw, 0)
            self.meta.free_head = next_free
            self._write_meta()
            # 清零该页（防止泄露旧内容）
            zero = bytes(self.meta.page_size)
            self.write_page(pid, zero)
            return pid
        else:
            # 2) 追加新页：当前 page_count 即新页下标
            pid = self.meta.page_count
            self.meta.page_count += 1
            self._write_meta()
            # 将文件扩展到新页位置并写入 0 填充
            self._f.seek(pid * self.meta.page_size)
            self._f.write(bytes(self.meta.page_size))
            return pid

    def free_page(self, page_id: int) -> None:
        """
        释放一个页（挂回空闲链表头）：
          - 禁止释放元页 0
          - 在该页页首写入“下一个空闲页”的指针（当前 free_head）
          - 再更新 meta.free_head = page_id
        复杂度 O(1)。
        """
        self._check_pid(page_id)
        if page_id == 0:
            raise ValueError("cannot free meta page 0")
        # 在被释放页的页首写入 next_free_page_id = 当前链表头
        buf = bytearray(self.meta.page_size)
        struct.pack_into(_FREE_HDR_FMT, buf, 0, self.meta.free_head)
        # 其余字节保持 0（buf 已是零化）
        self.write_page(page_id, bytes(buf))
        # 更新链表头指向该页
        self.meta.free_head = page_id
        self._write_meta()

    def page_count(self) -> int:
        """返回当前文件中总页数（包含第 0 页）。"""
        return self.meta.page_count

    def sync(self) -> None:
        """
        强制将文件缓冲区刷入磁盘（fsync）：
          - 保证 Meta 和页面数据都已持久化（崩溃后一致）
        """
        self._f.flush()
        os.fsync(self._f.fileno())

    def close(self) -> None:
        """关闭前先 sync，确保落盘安全。"""
        try:
            self.sync()
        finally:
            self._f.close()

    # ------------------------- 内部方法 -------------------------

    def _check_pid(self, pid: int) -> None:
        """检查页号是否在 [0, page_count) 范围内。"""
        if pid < 0 or pid >= self.meta.page_count:
            raise IndexError(f"page_id out of range: {pid} (page_count={self.meta.page_count})")

    def _read_exact(self, page_id: int) -> bytes:
        """读取一整页（内部使用），长度不足则视为损坏。"""
        self._f.seek(page_id * self.meta.page_size)
        data = self._f.read(self.meta.page_size)
        if len(data) != self.meta.page_size:
            raise IOError("short read")
        return data

    def _write_meta(self) -> None:
        """
        将 Meta 写入第 0 页：
          - 先 pack 出有效头部
          - 构造一个整页的缓冲区，前缀填入 Meta，其余补 0
          - 覆写到文件开头并 fsync
        """
        page = bytearray(self.meta.page_size)
        packed = self.meta.pack()
        page[: len(packed)] = packed
        self._f.seek(0)
        self._f.write(page)
        self._f.flush()
        os.fsync(self._f.fileno())
