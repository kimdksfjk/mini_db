
"""
Slotted Page layout suggestion:
  [Header 16B] page_id(4) | free_off(4) | slot_cnt(4) | flags(4)
  [Records Area ... ↑]
  [Slot Directory ↓ ...] each 8B: offset(4), length(4) (length=0 => tombstone)
"""
from typing import Tuple, Optional

HEADER_SIZE = 16

def init_page(page_id: int, page_size: int = 4096) -> bytearray:
    # TODO: Write header, set free_off, slot_cnt=0
    raise NotImplementedError

def insert_record(page: bytearray, rec: bytes) -> Optional[int]:
    """Return slot_id or None if not enough space"""
    raise NotImplementedError

def read_record(page: bytes, slot_id: int) -> bytes:
    raise NotImplementedError

def delete_record(page: bytearray, slot_id: int) -> None:
    raise NotImplementedError
