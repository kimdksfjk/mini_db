
"""
Pager: fixed-size page I/O, allocation/free, backed by files.
Exposed APIs:
  - read_page(file_id: str, page_id: int) -> bytes
  - write_page(file_id: str, page_id: int, data: bytes) -> None
  - alloc_page(file_id: str) -> int
  - free_page(file_id: str, page_id: int) -> None
  - flush_page(file_id: str, page_id: int) -> None
"""
PAGE_SIZE = 4096

def read_page(file_id: str, page_id: int) -> bytes:
    raise NotImplementedError

def write_page(file_id: str, page_id: int, data: bytes) -> None:
    raise NotImplementedError

def alloc_page(file_id: str) -> int:
    raise NotImplementedError

def free_page(file_id: str, page_id: int) -> None:
    raise NotImplementedError

def flush_page(file_id: str, page_id: int) -> None:
    raise NotImplementedError
