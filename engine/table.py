
"""TableHeap abstraction: append, scan, delete (stub)."""
from typing import Iterator, Tuple, Dict, Any

class TableHeap:
    def __init__(self, file_id: str):
        self.file_id = file_id

    def append(self, values) -> Tuple[int, int]:
        # TODO: Encode record and append to a page; return (page_id, slot_id)
        raise NotImplementedError

    def scan(self) -> Iterator[Dict[str, Any]]:
        # TODO: Iterate all pages/slots and yield row dicts
        raise NotImplementedError

    def delete_where(self, predicate) -> int:
        # TODO: Mark matched records as deleted; return count
        raise NotImplementedError
