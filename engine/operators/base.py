# engine/operators/base.py
from __future__ import annotations
from typing import Iterable, Iterator, Dict, Any, List

class Operator:
    """所有关系算子的基类（可迭代产生行: Dict[str,Any])"""
    def __iter__(self) -> Iterator[Dict[str, Any]]:
        yield from self.execute()

    @property
    def schema(self) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def execute(self) -> Iterable[Dict[str, Any]]:
        raise NotImplementedError
