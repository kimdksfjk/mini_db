
from abc import ABC, abstractmethod
class Op(ABC):
    def open(self): pass
    @abstractmethod
    def next(self): ...  # -> dict | None
    def close(self): pass
