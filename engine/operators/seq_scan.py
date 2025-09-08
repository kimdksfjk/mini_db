
from .base import Op
class SeqScan(Op):
    def __init__(self, table):
        self.table = table
        self._iter = None
    def open(self):
        self._iter = self.table.scan()
    def next(self):
        try:
            return next(self._iter)
        except StopIteration:
            return None
    def close(self): 
        self._iter = None
