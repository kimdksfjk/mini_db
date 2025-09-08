
from .base import Op
class Insert(Op):
    def __init__(self, table, values):
        self.table = table
        self.values = values
        self.done = False
    def open(self): pass
    def next(self):
        if self.done: return None
        self.table.append(self.values)
        self.done = True
        return {"affected": 1}
    def close(self): pass
