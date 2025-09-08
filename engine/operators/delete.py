
from .base import Op
from ..expressions import eval_expr
class Delete(Op):
    def __init__(self, table, predicate):
        self.table = table
        self.predicate = predicate
        self.done = False
    def open(self): pass
    def next(self):
        if self.done: return None
        # In a real impl, we'd SeqScan+Filter, but keep stubbed API:
        affected = self.table.delete_where(self.predicate)
        self.done = True
        return {"affected": affected}
    def close(self): pass
