
from .base import Op
from ..expressions import eval_expr
class Filter(Op):
    def __init__(self, child, predicate):
        self.child = child
        self.predicate = predicate
    def open(self):
        self.child.open()
    def next(self):
        while True:
            row = self.child.next()
            if row is None: return None
            if eval_expr(row, self.predicate):
                return row
    def close(self):
        self.child.close()
