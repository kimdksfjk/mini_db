
from .base import Op
class Project(Op):
    def __init__(self, child, columns):
        self.child = child
        self.columns = columns
    def open(self):
        self.child.open()
    def next(self):
        row = self.child.next()
        if row is None: return None
        return {k: row[k] for k in self.columns}
    def close(self):
        self.child.close()
