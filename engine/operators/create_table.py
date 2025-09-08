
from .base import Op
class CreateTable(Op):
    def __init__(self, catalog, plan):
        self.catalog = catalog
        self.plan = plan
        self.done = False
    def open(self): pass
    def next(self):
        if self.done: return None
        table = self.plan["table"]
        columns = self.plan["columns"]
        self.catalog.create_table(table, columns)
        self.done = True
        return {"ok": True}
    def close(self): pass
