# engine/operators/join.py
from __future__ import annotations
from typing import Dict, Any, Iterable, List
from .base import Operator

def _base_col(name: str) -> str:
    # 去掉别名与表前缀："t.col AS c" / "t.col" / "col"
    return name.split(" AS ")[0].split(".")[-1]

def _join_kind(s: str | None) -> str:
    s = (s or "INNER").upper()
    # 统一成 "INNER" / "LEFT" / "RIGHT"
    if s.startswith("LEFT"):
        return "LEFT"
    if s.startswith("RIGHT"):
        return "RIGHT"
    return "INNER"

class NestedLoopJoin(Operator):
    """
    朴素嵌套循环连接（可跑版本）：
    - 支持：INNER / LEFT / RIGHT
    - 条件：仅支持等值 ON a.col = b.col
    - 列名冲突：右侧重名列加后缀 "_r"
    """
    def __init__(self, left: Operator, right: Operator, join_type: str, on: Dict[str, Any]) -> None:
        self.left = left
        self.right = right
        self.join_type = _join_kind(join_type)
        self.on = on
        if (on or {}).get("operator") not in ("=", "=="):
            raise NotImplementedError("JOIN 目前只支持等值连接（=）。")

    @property
    def schema(self) -> List[Dict[str, Any]]:
        names = set()
        out: List[Dict[str, Any]] = []
        for col in self.left.schema:
            names.add(col["name"])
            out.append({"name": col["name"], "type": col.get("type", "")})
        for col in self.right.schema:
            n = col["name"]
            if n in names:
                n = n + "_r"
            out.append({"name": n, "type": col.get("type", "")})
        return out

    def _probe(self, lrow: Dict[str, Any], lkey: str, right_rows: List[Dict[str, Any]], rkey: str):
        lk = lrow.get(lkey)
        for rrow in right_rows:
            if lk == rrow.get(rkey):
                # 合并一行（右侧重名后缀 _r）
                out = dict(lrow)
                for c in self.right.schema:
                    name = c["name"]
                    outname = name if name not in out else name + "_r"
                    out[outname] = rrow.get(name)
                yield out

    def execute(self) -> Iterable[Dict[str, Any]]:
        # 解析左右键名
        lkey = _base_col(self.on.get("left_column", ""))
        rkey = _base_col(self.on.get("right_column", ""))

        # RIGHT 等价于交换左右后做 LEFT
        if self.join_type == "RIGHT":
            # 交换方向：右作为保留侧
            # 注意同时交换连接键
            self.left, self.right = self.right, self.left
            lkey, rkey = rkey, lkey
            self.join_type = "LEFT"

        right_rows = list(self.right)
        for lrow in self.left:
            matched_any = False
            for out in self._probe(lrow, lkey, right_rows, rkey):
                matched_any = True
                yield out
            if not matched_any and self.join_type == "LEFT":
                out = dict(lrow)
                for c in self.right.schema:
                    name = c["name"]
                    outname = name if name not in out else name + "_r"
                    out[outname] = None
                yield out
