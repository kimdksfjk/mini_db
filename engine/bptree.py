# engine/bptree.py
from __future__ import annotations
from typing import Any, List, Optional, Iterable

def _cmp_key(a: Any, b: Any) -> int:
    try:
        af, bf = float(a), float(b)
        if af < bf: return -1
        if af > bf: return 1
        return 0
    except Exception:
        sa, sb = str(a), str(b)
        if sa < sb: return -1
        if sa > sb: return 1
        return 0

class _Leaf:
    __slots__ = ("keys","vals","next")
    def __init__(self):
        self.keys: List[Any] = []
        self.vals: List[List[dict]] = []  # 同键可能多行
        self.next: Optional[_Leaf] = None

class _Inner:
    __slots__ = ("keys","children")
    def __init__(self):
        self.keys: List[Any] = []
        self.children: List[object] = []  # _Inner 或 _Leaf

class BPlusTree:
    """
    纯内存 B+ 树（教学版）：
      - 阶 M（每个节点最多 M-1 个键，M 个孩子）
      - 叶子用 next 串链，支持范围扫描
    """
    def __init__(self, order: int = 64):
        assert order >= 4
        self.M = order
        self.root: object = _Leaf()

    # ----- 搜索 -----
    def _find_leaf(self, key: Any) -> _Leaf:
        node = self.root
        while isinstance(node, _Inner):
            i = 0
            while i < len(node.keys) and _cmp_key(key, node.keys[i]) >= 0:
                i += 1
            node = node.children[i]
        return node

    def search_eq(self, key: Any) -> Iterable[dict]:
        leaf = self._find_leaf(key)
        for k, vs in zip(leaf.keys, leaf.vals):
            if _cmp_key(k, key) == 0:
                for r in vs:
                    yield r
                return
        return

    def search_range(self, low: Any = None, high: Any = None, incl_low=True, incl_high=True) -> Iterable[dict]:
        # 找到起始叶
        node = self.root
        while isinstance(node, _Inner):
            # 找最左可行
            i = 0
            if low is None:
                i = 0
            else:
                while i < len(node.keys) and _cmp_key(low, node.keys[i]) >= 0:
                    i += 1
            node = node.children[i]
        leaf = node
        # 在第一个叶里找到起始位置
        started = (low is None)
        while leaf:
            for k, vs in zip(leaf.keys, leaf.vals):
                if not started:
                    c = _cmp_key(k, low)
                    if c < 0 or (c == 0 and not incl_low):
                        continue
                    started = True
                if high is not None:
                    c2 = _cmp_key(k, high)
                    if c2 > 0 or (c2 == 0 and not incl_high):
                        return
                for r in vs:
                    yield r
            leaf = leaf.next

    # ----- 插入 -----
    def insert(self, key: Any, row: dict) -> None:
        path: List[_Inner] = []
        node = self.root
        # 下降并记录路径
        while isinstance(node, _Inner):
            path.append(node)
            i = 0
            while i < len(node.keys) and _cmp_key(key, node.keys[i]) >= 0:
                i += 1
            node = node.children[i]
        leaf: _Leaf = node
        # 插入到叶
        i = 0
        while i < len(leaf.keys) and _cmp_key(key, leaf.keys[i]) > 0:
            i += 1
        if i < len(leaf.keys) and _cmp_key(leaf.keys[i], key) == 0:
            leaf.vals[i].append(row)
        else:
            leaf.keys.insert(i, key)
            leaf.vals.insert(i, [row])
        # 分裂
        self._split_upward_leaf(leaf, path)

    def _split_upward_leaf(self, leaf: _Leaf, path: List[_Inner]) -> None:
        if len(leaf.keys) <= self.M - 1:
            return
        mid = len(leaf.keys)//2
        right = _Leaf()
        right.keys = leaf.keys[mid:]
        right.vals = leaf.vals[mid:]
        leaf.keys = leaf.keys[:mid]
        leaf.vals = leaf.vals[:mid]
        right.next = leaf.next
        leaf.next = right
        sep = right.keys[0]
        self._insert_to_parent(leaf, sep, right, path)

    def _insert_to_parent(self, left: object, sep_key: Any, right: object, path: List[_Inner]) -> None:
        if not path:
            # 新建根
            root = _Inner()
            root.keys = [sep_key]
            root.children = [left, right]
            self.root = root
            return
        parent = path.pop()
        # 在 parent 中插入 (sep,right) 到 left 之后
        i = 0
        while i < len(parent.children) and parent.children[i] is not left:
            i += 1
        parent.keys.insert(i, sep_key)
        parent.children.insert(i+1, right)
        # 若 parent 溢出则继续向上分裂
        if len(parent.keys) > self.M - 1:
            self._split_upward_inner(parent, path)

    def _split_upward_inner(self, node: _Inner, path: List[_Inner]) -> None:
        mid = len(node.keys)//2
        sep = node.keys[mid]
        left = _Inner()
        right = _Inner()
        left.keys = node.keys[:mid]
        left.children = node.children[:mid+1]
        right.keys = node.keys[mid+1:]
        right.children = node.children[mid+1:]
        # 如果 node 就是根
        if not path and node is self.root:
            self.root = _Inner()
            self.root.keys = [sep]
            self.root.children = [left, right]
            return
        # 否则把 (sep, right) 插到父节点
        self._insert_to_parent(left, sep, right, path)
