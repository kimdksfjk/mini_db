# engine/bptree.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Any, List, Optional, Iterable


def _cmp_key(a: Any, b: Any) -> int:
    """
    比较两个键值的大小关系。
    优先按数值比较；若转换失败则按字符串字典序比较。

    返回：
        -1：a < b
         0：a == b
         1：a > b
    """
    try:
        af, bf = float(a), float(b)
        if af < bf:
            return -1
        if af > bf:
            return 1
        return 0
    except Exception:
        sa, sb = str(a), str(b)
        if sa < sb:
            return -1
        if sa > sb:
            return 1
        return 0


class _Leaf:
    """
    叶子节点：
      - keys：有序键列表
      - vals：与 keys 一一对应的值列表（每个键可存放多行记录，故为 List[List[dict]]）
      - next：指向右兄弟叶子，用于范围顺扫
    """
    __slots__ = ("keys", "vals", "next")

    def __init__(self):
        self.keys: List[Any] = []
        self.vals: List[List[dict]] = []
        self.next: Optional[_Leaf] = None


class _Inner:
    """
    内部节点：
      - keys：分隔键（上升键）
      - children：孩子指针列表（长度比 keys 多 1），元素为 _Inner 或 _Leaf
    """
    __slots__ = ("keys", "children")

    def __init__(self):
        self.keys: List[Any] = []
        self.children: List[object] = []


class BPlusTree:
    """
    纯内存 B+ 树：
      - 阶（order）为 M：每个节点最多持有 M-1 个键、M 个孩子。
      - 所有有效数据存放于叶子节点；叶子间通过 next 串联以支持范围扫描。
    """

    def __init__(self, order: int = 64):
        """
        初始化一棵空树。

        参数：
            order：树的阶（M），要求 >= 4。
        """
        assert order >= 4
        self.M = order
        self.root: object = _Leaf()

    # =========================
    # 查找
    # =========================
    def _find_leaf(self, key: Any) -> _Leaf:
        """
        从根出发按键值下降，定位应包含该 key 的叶子节点。
        """
        node = self.root
        while isinstance(node, _Inner):
            i = 0
            while i < len(node.keys) and _cmp_key(key, node.keys[i]) >= 0:
                i += 1
            node = node.children[i]
        return node  # type: ignore[return-value]

    def search_eq(self, key: Any) -> Iterable[dict]:
        """
        等值查找：返回所有与 key 相等的记录（可能多条）。
        """
        leaf = self._find_leaf(key)
        for k, vs in zip(leaf.keys, leaf.vals):
            if _cmp_key(k, key) == 0:
                for r in vs:
                    yield r
                return
        return

    def search_range(
        self,
        low: Any = None,
        high: Any = None,
        incl_low: bool = True,
        incl_high: bool = True
    ) -> Iterable[dict]:
        """
        范围查找：按键值区间 [low, high] 返回记录，边界可配置是否包含。

        参数：
            low：下界；为 None 表示无下界。
            high：上界；为 None 表示无上界。
            incl_low：是否包含下界。
            incl_high：是否包含上界。
        """
        # 1) 自根向下，找到可能包含 low 的最左路径
        node = self.root
        while isinstance(node, _Inner):
            i = 0
            if low is None:
                i = 0
            else:
                while i < len(node.keys) and _cmp_key(low, node.keys[i]) >= 0:
                    i += 1
            node = node.children[i]
        leaf = node  # type: ignore[assignment]

        # 2) 在首个叶子中定位起点，然后顺着 next 串链向右扫
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

    # =========================
    # 插入
    # =========================
    def insert(self, key: Any, row: dict) -> None:
        """
        插入一条记录：
          - 若 key 已存在，追加到该键对应的记录列表；
          - 若 key 不存在，插入有序位置；
          - 发生溢出时自底向上分裂并可能提升新根。
        """
        path: List[_Inner] = []
        node = self.root

        # 1) 下降并记录沿途内部节点（用于回溯分裂）
        while isinstance(node, _Inner):
            path.append(node)
            i = 0
            while i < len(node.keys) and _cmp_key(key, node.keys[i]) >= 0:
                i += 1
            node = node.children[i]

        # 2) 插入到目标叶子
        leaf: _Leaf = node  # type: ignore[assignment]
        i = 0
        while i < len(leaf.keys) and _cmp_key(key, leaf.keys[i]) > 0:
            i += 1
        if i < len(leaf.keys) and _cmp_key(leaf.keys[i], key) == 0:
            leaf.vals[i].append(row)
        else:
            leaf.keys.insert(i, key)
            leaf.vals.insert(i, [row])

        # 3) 如有必要，自底向上分裂
        self._split_upward_leaf(leaf, path)

    def _split_upward_leaf(self, leaf: _Leaf, path: List[_Inner]) -> None:
        """
        叶子溢出处理：二分叶子，建立右兄弟并把第一个右侧键提升给父节点。
        """
        if len(leaf.keys) <= self.M - 1:
            return


        mid = len(leaf.keys) // 2
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
        """
        将分裂产生的 (sep_key, right) 插入父节点。
        若父节点不存在（分裂发生在根），则创建新根。
        """
        if not path:
            root = _Inner()
            root.keys = [sep_key]
            root.children = [left, right]
            self.root = root
            return

        parent = path.pop()
        i = 0
        while i < len(parent.children) and parent.children[i] is not left:
            i += 1
        parent.keys.insert(i, sep_key)
        parent.children.insert(i + 1, right)

        if len(parent.keys) > self.M - 1:
            self._split_upward_inner(parent, path)

    def _split_upward_inner(self, node: _Inner, path: List[_Inner]) -> None:
        """
        内部节点溢出处理：按中位键分裂为左右两部分，并将中位键上推至父节点。
        如分裂点为根，则创建新根。
        """
        mid = len(node.keys) // 2
        sep = node.keys[mid]

        left = _Inner()
        right = _Inner()
        left.keys = node.keys[:mid]
        left.children = node.children[: mid + 1]
        right.keys = node.keys[mid + 1 :]
        right.children = node.children[mid + 1 :]

        if not path and node is self.root:
            self.root = _Inner()
            self.root.keys = [sep]
            self.root.children = [left, right]
            return

        self._insert_to_parent(left, sep, right, path)
