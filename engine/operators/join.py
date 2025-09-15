# engine/operators/join.py
from __future__ import annotations
from typing import Dict, Any, Iterable, List, Tuple

def _parse_table_alias(spec: str) -> Tuple[str, str]:
    """
    把 "student AS s" / "student s" / "student" 解析为 (表名, 别名)
    """
    s = spec.strip()
    up = s.upper()
    if " AS " in up:
        # 按大小写不敏感处理 AS
        parts = s.split(" AS ")
        if len(parts) == 2:
            return parts[0].strip(), parts[1].strip()
    # "student s" 形式
    toks = s.split()
    if len(toks) == 2:
        return toks[0].strip(), toks[1].strip()
    return s, s  # 无别名时，别名=表名

def _qualify_row(row: Dict[str, Any], table: str, alias: str, as_left: bool) -> Dict[str, Any]:
    """
    为一行生成带前缀的列名，形如 's.id'；左侧表可同时保留未加前缀的列名（便于兼容旧查询）
    """
    out: Dict[str, Any] = {}
    for k, v in row.items():
        out[f"{alias}.{k}"] = v
        if as_left:
            # 左表保留未加前缀，尽量不覆盖（已有同名键则以已有为准）
            out.setdefault(k, v)
    return out

def _get_val(r: Dict[str, Any], col: str):
    """
    从行中取值：先查完全匹配，再尝试去掉前缀后的列名。
    """
    if col in r:
        return r[col]
    if "." in col:
        col2 = col.split(".", 1)[1]
        return r.get(col2)
    return r.get(col)

def _merge_rows(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(a)
    for k, v in b.items():
        if k not in out:
            out[k] = v
        else:
            # 同键冲突时，优先保留已有（一般来自左表），右表有前缀通常不会冲突
            pass
    return out

class JoinOperator:
    """
    轻量 JOIN 实现：
      - 支持 INNER / LEFT（包含 "LEFT OUTER"），等值连接优先用哈希连接；
      - 非等值连接（>,<,>=,<=,!=,<>) 退化为嵌套循环；
      - 只依赖 SeqScan 提供的行集（由 executor 调用时传入），不直接访问存储。
    """
    def __init__(self, catalog, storage):
        self.catalog = catalog
        self.storage = storage

    # --- 对外：从主表和 join 规格生成联接后的行流 ---
    def execute(self, main_table_spec: str, joins: List[Dict[str, Any]], seq_scan_op) -> Iterable[Dict[str, Any]]:
        """
        main_table_spec: 例如 "student AS s"
        joins: [{"type": "...", "right_table": "course AS c", "on_condition": {...}}, ...]
        seq_scan_op: 已构造好的 SeqScanOperator（避免重复 import）
        """
        base_table, base_alias = _parse_table_alias(main_table_spec)
        # 拉取主表行，并加上前缀
        left_rows = [_qualify_row(r, base_table, base_alias, as_left=True) for r in seq_scan_op.scan(base_table)]

        if not joins:
            # 无联接，直接返回主表
            for r in left_rows:
                yield r
            return

        current = left_rows
        for j in joins:
            jtype = (j.get("type") or "INNER").upper()
            # LEFT 或 LEFT OUTER
            if jtype.startswith("LEFT"):
                mode = "LEFT"
            elif jtype == "INNER":
                mode = "INNER"
            else:
                raise NotImplementedError(f"JOIN 类型暂不支持：{jtype}")

            right_spec = j.get("right_table") or ""
            r_table, r_alias = _parse_table_alias(right_spec)
            on = j.get("on_condition") or {}
            op = on.get("operator", "=")

            # 扫右表 + 前缀
            right_rows_raw = list(seq_scan_op.scan(r_table))
            right_rows = [_qualify_row(r, r_table, r_alias, as_left=False) for r in right_rows_raw]
            # 统计右表所有键，用于 LEFT JOIN 未匹配时补 None
            right_all_keys = set()
            for rr in right_rows:
                right_all_keys.update(rr.keys())

            # 做联接
            nxt: List[Dict[str, Any]] = []
            if op == "=":
                # 等值：简单哈希连接（右表建哈希）
                # 选择哈希键：优先 on['right_column']，否则退化
                rkey_name = on.get("right_column")
                lkey_name = on.get("left_column")
                if not rkey_name or not lkey_name:
                    # 信息不足，退化 NLJ
                    for lr in current:
                        matched = False
                        lv = _get_val(lr, lkey_name or "")
                        for rr in right_rows:
                            rv = _get_val(rr, rkey_name or "")
                            if lv == rv:
                                nxt.append(_merge_rows(lr, rr))
                                matched = True
                        if not matched and mode == "LEFT":
                            # LEFT：补 None
                            tmp = dict(lr)
                            for k in right_all_keys:
                                tmp.setdefault(k, None)
                            nxt.append(tmp)
                else:
                    # 构建哈希
                    bucket: Dict[Any, List[Dict[str, Any]]] = {}
                    for rr in right_rows:
                        key = _get_val(rr, rkey_name)
                        bucket.setdefault(key, []).append(rr)
                    # 探测
                    for lr in current:
                        lv = _get_val(lr, lkey_name)
                        rlist = bucket.get(lv, [])
                        if rlist:
                            for rr in rlist:
                                nxt.append(_merge_rows(lr, rr))
                        elif mode == "LEFT":
                            tmp = dict(lr)
                            for k in right_all_keys:
                                tmp.setdefault(k, None)
                            nxt.append(tmp)
            else:
                # 非等值：嵌套循环
                cmpop = op
                def _cmp(a, b) -> bool:
                    try:
                        if cmpop == "!=" or cmpop == "<>": return a != b
                        if cmpop == ">":  return a > b
                        if cmpop == "<":  return a < b
                        if cmpop == ">=": return a >= b
                        if cmpop == "<=": return a <= b
                    except Exception:
                        return False
                    return False

                lkey_name = on.get("left_column")
                rkey_name = on.get("right_column")
                for lr in current:
                    matched = False
                    lv = _get_val(lr, lkey_name or "")
                    for rr in right_rows:
                        rv = _get_val(rr, rkey_name or "")
                        if _cmp(lv, rv):
                            nxt.append(_merge_rows(lr, rr))
                            matched = True
                    if not matched and mode == "LEFT":
                        tmp = dict(lr)
                        for k in right_all_keys:
                            tmp.setdefault(k, None)
                        nxt.append(tmp)

            current = nxt

        # 输出
        for r in current:
            yield r
