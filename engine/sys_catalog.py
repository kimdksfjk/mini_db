# engine/sys_catalog.py
from __future__ import annotations
import os, json, glob
from typing import Dict, Any, List, Optional

class SysCatalog:
    """
    用两张“系统表”（都是 .mdb 堆文件）保存目录元数据：
      - __sys_tables(name, columns, storage)
      - __sys_indexes(table, name, column, type, storage, unique)
    启动时加载到内存缓存 self._tables / self._indexes_by_table，热路径只查内存。
    兼容已有数据：首次运行会扫描 data_dir 下已有表/索引目录的 meta.json，补登记进系统表。
    """
    SYS_TABLES = "__sys_tables"
    SYS_INDEXES = "__sys_indexes"

    def __init__(self, data_dir: str, storage_adapter):
        self.data_dir = os.path.abspath(data_dir)
        self.storage = storage_adapter
        os.makedirs(self.data_dir, exist_ok=True)

        # 确保系统表存在（若不存在则创建）
        self._desc_tables = self._ensure_sys_table(self.SYS_TABLES)
        self._desc_indexes = self._ensure_sys_table(self.SYS_INDEXES)

        # 内存缓存
        self._tables: Dict[str, Dict[str, Any]] = {}              # name -> {"columns":[...],"storage":{...}}
        self._indexes_by_table: Dict[str, Dict[str, Dict]] = {}   # table -> {index_name: meta}

        # 从系统表加载
        self._load_cache_from_sys()

        # 发现/迁移旧目录（只跑一次，有就补登记）
        self._discover_existing_tables()
        self._discover_existing_indexes()

    # ---------- 系统表存在性 ----------
    def _read_meta_desc(self, table_name: str) -> Optional[Dict[str, Any]]:
        meta_path = os.path.join(self.data_dir, table_name, "meta.json")
        if not os.path.exists(meta_path):
            return None
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        # 兼容不同结构：优先取 "storage"
        return meta.get("storage", meta)

    def _ensure_sys_table(self, tname: str) -> Dict[str, Any]:
        """
        若已存在（有目录且有 meta.json），读取其 storage 描述；
        否则调用 storage_adapter.create_table 创建。
        """
        desc = self._read_meta_desc(tname)
        if desc:
            return desc
        # 系统表的列模式硬编码（便于引导），底层仍是 .mdb 堆文件
        if tname == self.SYS_TABLES:
            columns = [{"name":"name","type":"VARCHAR"},
                       {"name":"columns","type":"JSON"},
                       {"name":"storage","type":"JSON"}]
        else:
            columns = [{"name":"table","type":"VARCHAR"},
                       {"name":"name","type":"VARCHAR"},
                       {"name":"column","type":"VARCHAR"},
                       {"name":"type","type":"VARCHAR"},
                       {"name":"storage","type":"JSON"},
                       {"name":"unique","type":"INT"}]
        return self.storage.create_table(tname, columns)

    # ---------- 加载缓存 ----------
    def _load_cache_from_sys(self) -> None:
        # tables
        opened = self.storage.open_table(self.SYS_TABLES, self._desc_tables)
        for row in self.storage.scan_rows(opened):
            name = row.get("name")
            if not name: continue
            self._tables[name] = {"columns": row.get("columns") or [], "storage": row.get("storage") or {}}
        # indexes
        opened_i = self.storage.open_table(self.SYS_INDEXES, self._desc_indexes)
        for row in self.storage.scan_rows(opened_i):
            t = row.get("table"); iname = row.get("name")
            if not t or not iname: continue
            self._indexes_by_table.setdefault(t, {})
            self._indexes_by_table[t][iname] = {
                "column": row.get("column"),
                "type": row.get("type","BTREE"),
                "storage": row.get("storage") or {},
                "unique": bool(row.get("unique", 0))
            }

    # ---------- 发现/迁移 ----------
    def _discover_existing_tables(self) -> None:
        """
        扫描 data_dir 下现有表目录（排除系统表与索引目录），将未登记的表写入 __sys_tables。
        """
        dirs = [p for p in glob.glob(os.path.join(self.data_dir, "*")) if os.path.isdir(p)]
        for d in dirs:
            name = os.path.basename(d)
            if name in (self.SYS_TABLES, self.SYS_INDEXES) or name.startswith("__idx__"):
                continue
            if name in self._tables:
                continue
            desc = self._read_meta_desc(name)
            if not desc: continue
            # 读取列定义（如果 meta.json 里有）
            cols = []
            try:
                with open(os.path.join(d, "meta.json"), "r", encoding="utf-8") as f:
                    meta = json.load(f)
                cols = meta.get("columns", [])
            except Exception:
                pass
            self._insert_sys_table(name, cols, desc)

    def _discover_existing_indexes(self) -> None:
        """
        扫描 data_dir 下索引目录（形如 __idx__<table>__<index>），将未登记的索引写入 __sys_indexes。
        """
        dirs = [p for p in glob.glob(os.path.join(self.data_dir, "__idx__*")) if os.path.isdir(p)]
        for d in dirs:
            b = os.path.basename(d)
            if "__" not in b[6:]:  # "__idx__" 后还需要有分隔
                continue
            try:
                # 格式：__idx__{table}__{index}
                rest = b[len("__idx__"):]
                table, iname = rest.split("__", 1)
            except Exception:
                continue
            # 已登记？
            if self._indexes_by_table.get(table, {}).get(iname):
                continue
            desc = self._read_meta_desc(b)
            if not desc: continue
            # 读取列名（元信息：建索引时会保留 column 字段）
            column = None
            try:
                with open(os.path.join(d, "meta.json"), "r", encoding="utf-8") as f:
                    meta = json.load(f)
                column = (meta.get("extra") or {}).get("column")
            except Exception:
                pass
            self._insert_sys_index(table, iname, column or "", "BTREE", desc, unique=False)

    # ---------- 写系统表 ----------
    def _insert_sys_table(self, name: str, columns: List[Dict[str,Any]], storage_desc: Dict[str,Any]) -> None:
        opened = self.storage.open_table(self.SYS_TABLES, self._desc_tables)
        self.storage.insert_row(opened, {"name": name, "columns": columns, "storage": storage_desc})
        self._tables[name] = {"columns": columns, "storage": storage_desc}

    def _insert_sys_index(self, table: str, iname: str, column: str, itype: str,
                          storage_desc: Dict[str,Any], unique: bool=False) -> None:
        opened = self.storage.open_table(self.SYS_INDEXES, self._desc_indexes)
        self.storage.insert_row(opened, {
            "table": table, "name": iname, "column": column, "type": itype,
            "storage": storage_desc, "unique": int(bool(unique))
        })
        self._indexes_by_table.setdefault(table, {})
        self._indexes_by_table[table][iname] = {
            "column": column, "type": itype, "storage": storage_desc, "unique": bool(unique)
        }

    # ---------- 对外 API（供 Catalog / IndexRegistry 使用） ----------
    # 表
    def get_table(self, name: str) -> Dict[str, Any]:
        if name not in self._tables:
            raise KeyError(f"table '{name}' not found")
        return self._tables[name]

    # engine/sys_catalog.py 里的方法
    from typing import Dict, Any, List, Optional

    def create_table_and_register(self, name: str, columns: List[Dict[str, Any]],
                                  storage_desc: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        若传入 storage_desc（外部已创建 .mdb），则直接登记系统表；
        否则调用 storage.create_table() 创建 .mdb 再登记。
        """
        if name in self._tables:
            raise KeyError(f"table '{name}' already exists")

        if storage_desc is None:
            storage_desc = self.storage.create_table(name, columns)  # 不写 meta.json 的版本

        # 登记系统表缓存与物理系统表
        self._insert_sys_table(name, columns, storage_desc)
        return {"columns": columns, "storage": storage_desc}

    def list_tables(self) -> List[str]:
        return sorted(self._tables.keys())

    # 索引
    def add_index(self, table: str, iname: str, column: str, storage_desc: Dict[str, Any],
                  itype: str = "BTREE", unique: bool = False) -> None:
        self._insert_sys_index(table, iname, column, itype, storage_desc, unique)

    def drop_index(self, table: str, iname: str) -> None:
        d = self._indexes_by_table.get(table, {})
        if iname in d:
            del d[iname]
        # 重新写 __sys_indexes
        opened = self.storage.open_table(self.SYS_INDEXES, self._desc_indexes)
        self.storage.clear_table(opened)
        # 关键：清空后需要重新 open
        opened = self.storage.open_table(self.SYS_INDEXES, self._desc_indexes)
        for t, mp in self._indexes_by_table.items():
            for nm, meta in mp.items():
                self.storage.insert_row(opened, {
                    "table": t, "name": nm, "column": meta.get("column"),
                    "type": meta.get("type", "BTREE"),
                    "storage": meta.get("storage") or {}, "unique": int(bool(meta.get("unique", False)))
                })
    def list_indexes(self, table: Optional[str]=None) -> Dict[str, Any]:
        return self._indexes_by_table if table is None else self._indexes_by_table.get(table, {})

    def find_index_by_column(self, table: str, column: str) -> Optional[Dict[str, Any]]:
        for nm, meta in self._indexes_by_table.get(table, {}).items():
            if meta.get("column") == column:
                m = dict(meta); m["name"] = nm
                return m
        return None
