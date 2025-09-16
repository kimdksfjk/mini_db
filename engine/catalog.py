# engine/catalog.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, List, Optional
import os

from .storage_adapter import StorageAdapter
from .sys_catalog import SysCatalog


class Catalog:
    """
    目录管理器（高层接口）

    职责：
      - 对接系统表（SysCatalog），提供统一的表级元数据访问入口；
      - 在需要时通过 StorageAdapter 协助创建底层存储（.mdb）；
      - 向上层隐藏系统表与存储细节，仅暴露 get/create/list/has 等常用操作。
    """

    def __init__(self, data_dir: str):
        """
        初始化目录管理器。

        参数：
            data_dir: 数据根目录路径（绝对或相对），用于存放系统表与各业务表的数据。
        """
        self.data_dir = os.path.abspath(data_dir)
        self._storage = StorageAdapter(self.data_dir)
        self._sys = SysCatalog(self.data_dir, self._storage)

    def get_table(self, name: str) -> Dict[str, Any]:
        """
        获取指定表的元信息。

        参数：
            name: 表名
        返回：
            一个包含表定义与存储描述的字典（由 SysCatalog 定义结构）
        异常：
            KeyError: 当表不存在时抛出
        """
        return self._sys.get_table(name)

    def create_table(
        self,
        name: str,
        columns: List[Dict[str, Any]],
        storage_desc: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        创建表并注册到系统表，兼容可选的存储描述。

        说明：
            - 若传入 storage_desc，则使用外部已创建的 .mdb 存储；
            - 若未传入，则由系统表逻辑负责创建并返回存储描述。

        参数：
            name: 表名
            columns: 列定义列表（每项至少包含 name / type 等字段）
            storage_desc: 可选的底层存储描述（kind/path 等）
        返回：
            创建完成后的表元信息字典
        """
        return self._sys.create_table_and_register(name, columns, storage_desc)

    def list_tables(self) -> List[str]:
        """
        列出所有用户表名称。

        返回：
            表名列表（不含系统表）
        """
        return self._sys.list_tables()

    def has_table(self, name: str) -> bool:
        """
        判断指定表是否存在。

        参数：
            name: 表名
        返回：
            True 存在；False 不存在
        """
        try:
            self._sys.get_table(name)
            return True
        except KeyError:
            return False
