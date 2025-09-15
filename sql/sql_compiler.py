#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL 编译器薄封装：向后兼容导出
"""

from .complier_engine import SQLCompiler, main  # noqa: F401

__all__ = [
    'SQLCompiler',
    'main',
]

if __name__ == "__main__":
    main()

