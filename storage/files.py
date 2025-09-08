
"""
Table file management utilities: map table name to file path/id,
handle create/open, ensure directory structure under data/tables/.
"""
import os
BASE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "tables")

def file_id_for_table(table: str) -> str:
    return os.path.join(BASE, f"{table}.heap")
