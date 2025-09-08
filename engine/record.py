
"""Tuple encode/decode for INT and STRING (u32 length + bytes)."""
import struct
from typing import List, Any
from .schema import Schema

def encode_tuple(schema: Schema, values: List[Any]) -> bytes:
    if len(schema.columns) != len(values):
        raise ValueError("Arity mismatch")
    out = bytearray()
    for col, v in zip(schema.columns, values):
        if col.type == "INT":
            out += struct.pack("<i", int(v))
        elif col.type == "STRING":
            b = str(v).encode("utf-8")
            out += struct.pack("<I", len(b)) + b
        else:
            raise TypeError(f"Unsupported type: {col.type}")
    return bytes(out)
