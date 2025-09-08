
"""
Parser: build AST for a SQL subset (CREATE TABLE, INSERT, SELECT, DELETE).
On error: report message + position + expected symbols.
"""
from typing import Any, Dict
class Parser:
    def parse(self, tokens) -> Dict[str, Any]:
        # TODO: Implement recursive descent or Pratt parser for subset
        raise NotImplementedError("parser.parse not implemented yet")
