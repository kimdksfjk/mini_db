
"""
Semantic analyzer: existence checks, type consistency, column count/order checks.
Maintains Catalog (in-memory); the persistence is handled by engine.catalog.
Output: success or [error_type, position, reason].
"""
from typing import Any, Dict
class SemanticAnalyzer:
    def analyze(self, ast: Dict[str, Any]) -> Dict[str, Any]:
        # TODO: Check table/column existence, types, arity, etc.
        # Return normalized AST and/or update a Catalog view
        raise NotImplementedError("semantic.analyze not implemented yet")
