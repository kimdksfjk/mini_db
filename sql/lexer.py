
"""
Lexer: recognize tokens and output [type_code, lexeme, line, column].
NOTE: This is a stub. Fill in token rules for keywords, identifiers, literals, operators, separators.
"""
from dataclasses import dataclass
from typing import Iterator, Tuple

KEYWORDS = {"CREATE","TABLE","INSERT","INTO","VALUES","SELECT","FROM","WHERE","DELETE"}

@dataclass
class Token:
    type_code: str   # e.g., KW_CREATE, ID, INT, STR, OP_EQ, COMMA, SEMI
    lexeme: str
    line: int
    col: int

class Lexer:
    def __init__(self, src: str):
        self.src = src

    def tokens(self) -> Iterator[Token]:
        # TODO: Implement proper lexing
        # yield Token("EOF", "", 0, 0)
        raise NotImplementedError("lexer.tokens not implemented yet")
