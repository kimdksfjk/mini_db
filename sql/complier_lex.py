#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
词法与Token定义
"""

import re
from typing import List
from dataclasses import dataclass
from enum import Enum


class TokenType(Enum):
    KEYWORD = "KEYWORD"
    IDENTIFIER = "IDENTIFIER"
    CONSTANT = "CONSTANT"
    OPERATOR = "OPERATOR"
    DELIMITER = "DELIMITER"
    EOF = "EOF"


@dataclass
class Token:
    type: TokenType
    value: str
    line: int
    column: int


class LexicalAnalyzer:
    """词法分析器"""

    def __init__(self):
        self.keywords = {
            'SELECT', 'FROM', 'WHERE', 'CREATE', 'TABLE', 'INSERT', 'INTO',
            'VALUES', 'DELETE', 'UPDATE', 'SET', 'INT', 'VARCHAR', 'CHAR',
            'FLOAT', 'DOUBLE', 'AND', 'OR', 'NOT', 'NULL', 'PRIMARY', 'KEY',
            'UNIQUE', 'ORDER', 'BY', 'GROUP', 'HAVING', 'ASC', 'DESC',
            'INNER', 'LEFT', 'RIGHT', 'OUTER', 'JOIN', 'ON', 'AS', 'COUNT',
            'SUM', 'AVG', 'MIN', 'MAX', 'DISTINCT', 'LIMIT', 'OFFSET'
        }

        self.operators = {
            '=', '>', '<', '>=', '<=', '!=', '<>', '+', '-', '*', '/',
            'AND', 'OR', 'NOT'
        }

        self.delimiters = {
            '(', ')', ',', ';', "'", '"', '.'
        }

        self.patterns = [
            ('STRING', r"'([^']*)'|\"([^\"]*)\""),
            ('NUMBER', r'\d+(\.\d+)?'),
            ('IDENTIFIER', r'[a-zA-Z_][a-zA-Z0-9_]*'),
            ('OPERATOR', r'[=<>!]++|[+\-*/]'),
            ('DELIMITER', r'[(),;.]'),
            ('WHITESPACE', r'\s+'),
        ]

        # 注意：re不支持 possessive quantifier ++，保持与原实现一致改为普通量词
        self.patterns[3] = ('OPERATOR', r'[=<>!]+|[+\-*/]')

        self.compiled_patterns = [(name, re.compile(pattern))
                                  for name, pattern in self.patterns]

    def tokenize(self, sql_text: str) -> List[Token]:
        tokens: List[Token] = []
        lines = sql_text.split('\n')

        for line_num, line in enumerate(lines, 1):
            column = 1
            pos = 0
            while pos < len(line):
                matched = False
                for pattern_name, pattern in self.compiled_patterns:
                    match = pattern.match(line, pos)
                    if match:
                        if pattern_name == 'WHITESPACE':
                            pos = match.end()
                            column += match.end() - match.start()
                            matched = True
                            break
                        elif pattern_name == 'STRING':
                            value = match.group(1) or match.group(2)
                            tokens.append(Token(TokenType.CONSTANT, value, line_num, column))
                        elif pattern_name == 'NUMBER':
                            value = match.group(0)
                            tokens.append(Token(TokenType.CONSTANT, value, line_num, column))
                        elif pattern_name == 'IDENTIFIER':
                            value = match.group(0).upper()
                            if value in self.keywords:
                                tokens.append(Token(TokenType.KEYWORD, value, line_num, column))
                            else:
                                tokens.append(Token(TokenType.IDENTIFIER, match.group(0), line_num, column))
                        elif pattern_name == 'OPERATOR':
                            value = match.group(0)
                            tokens.append(Token(TokenType.OPERATOR, value, line_num, column))
                        elif pattern_name == 'DELIMITER':
                            value = match.group(0)
                            tokens.append(Token(TokenType.DELIMITER, value, line_num, column))
                        pos = match.end()
                        column += match.end() - match.start()
                        matched = True
                        break
                if not matched:
                    char = line[pos]
                    raise SyntaxError(f"第{line_num}行第{column}列：未识别的字符 '{char}'")

        tokens.append(Token(TokenType.EOF, "EOF", len(lines), 1))
        return tokens


