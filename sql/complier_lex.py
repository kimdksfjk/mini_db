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
    #关键字
    KEYWORD = "KEYWORD"
    #表名，列名等标识符
    IDENTIFIER = "IDENTIFIER"
    #常量
    CONSTANT = "CONSTANT"
    #运算符
    OPERATOR = "OPERATOR"
    #分隔，比如，()
    DELIMITER = "DELIMITER"
    #结束标记
    EOF = "EOF"

#定义token数据结构
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
        #优先级匹配模式
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
        #编译正则表达式，优化代码
        self.compiled_patterns = [(name, re.compile(pattern))
                                  for name, pattern in self.patterns]

    def tokenize(self, sql_text: str) -> List[Token]:
        """将sql文本转化为token列表"""
        tokens: List[Token] = []
        lines = sql_text.split('\n')

        for line_num, line in enumerate(lines, 1):
            column = 1
            pos = 0
            while pos < len(line):
                matched = False
                for pattern_name, pattern in self.compiled_patterns:
                    #进行匹配
                    match = pattern.match(line, pos)
                    if match:
                        if pattern_name == 'WHITESPACE':
                            pos = match.end()
                            column += match.end() - match.start()
                            matched = True
                            break
                        elif pattern_name == 'STRING':
                            #获取单引号双引号后的内容
                            value = match.group(1) or match.group(2)
                            tokens.append(Token(TokenType.CONSTANT, value, line_num, column))
                        elif pattern_name == 'NUMBER':
                            value = match.group(0)
                            tokens.append(Token(TokenType.CONSTANT, value, line_num, column))
                        elif pattern_name == 'IDENTIFIER':
                            #大写进行关键词判断
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


