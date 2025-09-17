# mini_db

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)](https://github.com/yourusername/projectname/actions)
[![Version](https://img.shields.io/badge/version-v1.0.0-orange.svg)](https://github.com/yourusername/projectname/releases)

> 一个用python编写的轻量级的关系型数据库系统

## 📖 项目简介

mini_db是一个从零开始构建的简单的关系型数据库管理系统（RDBMS），作为本科生实训项目。本项目实现了完整的数据库核心组件，包括：

- **SQL编译器**: 支持SQL语句解析、语义分析、为执行引擎提供执行计划。
- **页式存储系统**: 支持meta页+数据页，能分配、释放、读写页，支持两种缓存替换策略。
- **查询执行引擎**: 利用多种算子运行执行计划，实现了新建B+树索引的功能。

## ✨ 特性

- 🔍 **基本的SQL支持**: 支持SELECT, INSERT, UPDATE, DELETE等基本SQL操作
- 🔧 **额外的SQL支持**: 支持group by,count,min,max，多表联查等操作
- 🗃️ **索引优化**: B+树索引，快速数据检索
- 💾 **持久化存储**: 基于页的存储管理，数据安全可靠


## 🏗️ 系统架构

```
┌─────────────────┐
│   SQL Parser    │  ← SQL语句解析和优化
├─────────────────┤
│ Query Executor  │  ← 查询执行引擎
├─────────────────┤
│ Storage Engine  │  ← 页式存储管理
├─────────────────┤
│  Buffer Pool    │  ← 内存缓冲池
├─────────────────┤
│   File System   │  ← 底层文件存储
└─────────────────┘
```

## 🚀 快速开始

### 环境要求

- Python 3.8+（推荐 3.10+）
- 操作系统: Windows
- 终端/控制台支持中文
- （可选）导出 Excel 需安装：pip install openpyxl


### 编译安装

```bash
# 克隆项目
git clone https://github.com/kimdksfjk/mini_db.git
cd mini_db


### 基本使用

```bash
# 启动数据库
python -m engine.cli.mysql_cli --data 文件夹名
python -m engine.cli.mysql_cli --data 文件夹名 --debug （可选可查看堆栈信息）
python -m engine.cli.mysql_cli --data data_test < tests\demo_error.sql（直接运行文件不支持注释）
```

```sql
-- 创建表
CREATE TABLE student(id INT, name VARCHAR, age INT, grade VARCHAR);

-- 插入数据
INSERT INTO student (id,name,age,grade) VALUES 
(1,'Alice',20,'A'), 
(3,'Bob',19,'B'), 
(3,'Carol',21,'A'), 
(1,'Dave',22,'B'), 
(1,'Eve',23,'C'), 
(3,'Frank',20,'A'), 
(1,'Grace',21,'B');

-- 查询数据
SELECT id,name,age,grade FROM student WHERE id=3;
```

## 📚 API文档

### SQL支持

| 功能 | 状态 | 说明 |
|------|------|------|
| CREATE TABLE | ✅ | 支持基本数据类型 |
| DROP TABLE | ✅ | 删除表和索引 |
| INSERT | ✅ | 单行和批量插入 |
| SELECT | ✅ | 支持WHERE, ORDER BY, GROUP BY |
| UPDATE | ✅ | 条件更新 |
| DELETE | ✅ | 条件删除 |
| INDEX | ✅ | B+树索引 |


### 数据类型

- `INT`: 32位整数
- `BIGINT`: 64位整数
- `VARCHAR(n)`: 变长字符串
- `CHAR(n)`: 定长字符串
- `FLOAT`: 单精度浮点数
- `DOUBLE`: 双精度浮点数

## 🛠️ 开发指南

### 代码风格

- 遵循python代码规范

### 贡献流程

1. Fork本项目
2. 创建feature分支
3. 编写代码和测试
4. 确保所有测试通过
5. 提交Pull Request

## 🐛 问题报告

如果发现bug或有功能建议，请[提交issue](https://github.com/kimdksfjk/mini_db.git/issues)。

在报告问题时，请提供：
- 操作系统和版本
- 数据库版本
- 重现步骤
- 错误日志

## 📋 开发计划

- [ ] 支持更多SQL函数
- [ ] 实现分布式架构
- [ ] 添加Web管理界面
- [ ] 欢迎提出建议

## 🤝 贡献者

感谢所有为这个项目做出贡献的开发者：

- [@kimdksfjk](https://github.com/kimdksfjk) - 执行引擎开发
- [@kHan6881](https://github.com/kHan6881) - 存储引擎开发
- [@lrckkk](https://github.com/lrckkk) - SQL编译器开发

## 📄 开源协议

本项目基于[MIT License](LICENSE)开源协议。

## 📞 联系方式

- 邮箱: 1378264698@qq.com

---

如果这个项目对你有帮助，请给个⭐️支持一下！