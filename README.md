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
- **命令行强化**: 可视化SELECT，支持文件的多种格式的导出。

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

### 系统元命令
> 以分号 `;` 结尾回车执行 SQL

| 命令                  | 说明                      |
|---------------------|-------------------------|
| `\dt`               | 显示当前所有表                 |
| `\create_index 表 列` | 建立索引                    |
| `\list_indexes 表`   | 显示表的索引                  |
| `\drop_index 表 索引名` | 删除索引                    |
| `\popup`            | 弹窗显示最近一次查询结果            |
| `\export 路径`        | 导出最近一次查询结果（xlsx/缺库回退 csv） |
| `\bpstat`           | 打印缓冲池统计（命中率、读写、淘汰数等）    |
| `\bplog on或off`     | 开启或关闭替换日志               |
| `\q`                | 退出                      |

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

## 🐛 常见问题
- Q1：为什么第一次查询很慢，第二次很快？
- A：第一次需要从磁盘加载数据页与索引页（miss 多），而后命中缓冲池与内存 B+树（hit 多）。
- Q2：删除索引失败/重建很慢？
- A：使用 \drop_index 表 索引名 删除会清除系统表登记与内存树；不要手动删索引目录。若确实手动删除，请同时清理系统表条目（__sys_indexes），或直接删除整个 --data 目录后重建。
- Q3：Windows \export 报路径无效？
- A：请传入标准路径字符串，不要使用类似 ["C:\path"] 的数组写法。例：\export C:\Users\Me\Desktop\out.xlsx。
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

- 邮箱: 1379587654@qq.com

---

如果这个项目对你有帮助，请给个⭐️支持一下！