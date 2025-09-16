# tests/gen_perf_data.py
# -*- coding: utf-8 -*-
import argparse, os, random, string

def gen_rows(n: int):
    grades = ["A", "B", "C", "D"]
    for i in range(1, n+1):
        name = "N" + str(i)
        age = 18 + (i % 10)
        grade = grades[i % 4]
        yield (i, name, age, grade)

def write_seed(path: str, rows: int, batch: int):
    with open(path, "w", encoding="utf-8") as f:
        f.write("CREATE TABLE bench(id INT, name VARCHAR, age INT, grade VARCHAR);\n")
        buf = []
        cnt = 0
        for i, name, age, grade in gen_rows(rows):
            buf.append(f"({i},'{name}',{age},'{grade}')")
            if len(buf) >= batch:
                f.write("INSERT INTO bench (id,name,age,grade) VALUES " + ",".join(buf) + ";\n")
                buf.clear()
        if buf:
            f.write("INSERT INTO bench (id,name,age,grade) VALUES " + ",".join(buf) + ";\n")

def write_baseline_queries(path: str, rows: int):
    # 选择靠后的 id 以放大无索引全表扫描代价
    hot = max(10, rows - 10)
    with open(path, "w", encoding="utf-8") as f:
        # 预热（不计时用，避免首次 I/O 偏差）：跑一条轻查询
        f.write("SELECT id FROM bench WHERE id >= 1 LIMIT 1;\n")
        # 1) 等值查询（无索引需全表扫）
        for _ in range(5):
            f.write(f"SELECT name FROM bench WHERE id = {hot};\n")
        # 2) 范围+LIMIT（模拟翻页）
        for _ in range(5):
            f.write(f"SELECT id,name FROM bench WHERE id >= {hot} LIMIT 100;\n")
        # 3) 范围聚合（COUNT）
        for _ in range(3):
            f.write(f"SELECT COUNT(*) AS c FROM bench WHERE id >= {rows//2};\n")
        # 4) 字段过滤（字符串键）
        for _ in range(3):
            f.write("SELECT COUNT(*) AS c FROM bench WHERE grade = 'A';\n")

def write_create_index(path: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write("\\create_index bench id idx_id\n")
        f.write("\\create_index bench grade idx_grade\n")

def write_with_index_queries(path: str, rows: int):
    hot = max(10, rows - 10)
    with open(path, "w", encoding="utf-8") as f:
        # 同一组查询，便于前后对比
        f.write("SELECT id FROM bench WHERE id >= 1 LIMIT 1;\n")
        for _ in range(5):
            f.write(f"SELECT name FROM bench WHERE id = {hot};\n")
        for _ in range(5):
            f.write(f"SELECT id,name FROM bench WHERE id >= {hot} LIMIT 100;\n")
        for _ in range(3):
            f.write(f"SELECT COUNT(*) AS c FROM bench WHERE id >= {rows//2};\n")
        for _ in range(3):
            f.write("SELECT COUNT(*) AS c FROM bench WHERE grade = 'A';\n")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=200000, help="生成的数据行数")
    ap.add_argument("--batch", type=int, default=1000, help="每条 INSERT 的多值条数")
    ap.add_argument("--outdir", default="tests", help="输出目录")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    seed = os.path.join(args.outdir, "perf_seed.sql")
    noidx = os.path.join(args.outdir, "perf_baseline.sql")
    mkidx = os.path.join(args.outdir, "perf_create_index.sql")
    withidx = os.path.join(args.outdir, "perf_with_index.sql")

    write_seed(seed, args.rows, args.batch)
    write_baseline_queries(noidx, args.rows)
    write_create_index(mkidx)
    write_with_index_queries(withidx, args.rows)

    print("生成完成：")
    print("  ", seed)
    print("  ", noidx)
    print("  ", mkidx)
    print("  ", withidx)
