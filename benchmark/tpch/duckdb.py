"""
Benchmark DuckDB performance on TPC-H GroupBy queries with varying thread counts.

Author: Hao Kang
Date: April 8, 2025
"""

import os
import logging
import tempfile
import subprocess
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

template = """
INSTALL tpch;
LOAD tpch;

SET threads TO {};

PRAGMA enable_profiling;
SET enable_profiling = 'json';
SET profiling_output = '{}';

PRAGMA tpch(1);
"""

def main():
    for database in Path("data").glob("tpch-sf*.db"):
        i, n = 1, os.cpu_count()
        while i <= n:
            outfile = Path("report", database.stem, "duckdb", f"thread_{i}.json")
            outfile.parent.mkdir(parents=True, exist_ok=True)
            logging.info(f"Processing {database} under {i} threads")
            with tempfile.NamedTemporaryFile() as file:
                file.write(template.format(i, outfile).encode())
                file.flush()
                subprocess.run(
                    f"duckdb {database} < {file.name}", 
                    check=True, shell=True, stdout=subprocess.PIPE,
                )
            i *= 2

if __name__ == "__main__":
    main()
