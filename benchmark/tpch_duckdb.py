import os
import tempfile
import logging
import subprocess
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

template = """
SET threads TO {};
PRAGMA enable_profiling;
SET enable_profiling = 'json';
SET profiling_output = '{}.json';

SELECT l_orderkey, COUNT(*), SUM(l_extendedprice), AVG(l_discount) FROM lineitem GROUP BY l_orderkey;
SELECT l_orderkey, COUNT(*), SUM(l_extendedprice), AVG(l_discount) FROM lineitem GROUP BY l_orderkey;
SELECT l_orderkey, COUNT(*), SUM(l_extendedprice), AVG(l_discount) FROM lineitem GROUP BY l_orderkey;
SELECT l_orderkey, COUNT(*), SUM(l_extendedprice), AVG(l_discount) FROM lineitem GROUP BY l_orderkey;
SELECT l_orderkey, COUNT(*), SUM(l_extendedprice), AVG(l_discount) FROM lineitem GROUP BY l_orderkey;
"""

def main():
    for database in Path("data").glob("tpch-sf*.db"):
        for threads in range(1, os.cpu_count() + 1):
            outfile = Path("report", database.stem, "duckdb", str(threads))
            outfile.parent.mkdir(parents=True, exist_ok=True)
            logging.info(f"Running query on {database} with {threads} threads.")
            with tempfile.NamedTemporaryFile() as file:
                file.write(template.format(threads, outfile).encode()); file.flush()
                kwargs = dict(check=True, shell=True, stdout=subprocess.DEVNULL)
                subprocess.run(f"duckdb {database} < {file.name}", **kwargs)

if __name__ == "__main__":
    main()
