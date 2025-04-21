import time
import logging
import argparse
import duckdb

QUERY = "SELECT key, SUM(val) FROM test GROUP BY key"
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--read-from", type=str, required=True)
    parser.add_argument("--num-threads", type=int, required=True)
    parser.add_argument("--warmup-steps", type=int, default=3)
    parser.add_argument("--measure-steps", type=int, default=5)
    parsed = parser.parse_args()

    con = duckdb.connect(config={"threads": parsed.num_threads})
    logging.info(f"Connected to DuckDB using {parsed.num_threads} thread(s)")

    logging.info(f"Loading table from '{parsed.read_from}' into memory")
    con.sql(f"CREATE TEMP TABLE test AS SELECT key, val FROM '{parsed.read_from}';")

    logging.info(f"Running {parsed.warmup_steps} warm-up iteration(s) to stabilize performance")
    for _ in range(parsed.warmup_steps):
        con.sql(QUERY).execute()

    logging.info(f"Running {parsed.measure_steps} evaluation iteration(s) for benchmarking")
    t0 = time.time()
    for _ in range(parsed.measure_steps):
        con.sql(QUERY).execute()
    t1 = time.time()
    
    elapsed = (t1 - t0) / parsed.measure_steps
    logging.info(f"Benchmark completed: Elapsed {elapsed:.8f}s")

if __name__ == "__main__":
    main()
