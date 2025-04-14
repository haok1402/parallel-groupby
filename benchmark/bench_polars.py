import os
from loguru import logger
import argparse
from rich import inspect as rinspect
import subprocess
import time

# parse arguments

parser = argparse.ArgumentParser(description='Experiment')

parser.add_argument('-np', '--num_threads', type=int, required=True)
parser.add_argument('-i', '--in_parquet', type=str, required=True)
parser.add_argument('-d', '--in_dist', type=str, default="uniform")
parser.add_argument('-nd', '--num_dryruns', type=int, default=3)
parser.add_argument('-nt', '--num_trials', type=int, default=10)

args = parser.parse_args()
logger.info(f"args = {args}")

# import polars and set n threads

os.environ["POLARS_MAX_THREADS"] = str(args.num_threads)
import polars as pl

logger.info(f"pl threadpool size = {pl.thread_pool_size()}")

# load into memory

df = pl.read_parquet(args.in_parquet)
logger.info(f"df shape = {df.shape}")

# run

sql_str = f"""--sql
select {args.in_dist}, sum(value) as sum_value from self group by {args.in_dist} order by {args.in_dist}
"""

logger.info(f"doing {args.num_dryruns} dry runs")
for i in range(args.num_dryruns):
    df.sql(sql_str)
logger.success("done with dry runs")

logger.info(f"timing {args.num_trials} trial runs")

t_start = time.time()
for i in range(args.num_dryruns):
    df.sql(sql_str)
t_end = time.time()

logger.success("done with trials")
print(f"run took {t_end - t_start:.4f} seconds to run {args.num_trials} trials on {args.num_threads} threads")