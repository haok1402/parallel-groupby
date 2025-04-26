#! unified script for benchmarking different engines and approaches

import os
from loguru import logger
import argparse
from rich import inspect as rinspect
import subprocess
import time
import duckdb

def mk_bench_qry_line_str(args, table_name: str):
    return f"SELECT key, count(val) as 'count', sum(val) as 'sum', min(val) as 'min', max(val) as 'max' from {table_name} GROUP BY key"

def main():
    # parse arguments
    
    parser = argparse.ArgumentParser(description='Experiment')
    
    parser.add_argument('-np', '--num_threads', type=int, required=True)
    parser.add_argument('-i', '--dataset_file_path', type=str, required=True) # a csv or csv.gz file
    parser.add_argument('-d', '--in_dist', type=str, default="uniform")
    parser.add_argument('-nd', '--num_dryruns', type=int, default=3)
    parser.add_argument('-nt', '--num_trials', type=int, default=5)
    parser.add_argument('-e', '--engine', type=str, default='polars', choices=['polars', 'duckdb'])
    parser.add_argument('-q', '--quiet', action='store_true')
    
    args = parser.parse_args()
    if args.quiet:
        logger.remove()
    else:
        rinspect(args,  title="args")
    
    match args.engine:
        case 'polars':
            bench_polars(args)
        case 'duckdb':
            bench_duckdb(args)
    
def bench_polars(args):
    os.environ["POLARS_MAX_THREADS"] = str(args.num_threads)
    import polars as pl
    logger.info(f"pl threadpool size = {pl.thread_pool_size()}")
    
    # 1 > load in the data
    con = duckdb.connect(':memory:', config={'threads': args.num_threads})
    df = con.sql(f"SELECT * from '{args.dataset_file_path}'").pl()
    logger.info(f"df shape = {df.shape}")
    
    # 2 > make sql query
    
    table_name = "self"
    sql_str = mk_bench_qry_line_str(args, table_name)
    logger.info(f"test query: {sql_str}")
    
    # 3 > do dry runs
    
    logger.info(f"doing {args.num_dryruns} dry runs")
    for i in range(args.num_dryruns):
        df.sql(sql_str)
    logger.success("done with dry runs")
    
    # 4 > do actual runs
    
    logger.info(f"timing {args.num_trials} trial runs")
    
    for i in range(args.num_trials):
        t_start = time.time()
        df.sql(sql_str)
        t_end = time.time()
        t_elapsed = t_end - t_start
        print(f">>> run={i}, elapsed_time={t_elapsed*1000:.2f}ms")
    
    logger.success("done with trials")

def bench_duckdb(args):    
    # 1 > load in the data
    con = duckdb.connect(':memory:', config={'threads': args.num_threads})
    con.sql(f"CREATE TEMP TABLE in_table AS SELECT * from '{args.dataset_file_path}';")
    # con.sql("SET threads TO 4;")
    logger.info("loaded data into memory")
    
    # 2 > make sql query
    table_name = "in_table"
    sql_str = mk_bench_qry_line_str(args, table_name)
    logger.info(f"test query: {sql_str}")
    
    # 3 > do dry runs
    logger.info(f"doing {args.num_dryruns} dry runs")
    for i in range(args.num_dryruns):
        con.sql(sql_str).execute()
    logger.success("done with dry runs")
    
    # 4 > do actual runs
    logger.info(f"timing {args.num_trials} trial runs")
    
    for i in range(args.num_trials):
        t_start = time.time()
        con.sql(sql_str).execute()
        t_end = time.time()
        t_elapsed = t_end - t_start
        print(f">>> run={i}, elapsed_time={t_elapsed*1000:.2f}ms")
        
    logger.success("done with trials")

if __name__ == "__main__":
    main()