#! unified script for benchmarking different engines and approaches

import os
import polars as pl
from loguru import logger
import argparse
from rich import inspect as rinspect
import subprocess
import time
import duckdb

def mk_bench_qry_line_str(args, table_name: str):
    # return f"SELECT l_orderkey, COUNT(*), SUM(l_extendedprice), AVG(l_discount) FROM {table_name} GROUP BY l_orderkey"
    # return f"SELECT l_orderkey, COUNT(*), median(l_extendedprice), median(l_discount) FROM {table_name} GROUP BY l_orderkey"
    return f"SELECT l_orderkey, SUM(l_partkey), SUM(l_suppkey) FROM {table_name} GROUP BY l_orderkey"

def main():
    # parse arguments
    
    parser = argparse.ArgumentParser(description='Experiment')
    
    parser.add_argument('-np', '--num_threads', type=int, required=True)
    parser.add_argument('-i', '--in_db', type=str, required=True) # a duckdb .db file, not parquet
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
    logger.info(f"pl threadpool size = {pl.thread_pool_size()}")
    
    # 1 > load in the data
    con = duckdb.connect(args.in_db, read_only = True)
    df = con.sql("select l_orderkey, l_partkey, l_suppkey from lineitem").pl()
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
    
    t_start = time.time()
    for i in range(args.num_trials):
        df.sql(sql_str)
    t_end = time.time()
    
    # 5 > print out stats
    t_total = t_end - t_start
    
    logger.success("done with trials")
    print(f"{args.num_trials} trials average is {t_total/args.num_trials:.6f} seconds to run on {args.num_threads} threads")

def bench_duckdb(args):
    import duckdb as db
    
    # 1 > load in the data
    con = db.connect(args.in_db, config={'threads': args.num_threads})
    con.sql("CREATE TEMP TABLE in_table AS SELECT l_orderkey, l_partkey, l_suppkey FROM lineitem;")
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
    
    t_start = time.time()
    for i in range(args.num_trials):
        con.sql(sql_str).execute()
    t_end = time.time()
    
    # 5 > print out stats
    t_total = t_end - t_start
    
    logger.success("done with trials")
    print(f"{args.num_trials} trials average is {t_total/args.num_trials:.6f} seconds to run on {args.num_threads} threads")

if __name__ == "__main__":
    main()