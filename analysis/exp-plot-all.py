# take an output parquet file created by extract.py and make many plots

from polars.functions import eager
import seaborn as sns
from pathlib import Path
import os
from loguru import logger
import math
import jq
import json
import polars as pl
import matplotlib.pyplot as plt
import pandas as pd
sns.set_style("whitegrid")
from loguru import logger
import argparse
from rich import inspect as rinspect

parser = argparse.ArgumentParser(description='Extract results from logs')
parser.add_argument('-eid', '--experiment_id', type=str, required=True)
args = parser.parse_args()
rinspect(args,  title="args")

exp_id = args.experiment_id
result_filepath = f"results/{exp_id}.parquet"

ALG_ORD = ['two-phase-central-merge', 'two-phase-central-merge-xxhash', 'two-phase-radix', 'two-phase-radix-xxhash', 'three-phase-radix', 'duckdbish-two-phase', 'implicit-repartitioning', 'lock-free-hash-table', 'polars', 'duckdb']

# 1 > read data

logger.info(f"will read from {result_filepath}")

results_df = pl.read_parquet(result_filepath)
def value_mapper(s: str):
    if "ms" in s:
        return float(s.replace("ms", ""))
    else:
        assert False

results_df = results_df.with_columns(
    time=pl.col("value").map_elements(value_mapper, return_dtype=pl.Float64)
)


logger.info(f"read results from {result_filepath}")
with pl.Config(tbl_cols=999):
    print(results_df)

# 2 > take time averages
# columns become │ exp_id ┆ machine_id ┆ dist        ┆ size_config ┆ n_rows  ┆ n_groups ┆ algorithm               ┆ np  ┆ attribute    ┆ avg_time │

results_df = results_df.sql("""--sql
    select exp_id, machine_id, dist, size_config, n_rows, n_groups, algorithm, np, attribute, avg(time) as avg_time from self
    where true
      --and n_rows = 8000000
      and algorithm != 'sequential'
      --and dist = 'uniform'
      --and (dist = 'uniform' or dist = 'biuniform')
    group by exp_id, machine_id, dist, size_config, n_rows, n_groups, algorithm, np, attribute
    order by exp_id, machine_id, dist, n_rows, n_groups, algorithm, attribute, np
""")
with pl.Config(tbl_cols=999):
    print("results_df")
    print(results_df)

single_thread_df = results_df.sql("""--sql
    select * from self
    where np = 1
""")
with pl.Config(tbl_cols=999):
    print("single_thread_df")
    print(single_thread_df)


max_np = results_df['np'].max()
logger.info(f"max num threads is {max_np}, will use as y max for speedup plots?")

logger.info("start plotting")

# 3 > overall latency plot

plot_df = results_df.sql(f"""--sql
    select algorithm, np, size_config, dist, machine_id, avg_time as latency from self
    where attribute = 'elapsed_time'
""")
with pl.Config(tbl_cols=999):
    print(plot_df)

g = sns.FacetGrid(plot_df, col="size_config", row='dist', height=3, aspect=1.5)
g.map_dataframe(sns.lineplot,     
    hue='algorithm',  hue_order=ALG_ORD,
    x='np',
    y='latency',
    style='machine_id',
)
g.set(xlim=(1, None), ylim=(0, None), yscale="log")
g.add_legend()
g.figure.suptitle("Elapsed Time vs Num Threads", y=1.03)
g.figure.savefig(f"results/{exp_id}-latency.pdf")
logger.success(f"saved results/{exp_id}-latency.pdf")

# 4 > oob speedup plot

plot_df = pl.SQLContext(results_df=results_df, single_thread_df=single_thread_df).execute(f"""--sql
    select machine_id, dist, size_config, algorithm, np, single_thread_df.avg_time / results_df.avg_time as speedup 
    from results_df
    left join single_thread_df on 
        results_df.algorithm = single_thread_df.algorithm and 
        results_df.size_config = single_thread_df.size_config and 
        results_df.dist = single_thread_df.dist and 
        results_df.machine_id = single_thread_df.machine_id and
        results_df.attribute = single_thread_df.attribute
    where true
        and attribute = 'elapsed_time'
        and ((algorithm = 'duckdb' or algorithm = 'polars'))
""", eager=True)
with pl.Config(tbl_cols=999):
    print(plot_df)

if len(plot_df) > 0:
    g = sns.FacetGrid(plot_df, col="size_config", row='dist', height=3, aspect=1.5)
    g.map_dataframe(sns.lineplot,     
        hue='algorithm',  hue_order=ALG_ORD,
        x='np',
        y='speedup',
        style='machine_id',
    )
    g.set(xlim=(1, None), ylim=(0, max_np))
    g.add_legend()
    g.figure.suptitle("Speedup vs Num Threads", y=1.03)
    g.figure.savefig(f"results/{exp_id}-oob-speedup.pdf")
    logger.success(f"saved results/{exp_id}-oob-speedup.pdf")

# 5 > aggregation speedup plot

plot_df = pl.SQLContext(results_df=results_df, single_thread_df=single_thread_df).execute(f"""--sql
    select machine_id, dist, size_config, algorithm, np, single_thread_df.avg_time / results_df.avg_time as speedup 
    from results_df
    left join single_thread_df on 
        results_df.algorithm = single_thread_df.algorithm and 
        results_df.size_config = single_thread_df.size_config and 
        results_df.dist = single_thread_df.dist and 
        results_df.machine_id = single_thread_df.machine_id and
        results_df.attribute = single_thread_df.attribute
    where true
        and attribute = 'aggregation_time'
        and (not (algorithm = 'duckdb' or algorithm = 'polars'))
""", eager=True)

with pl.Config(tbl_cols=999):
    print(plot_df)

g = sns.FacetGrid(plot_df, col="size_config", row='dist', height=3, aspect=1.5)
g.map_dataframe(sns.lineplot,     
    hue='algorithm',  hue_order=ALG_ORD,
    x='np',
    y='speedup',
    style='machine_id',
)
g.set(xlim=(1, None), ylim=(0, max_np))
g.add_legend()
g.figure.suptitle("Aggregation Speedup vs Num Threads", y=1.03)
g.figure.savefig(f"results/{exp_id}-agg-speedup.pdf")
logger.success(f"saved results/{exp_id}-agg-speedup.pdf")

# 6 > per phase latency

plot_df = results_df.sql(f"""--sql
    select machine_id, dist, size_config, algorithm, np, attribute as phase, avg_time as latency from self
    where 
        (attribute = 'phase_0' or attribute = 'phase_1' or attribute = 'phase_2' or attribute = 'phase_3')
""")
with pl.Config(tbl_cols=999):
    print(plot_df)

g = sns.FacetGrid(plot_df, col="size_config", row='dist', height=3, aspect=1.5)
g.map_dataframe(sns.lineplot,     
    hue='algorithm',  hue_order=ALG_ORD,
    x='np',
    y='latency',
    size='machine_id',
    style='phase',
)
g.set(xlim=(1, None), ylim=(0, None), yscale="log")
g.add_legend()
g.figure.suptitle("Elapsed Time vs Num Threads", y=1.03)
g.figure.savefig(f"results/{exp_id}-phase-latency.pdf")
logger.success(f"saved results/{exp_id}-phase-latency.pdf")
