# take an output parquet file created by extract.py and make many plots

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
print(results_df)

# 2 > overall latency plot

plot_df = results_df.sql(f"""--sql
    select algorithm, np, size_config, dist, machine_id, avg(time) as latency from self
    where true
        and attribute = 'elapsed_time'
    group by algorithm, np, size_config, dist, machine_id
""")

g = sns.FacetGrid(plot_df, col="size_config", row='dist', height=3, aspect=1.5)
g.map_dataframe(sns.lineplot,     
    x='np',
    y='latency',
    hue='algorithm',
    style='machine_id',
)
g.set(xlim=(1, None), ylim=(0, None))
g.add_legend()
g.figure.suptitle("Elapsed Time vs Num Threads", y=1.03)
g.figure.savefig(f"results/{exp_id}-latency.pdf")
logger.info(f"saved results/{exp_id}-latency.pdf")