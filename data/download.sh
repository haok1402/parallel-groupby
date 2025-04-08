#!/bin/bash
# Download TPC-H datasets (SF1/10/100) from DuckDB-hosted links.

# Author: Hao Kang
# Date: April 8, 2025

for scale in 1 10 100; do
    wget -P data/ "https://blobs.duckdb.org/data/tpch-sf${scale}.db"
done
