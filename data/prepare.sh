#!/bin/bash

for scale in 1 10 100; do
    wget -P data/ "https://blobs.duckdb.org/data/tpch-sf${scale}.db"
done
