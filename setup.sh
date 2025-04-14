#!/bin/bash

# Author: Hao Kang
# Date: April 14, 2025

# Prepare the development environment.
wget https://github.com/duckdb/duckdb/releases/download/v1.2.2/libduckdb-linux-amd64.zip
unzip libduckdb-linux-amd64.zip && rm libduckdb-linux-amd64.zip

mkdir -p lib/duckdb/include
mv duckdb.h duckdb.hpp lib/duckdb/include/

mkdir -p lib/duckdb/lib
mv libduckdb.so libduckdb_static.a lib/duckdb/lib/

# Download the dataset for benchmarking purposes.
wget -P data/ https://blobs.duckdb.org/data/tpch-sf1.db
wget -P data/ https://blobs.duckdb.org/data/tpch-sf10.db
wget -P data/ https://blobs.duckdb.org/data/tpch-sf100.db
