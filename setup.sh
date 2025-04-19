#!/bin/bash

# Prepare the development environment.
wget https://github.com/duckdb/duckdb/releases/download/v1.2.2/libduckdb-linux-amd64.zip
unzip libduckdb-linux-amd64.zip && rm libduckdb-linux-amd64.zip

mkdir -p lib/duckdb/include
mv duckdb.h duckdb.hpp lib/duckdb/include/

mkdir -p lib/duckdb/lib
mv libduckdb.so libduckdb_static.a lib/duckdb/lib/

mkdir -p lib/CLI11/include/
wget -O lib/CLI11/include/CLI11.hpp https://github.com/CLIUtils/CLI11/releases/download/v2.5.0/CLI11.hpp

python3 -m venv venv
venv/bin/pip install -r requirements.txt

# Download the dataset for benchmarking purposes.
wget -P data/ https://blobs.duckdb.org/data/tpch-sf1.db
wget -P data/ https://blobs.duckdb.org/data/tpch-sf10.db
wget -P data/ https://blobs.duckdb.org/data/tpch-sf100.db
