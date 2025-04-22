#!/bin/bash

set -e

if [[ "$(uname)" != "Linux" ]]; then
    echo "Error: This script must be run on Linux." >&2
    exit 1
fi

wget https://github.com/duckdb/duckdb/releases/download/v1.2.2/libduckdb-linux-amd64.zip
unzip libduckdb-linux-amd64.zip && rm libduckdb-linux-amd64.zip

mkdir -p lib/duckdb/include
mv duckdb.h duckdb.hpp lib/duckdb/include/

mkdir -p lib/duckdb/lib
mv libduckdb.so libduckdb_static.a lib/duckdb/lib/

mkdir -p lib/CLI11/include/
wget -O lib/CLI11/include/CLI11.hpp https://github.com/CLIUtils/CLI11/releases/download/v2.5.0/CLI11.hpp

mkdir -p lib/indicators/include
wget -O lib/indicators/include/indicators.hpp https://raw.githubusercontent.com/p-ranav/indicators/refs/heads/master/single_include/indicators/indicators.hpp

python3 -m venv venv
venv/bin/pip install -r requirements.txt
