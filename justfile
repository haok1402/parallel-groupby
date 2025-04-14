download-tpch:
    bash data/download.sh

bench-duckdb-tpch:
    python benchmark/tpch/duckdb.py

# - engine choices are duckdb, polars for now
# - dbfile should point to a duckdb database file
bench engine="duckdb" dbfile="data/tpch-sf1.db":
    #!/bin/bash
    dbfile={{dbfile}}
    engine={{engine}}
    for np in 1 2 4 8; do
        python benchmark/bench.py -np $np -i $dbfile -q -e $engine
    done