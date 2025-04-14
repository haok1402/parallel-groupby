
# - engine choices are duckdb, polars for now
# - dbfile should point to a duckdb database file
bench engine="duckdb" dbfile="data/tpch-sf1.db":
    #!/bin/bash
    dbfile={{dbfile}}
    engine={{engine}}
    for np in 1 2 4 8; do
        python benchmark/bench.py -np $np -i $dbfile -q -e $engine
    done

download-tpch:
    wget -P data/ https://blobs.duckdb.org/data/tpch-sf1.db
    wget -P data/ https://blobs.duckdb.org/data/tpch-sf10.db
    # wget -P data/ https://blobs.duckdb.org/data/tpch-sf100.db
