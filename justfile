
# - engine choices are duckdb, polars for now
# - dbfile should point to a duckdb database file
bench engine="duckdb" max_core="8" dbfile="data/tpch-sf1.db":
    #!/bin/bash
    dbfile={{dbfile}}
    engine={{engine}}
    max_core={{max_core}}
    echo "--- 🧪 benchmark $engine on $dbfile ---"
    for np in 1 2 4 8 16 32 64 128; do
        if [[ $np -gt $max_core ]]; then
            continue
        fi
        echo "> benchmarking with $np cores"
        python benchmark/bench.py -np $np -i $dbfile -q -e $engine
    done

download-tpch:
    wget -P data/ https://blobs.duckdb.org/data/tpch-sf1.db
    wget -P data/ https://blobs.duckdb.org/data/tpch-sf10.db
    # wget -P data/ https://blobs.duckdb.org/data/tpch-sf100.db

linux-setup-duckdb:
    wget https://github.com/duckdb/duckdb/releases/download/v1.2.2/libduckdb-linux-amd64.zip
    unzip libduckdb-linux-amd64.zip && rm libduckdb-linux-amd64.zip

    mkdir -p lib/duckdb/include
    mv duckdb.h duckdb.hpp lib/duckdb/include/

    mkdir -p lib/duckdb/lib
    mv libduckdb.so libduckdb_static.a lib/duckdb/lib/

build-cpp:
    cmake .
    make

run-cpp: build-cpp
    # ./main --num_threads 1
    ./main --num_threads 8 --strategy SIMPLE_TWO_PHASE_RADIX

tmp-run-cpp-bench strat="SIMPLE_THREE_PHASE_RADIX" max_core="8" cardinality_reduction="-1": build-cpp
    #!/bin/bash
    max_core={{max_core}}
    strat={{strat}}
    cardinality_reduction={{cardinality_reduction}}
    echo -e "================================"
    echo -e "strategy: $strat"
    echo -e "max_core: $max_core"
    echo -e "cardinality_reduction: $cardinality_reduction"
    echo -e "================================"
    config_args="--strategy {{strat}} --in_db_file_path data/tpch-sf1.db --cardinality_reduction $cardinality_reduction --num_dryruns 1 --num_trials 3"
    for np in 1 2 4 8 16 32 64 128; do
        if [[ $np -gt $max_core ]]; then
            continue
        fi
        echo -e "\n======== benchmarking with $np cores ========"
        ./main --num_threads $np $config_args | grep ">>"
    done

[working-directory: 'src-go']
@run-go:
    go run main.go

[working-directory: 'src-rs']
@run-rs:
    cargo run --release

setup-omp:
    brew install libomp
    # need to do
    # export LDFLAGS="-L/opt/homebrew/opt/libomp/lib"
    # export CPPFLAGS="-I/opt/homebrew/opt/libomp/include"
