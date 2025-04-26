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

download-cpp-libs:
    mkdir -p lib/CLI11/include/
    wget -O lib/CLI11/include/CLI11.hpp https://github.com/CLIUtils/CLI11/releases/download/v2.5.0/CLI11.hpp
    mkdir -p lib/indicators/include
    wget -O lib/indicators/include/indicators.hpp https://raw.githubusercontent.com/p-ranav/indicators/refs/heads/master/single_include/indicators/indicators.hpp

clean:
    make clean
    rm CMakeCache.txt
    rm -rf CMakeFiles
    
linux-setup-duckdb:
    wget https://github.com/duckdb/duckdb/releases/download/v1.2.2/libduckdb-linux-amd64.zip
    unzip libduckdb-linux-amd64.zip && rm libduckdb-linux-amd64.zip

    mkdir -p lib/duckdb/include
    mv duckdb.h duckdb.hpp lib/duckdb/include/

    mkdir -p lib/duckdb/lib
    mv libduckdb.so libduckdb_static.a lib/duckdb/lib/

build-cpp:
    # cmake -DCMAKE_OSX_SYSROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX15.4.sdk .
    cmake .
    make

build-cpp-release:
    cmake -DCMAKE_OSX_SYSROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX15.4.sdk -DCMAKE_BUILD_TYPE=Release .
    make

run-cpp: build-cpp
    # ./main --num_threads 1
    # ./main --num_threads 8 --strategy SIMPLE_TWO_PHASE_RADIX
    # ./main --num_threads 4 --algorithm single-thread --dataset_file_path data/exponential/100K-1K.csv.gz --num_dryruns 1 --num_trials 1
    ./main --num_threads 1 --algorithm two-phase-central-merge --dataset_file_path data/exponential/20-10.csv.gz --num_dryruns 1 --num_trials 1 --validation_file_path data/exponential/val-20-10.csv

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

generate dist="exponential" nrows="100K" ngroups="1K": build-cpp
    ./generate --distribution {{dist}} --num-rows {{nrows}} --num-groups {{ngroups}}
    # create validation data
    duckdb -c "COPY (select * from (select key, count(val) as 'count', sum(val) as 'sum', min(val) as 'min', max(val) as 'max' from 'data/{{dist}}/{{nrows}}-{{ngroups}}.csv.gz' group by key order by key) using sample 100 rows (reservoir, 42)) to 'data/{{dist}}/val-{{nrows}}-{{ngroups}}.csv'"

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
