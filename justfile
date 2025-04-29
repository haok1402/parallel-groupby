# - engine choices are duckdb, polars for now
# - dbfile should point to a duckdb database file
bench engine="duckdb" max_core="8" dbfile="data/exponential/1M-1K.csv.gz":
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
    mkdir -p lib/tsl/include
    wget -O lib/tsl/include/robin_growth_policy.hpp https://raw.githubusercontent.com/Tessil/robin-map/refs/heads/master/include/tsl/robin_growth_policy.h
    wget -O lib/tsl/include/robin_hash.hpp https://raw.githubusercontent.com/Tessil/robin-map/refs/heads/master/include/tsl/robin_hash.h
    wget -O lib/tsl/include/robin_map.hpp https://raw.githubusercontent.com/Tessil/robin-map/refs/heads/master/include/tsl/robin_map.h
    wget -O lib/tsl/include/robin_set.hpp https://raw.githubusercontent.com/Tessil/robin-map/refs/heads/master/include/tsl/robin_set.h
    mkdir -p lib/skarupke/include
    wget -O lib/skarupke/include/flat_hash_map.hpp https://raw.githubusercontent.com/skarupke/flat_hash_map/refs/heads/master/flat_hash_map.hpp
    mkdir -p lib/Cyan4973/include
    wget -O lib/Cyan4973/include/xxhash.h https://raw.githubusercontent.com/Cyan4973/xxHash/refs/heads/dev/xxhash.h
    wget -O lib/Cyan4973/include/xxhash.c https://raw.githubusercontent.com/Cyan4973/xxHash/refs/heads/dev/xxhash.c
    
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

build-cpp-quick:
    # cmake -DCMAKE_OSX_SYSROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX15.4.sdk .
    cmake .
    make

build-cpp:
    # cmake -DCMAKE_OSX_SYSROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX15.4.sdk .
    cmake -DCMAKE_BUILD_TYPE=Release .
    make -j $(nproc)

build-cpp-release:
    cmake -DCMAKE_BUILD_TYPE=Release .
    make -j $(nproc)

run-cpp: build-cpp
    # ./main --num_threads 1
    # ./main --num_threads 8 --strategy SIMPLE_TWO_PHASE_RADIX
    # ./main --num_threads 4 --algorithm single-thread --dataset_file_path data/exponential/100K-1K.csv.gz --num_dryruns 1 --num_trials 1
    ./main --num_threads 1 --algorithm two-phase-central-merge --dataset_file_path data/exponential/20-10.csv.gz --num_dryruns 1 --num_trials 1 --validation_file_path data/exponential/val-20-10.csv

example-bench-cmds:
    python benchmark/bench.py -np 2 -i data/exponential/1M-1K.csv.gz -e duckdb -q --num_dryruns 3 --num_trials 5
    ./main --num_threads 2 --algorithm two-phase-central-merge --dataset_file_path data/exponential/1M-1K.csv.gz --num_dryruns 1 --num_trials 1 --validation_file_path data/exponential/val-1M-1K.csv

bench-cpp dist="uniform" alg="lock-free-hash-table" max_core="8": build-cpp
    #!/bin/bash
    max_core={{max_core}}
    alg={{alg}}
    echo -e "================================"
    echo -e "algorithm: $alg"
    echo -e "distribution: $dist"
    echo -e "max_core: $max_core"
    echo -e "================================"
    config_args="--algorithm {{alg}} --dataset_file_path data/{{dist}}/8M-2M.csv.gz --num_dryruns 0 --num_trials 1 --validation_file_path data/{{dist}}/val-8M-2M.csv"
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

generate-all: 
    just generate-dist normal
    just generate-dist exponential
    just generate-dist biuniform
    just generate-dist uniform

generate-dist distribution="normal": 
    # just generate {{distribution}} 8M 2K
    # just generate {{distribution}} 8M 20K
    # just generate {{distribution}} 8M 200K
    # just generate {{distribution}} 8M 2M
    
    # just generate {{distribution}} 80M 20K
    # just generate {{distribution}} 80M 200K
    just generate {{distribution}} 80M 2M
    # just generate {{distribution}} 80M 20M
    
    # just generate uniform 800M 200K
    # just generate uniform 800M 2M
    # just generate uniform 800M 20M
    # just generate uniform 800M 200M
    
run-experiment exp_id machine_id max_np:
    echo -e "================================"
    echo -e "exp_id: {{exp_id}}"
    echo -e "machine_id: {{machine_id}}"
    echo -e "max_np: {{max_np}}"
    echo -e "================================"
    bash benchmark/experiment.sh {{exp_id}} {{machine_id}} {{max_np}}
    python benchmark/extract.py -eid {{exp_id}}
    python analysis/exp-plot-all.py -eid {{exp_id}}
    
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


submit:
    tar -czvf submission.tar.gz src/**/*.cpp src/*.cpp src/**/*.hpp src/*.hpp CMakeLists.txt README.md src-go/*.go src-rs/*.rs src-rs/*.toml src-rs/*.lock src-rs/src/*.rs src-go/go.mod