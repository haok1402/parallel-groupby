# Parallel GroupBy

Run the following in this directory to install c++ libraries:

```sh
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
```

To build executables, run:

```sh
cmake -DCMAKE_BUILD_TYPE=Release .
make -j $(nproc)
```

First, we will need to generate input data. Run `.generate -h` to find out how to generate data. A command to generate data looks like:

```sh
./generate --distribution {{dist}} --num-rows {{nrows}} --num-groups {{ngroups}}
```

To generate a validation file (i.e. expected results for a subset of the input), run:

```sh
duckdb -c "COPY (select * from (select key, count(val) as 'count', sum(val) as 'sum', min(val) as 'min', max(val) as 'max' from 'data/{{dist}}/{{nrows}}-{{ngroups}}.csv.gz' group by key order by key) using sample 100 rows (reservoir, 42)) to 'data/{{dist}}/val-{{nrows}}-{{ngroups}}.csv'"
```

To run parallel aggregation, run `./main`. `./main -h` should provide some help on the possible parameters. Usually, we run an experiment using:

```sh
./main --num_dryruns {{num_dryruns}} --num_trials {{num_trials}} --dataset_file_path data/{{dist}}/{{size_config}}.csv.gz  --validation_file_path data/{{dist}}/val-{{size_config}}.csv  --num_threads {{np}} --algorithm {{algorithm}}
```

Algorithm options are as follows (there are some additional options in `main.cpp` for historical reasons). The naming convention is not consistent and the meaning of each algorithm should be referred to in the following list:

- `two-phase-tree-merge` = Tree Merge
- `two-phase-central-merge-xxhash` = Central Merge
- `two-phase-radix-xxhash` = Radix
- `omp-lock-free-hash-table` = Lock-free Hash Table
- `adaptive-alg1` = Adaptive Algorithm 1
- `adaptive-alg2` = Adaptive Algorithm 2
- `adaptive-alg3` = Adaptive Algorithm 3
- `adaptive-alg4` = Adaptive Algorithm 4

To benchmark DuckDB and Polars, use `python benchmark/bench.py --help` to see options. We run this command:

```sh
python benchmark/bench.py -np {{np}} -i data/{{dist}}/{{size_config}}.csv.gz -e {{engine}} -q --num_dryruns {{num_dryruns}} --num_trials {{num_trials}}
```

Where `engine` is either `duckdb` or `polars`