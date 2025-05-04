#!/bin/bash
    
# 1. just clean && just build-cpp-release
# 2. make sure datasets have been generated
# 3. run this script from root of repository, with 2>&1 | tee <logfile>
# or sbatch -p RM -N 1 -t 6:00:00 benchmark/adaptation-curve-ghc.sh dev-curve2-alg123 ghc 8


ts=$(date "+%Y-%m-%d %H:%M:%S")
exp_identifier=$1 # e.g. "dev0"
machine_identifier=$2 # e.g. "psc"
echo "Timestamp: $ts"
echo "Script: experiment.sh"
echo "Comment: testing experiment runner script"
echo "Experiment Identifier: $exp_identifier"
echo "machine Identifier: $machine_identifier"

log_dir="logs/$exp_identifier/$machine_identifier"
echo "We'll write outputs to $log_dir/"

oob_engines=() # out of the box engines to benchmark
# oob_engines=('duckdb' 'polars') # out of the box engines to benchmark

# algorithms=('two-phase-central-merge-xxhash')
# algorithms=('two-phase-radix-xxhash' 'lock-free-hash-table' 'two-phase-central-merge' 'duckdbish-two-phase' 'implicit-repartitioning' 'three-phase-radix' 'two-phase-radix')
# algorithms=('two-phase-central-merge-xxhash' 'two-phase-radix-xxhash' 'duckdbish-two-phase' 'lock-free-hash-table' 'implicit-repartitioning') # algorithms we implement
# algorithms=('omp-lock-free-hash-table' 'two-phase-tree-merge')
# algorithms=('two-phase-central-merge-xxhash')
algorithms=('adaptive-alg1' 'adaptive-alg2' 'adaptive-alg3'  'two-phase-central-merge-xxhash' 'two-phase-radix-xxhash' 'two-phase-tree-merge' 'lock-free-hash-table' )
# algorithms=()

distributions=('uniform')

size_configs=('80M-20K' '80M-200K' '80M-2M' '80M-20M' '80M-10K' '80M-100K' '80M-1M' '80M-10M')
# size_configs=('8M-2K' '8M-20K' '8M-200K' '8M-2M' '8M-1K' '8M-10K' '8M-100K' '8M-1M'  '80M-20K' '80M-200K' '80M-2M' '80M-20M' '80M-10K' '80M-100K' '80M-1M' '80M-10M')
# size_configs=('8M-2K' '8M-20K' '8M-200K' '8M-2M'    '80M-20K' '80M-200K' '80M-2M' '80M-20M')
# '8M-2K' '8M-20K' '8M-200K' '8M-2M'   '80M-20K' '80M-200K' '80M-2M' '80M-20M'    '800M-200K' '800M-2M' '800M-20M' '800M-200M'

max_np=$3
possible_np=($max_np)
num_dryruns=3
num_trials=5

mkdir -p $log_dir

# example commands
# python benchmark/bench.py -np 2 -i data/exponential/1M-1K.csv.gz -e duckdb -q --num_dryruns 3 --num_trials 5
# ./main --num_threads 2 --algorithm two-phase-central-merge --dataset_file_path data/exponential/1M-1K.csv.gz --num_dryruns 1 --num_trials 1 --validation_file_path data/exponential/val-1M-1K.csv

# go on grid and run each experiment
for dist in "${distributions[@]}"; do
    for size_config in "${size_configs[@]}"; do
        pushd /dev/shm/datagen > /dev/null
        echo -e "running just generate $dist ${size_config%-*} ${size_config#*-} in /dev/shm/datagen"
        just generate $dist ${size_config%-*} ${size_config#*-}
        popd > /dev/null
        
        # try each oob engine
        for engine in "${oob_engines[@]}"; do
            for np in "${possible_np[@]}"; do
                if [[ $np -gt $max_np ]]; then
                    continue
                fi
                exp_identifier="$dist,$size_config,$engine,np$np"
                exp_log_path="$log_dir/$exp_identifier.log"
                echo "🧪 running $exp_identifier, will write to $exp_log_path"
                python benchmark/bench.py -np $np -i /dev/shm/datagen/data/$dist/$size_config.csv.gz -e $engine -q --num_dryruns $num_dryruns --num_trials $num_trials > $exp_log_path
            done
        done
        
        # try each algorithm
        for algorithm in "${algorithms[@]}"; do
            for np in "${possible_np[@]}"; do
                if [[ $np -gt $max_np ]]; then
                    continue
                fi
                exp_identifier="$dist,$size_config,$algorithm,np$np"
                exp_log_path="$log_dir/$exp_identifier.log"
                echo "🧪 running $exp_identifier, will write to $exp_log_path"
                echo "💨 ./main --num_dryruns $num_dryruns --num_trials $num_trials --dataset_file_path /dev/shm/datagen/data/$dist/$size_config.csv.gz  --validation_file_path /dev/shm/datagen/data/$dist/val-$size_config.csv  --num_threads $np --algorithm $algorithm"
                ./main --num_dryruns $num_dryruns --num_trials $num_trials --dataset_file_path /dev/shm/datagen/data/$dist/$size_config.csv.gz  --validation_file_path /dev/shm/datagen/data/$dist/val-$size_config.csv  --num_threads $np --algorithm $algorithm > $exp_log_path
                if [[ "$(grep "Validation passes" $exp_log_path)" != *"Validation passes"* ]]; then
                    echo "🚨 Validation failed for $exp_identifier, see log at $exp_log_path"
                    echo "⚠️ retry once"
                    ./main --num_dryruns $num_dryruns --num_trials $num_trials --dataset_file_path /dev/shm/datagen/data/$dist/$size_config.csv.gz  --validation_file_path /dev/shm/datagen/data/$dist/val-$size_config.csv  --num_threads $np --algorithm $algorithm > $exp_log_path
                    if [[ "$(grep "Validation passes" $exp_log_path)" != *"Validation passes"* ]]; then
                        echo "🚨🚨 Validation again for $exp_identifier, see log at $exp_log_path, give up"
                        echo "log as below"
                        tail -n 50 $exp_log_path
                    fi
                fi
            done
        done

        rm -rf /dev/shm/datagen/data        
    done
done