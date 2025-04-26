#!/bin/bash
    
# 1. just build-cpp
# 2. make sure datasets have been generated
# 3. run this script from root of repository, with 2>&1 | tee <logfile>

ts=$(date "+%Y-%m-%d %H:%M:%S")
exp_identifier=$1 # e.g. "dev0"
machine_identifier=$2
echo "Timestamp: $ts"
echo "Script: experiment.sh"
echo "Comment: testing experiment runner script"
echo "Experiment Identifier: $exp_identifier"
echo "machine Identifier: $machine_identifier"

log_dir="logs/$exp_identifier/$machine_identifier"
echo "We'll write outputs to $log_dir/"

oob_engines=('duckdb' 'polars') # out of the box engines to benchmark
algorithms=('sequential' 'two-phase-central-merge' 'global-lock' 'duckdbish-two-phase' 'implicit-repartitioning' 'three-phase-radix' 'two-phase-radix') # algorithms we implement
distributions=('uniform' 'normal' 'exponential')
size_configs=('1M-1K' '1M-10K')
possible_np=(1 2 4 8 16 32 64 128)
max_np=$3
num_dryruns=3
num_trials=5

mkdir -p $log_dir

# example commands
# python benchmark/bench.py -np 2 -i data/exponential/1M-1K.csv.gz -e duckdb -q --num_dryruns 3 --num_trials 5
# ./main --num_threads 2 --algorithm two-phase-central-merge --dataset_file_path data/exponential/1M-1K.csv.gz --num_dryruns 1 --num_trials 1 --validation_file_path data/exponential/val-1M-1K.csv

# go on grid and run each experiment
for dist in "${distributions[@]}"; do
    for size_config in "${size_configs[@]}"; do
        
        # try each oob engine
        for engine in "${oob_engines[@]}"; do
            for np in "${possible_np[@]}"; do
                if [[ $np -gt $max_np ]]; then
                    continue
                fi
                exp_identifier="$dist,$size_config,$engine,np$np"
                exp_log_path="$log_dir/$exp_identifier.log"
                echo "🧪 running $exp_identifier, will write to $exp_log_path"
                python benchmark/bench.py -np $np -i data/$dist/$size_config.csv.gz -e $engine -q --num_dryruns $num_dryruns --num_trials $num_trials > $exp_log_path
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
                ./main --num_threads $np --algorithm $algorithm --dataset_file_path data/$dist/$size_config.csv.gz --num_dryruns $num_dryruns --num_trials $num_trials --validation_file_path data/$dist/val-$size_config.csv > $exp_log_path                  
                if [[ "$(grep "Validation passes" $exp_log_path)" != *"Validation passes"* ]]; then
                    echo "🚨 Validation failed for $exp_identifier"
                fi
            done
        done
        
    done
done