#!/bin/bash

# As the number of groups increases, which algorithm performs better at scaling?

# WORST
vtune -collect hotspots -result-dir profiling/two-phase-merge-8M20K -- ./main --algorithm two-phase-central-merge --num_threads 16 --dataset_file_path data/uniform/8M-20K.csv.gz --validation_file_path data/uniform/val-8M-20K.csv
vtune -collect hotspots -result-dir profiling/two-phase-merge-8M200K -- ./main --algorithm two-phase-central-merge --num_threads 16 --dataset_file_path data/uniform/8M-200K.csv.gz --validation_file_path data/uniform/val-8M-200K.csv
vtune -collect hotspots -result-dir profiling/two-phase-merge-8M2M -- ./main --algorithm two-phase-central-merge --num_threads 16 --dataset_file_path data/uniform/8M-2M.csv.gz --validation_file_path data/uniform/val-8M-2M.csv

# 2nd WORST
vtune -collect hotspots -result-dir profiling/three-phase-radix-8M20K -- ./main --algorithm three-phase-radix --num_threads 16 --dataset_file_path data/uniform/8M-20K.csv.gz --validation_file_path data/uniform/val-8M-20K.csv
vtune -collect hotspots -result-dir profiling/three-phase-radix-8M200K -- ./main --algorithm three-phase-radix --num_threads 16 --dataset_file_path data/uniform/8M-200K.csv.gz --validation_file_path data/uniform/val-8M-200K.csv
vtune -collect hotspots -result-dir profiling/three-phase-radix-8M2M -- ./main --algorithm three-phase-radix --num_threads 16 --dataset_file_path data/uniform/8M-2M.csv.gz --validation_file_path data/uniform/val-8M-2M.csv

# BEST
vtune -collect hotspots -result-dir profiling/lock-free-hash-table-8M20K -- ./main --algorithm lock-free-hash-table --num_threads 16 --dataset_file_path data/uniform/8M-20K.csv.gz --validation_file_path data/uniform/val-8M-20K.csv
vtune -collect hotspots -result-dir profiling/lock-free-hash-table-8M200K -- ./main --algorithm lock-free-hash-table --num_threads 16 --dataset_file_path data/uniform/8M-200K.csv.gz --validation_file_path data/uniform/val-8M-200K.csv
vtune -collect hotspots -result-dir profiling/lock-free-hash-table-8M2M -- ./main --algorithm lock-free-hash-table --num_threads 16 --dataset_file_path data/uniform/8M-2M.csv.gz --validation_file_path data/uniform/val-8M-2M.csv
