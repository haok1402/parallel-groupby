#include <cassert>
#include <chrono>
#include <csignal>
#include <ctime>
#include <duckdb.hpp>
#include <iostream>
#include <omp.h>
#include <string>
#include <utility>
#include "CLI11.hpp"
#include "lib.hpp"
#include "algs/two-phase-central-merge.hpp"
#include "algs/single-thread.hpp"


void print_spotcheck(std::vector<AggResRow> agg_res) {
    for (const auto& row : agg_res) {
        if (row[0] == 419 || row[0] == 3488 || row[0] == 5997667) { // spot checking
            std::cout << row[0] << " -> (" << row[1] << ", " << row[2] << ")" << std::endl;
        }
    }
}

void print_agg_stats(std::vector<AggResRow> agg_res) {
    std::cout << ">> output has " << agg_res.size() << " rows" << std::endl;
}

int main(int argc, char *argv[]) {
    
    // 1 > parse command line
    
    // set defaults
    ExpConfig config;
    config.num_threads = 1;
    config.radix_partition_cnt_ratio = 4; // num radix partitions = this * num_threads, higher number may mean smaller granularity for any dynamic scheduling
    config.num_dryruns = 0;
    config.num_trials = 1;
    config.cardinality_reduction = -1; // option to reduce the number of unique group keys, or -1 to not do it
    config.batch_size = 10000;
    config.duckdb_style_adaptation_threshold = 10000;
    config.algorithm = "SEQUENTIAL";
    config.dataset_file_path = "data/exponential/100K-1K.csv.gz";
    config.in_table_name = "lineitem";
    // for our own generated dataset, there's only a key column and a val column:
    config.group_key_col_name = "key";
    config.data_col_names = {"val"};
    
    CLI::App app{"Whatever"};
    
    app.add_option("--num_threads", config.num_threads, "Number of threads to use during execution")->required();
    app.add_option("--num_dryruns", config.num_dryruns, "Number of warmup iterations before measurement begins")->default_val(3);
    app.add_option("--num_trials", config.num_trials, "Number of timed iterations to run for benchmarking")->default_val(5);
    app.add_option("--cardinality_reduction", config.cardinality_reduction);
    app.add_option("--radix_partition_cnt_ratio", config.radix_partition_cnt_ratio);
    app.add_option("--duckdb_style_adaptation_threshold", config.duckdb_style_adaptation_threshold);
    app.add_option("--batch_size", config.batch_size);
    std::string strat_str = "SEQUENTIAL";
    app.add_option("--algorithm", config.algorithm);
    app.add_option("--dataset_file_path", config.dataset_file_path, "Path to the gzipped CSV input file (with two integer columns)")
        ->check(CLI::ExistingFile)
        ->required();
    app.add_option("--in_table_name", config.in_table_name);
    
    CLI11_PARSE(app, argc, argv);

    config.display();
    
    // 2 > load the data
    
    RowStore table;
    load_data(config, table);
    std::vector<AggResRow> agg_res; // where to write results to
    
    std::function<void(ExpConfig &config, RowStore &table, int trial_idx, std::vector<AggResRow> &agg_res)> selected_alg;
    
    if (config.algorithm == "single-thread") {
        selected_alg = single_thread_sol;
    } else if (config.algorithm == "two-phase-centralized-merge") {
        selected_alg = two_phase_centralised_merge_sol;
    // } else if (config.algorithm == "global-lock") {
    //     selected_alg = dumb_global_lock_sol;
    // } else if (config.algorithm == "simple-two-phase-radix") {
    //     selected_alg = simple_2phase_radix_partition_sol;
    // } else if (config.algorithm == "simple-three-phase-radix") {
    //     selected_alg = simple_3phase_radix_partition_sol;
    // } else if (config.algorithm == "implicit-repartitioning") {
    //     selected_alg = implicit_repartitioning_sol;
    // } else if (config.algorithm == "duckdbish-two_phase") {
    //     selected_alg = duckdb_style_2phase_adaptation_sol;
    } else {
        throw std::runtime_error("Unsupported algorithm");
    }
    
    // 3 > run the experiment
    std::cout << "Running " << config.num_dryruns << " warm-up iteration(s) to stabilize performance" << std::endl;
    for (int dryrun_idx = 0; dryrun_idx < config.num_dryruns; dryrun_idx++) {
        agg_res.clear();
        printf(">> --- running dryrun %d ---\n", dryrun_idx);
        selected_alg(config, table, dryrun_idx, agg_res);
        if (dryrun_idx == 0) {
            print_spotcheck(agg_res);
        }
    }

    std::cout << "Running " << config.num_trials << " evaluation iteration(s) for benchmarking" << std::endl;
    for (int trial_idx = 0; trial_idx < config.num_trials; trial_idx++) {
        printf(">> --- running trial %d ---\n", trial_idx);
        agg_res.clear();
        selected_alg(config, table, trial_idx, agg_res);
    }
    
    print_agg_stats(agg_res);
    
    return 0;
}