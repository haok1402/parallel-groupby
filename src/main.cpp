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
#include "algs/_all_algs.hpp"

void validate_results(std::vector<AggResRow> agg_res, std::unordered_map<int64_t, AggMapValue> reference_agg_map) {
    for (const auto& row : agg_res) {
        // if row in reference_agg_map, check each value equal
        auto group_key = row[0];
        if (auto search = reference_agg_map.find(group_key); search != reference_agg_map.end()) {
            for (size_t i = 1; i < row.size(); ++i) {
                if (row[i] != search->second[i-1]) {
                    std::cerr << "Validation failed: group_key " << row[0] << ", column " << i << ", expect " << search->second[i-1] << ", got " << row[i] << std::endl;
                    throw std::runtime_error("Results are wrong");
                }
            }
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
    config.validation_file_path = "data/exponential/val-100K-1K.csv.gz";
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
    app.add_option("--dataset_file_path", config.dataset_file_path, "Path to the gzipped CSV input file (with two integer columns)")->check(CLI::ExistingFile)->required();
    app.add_option("--validation_file_path", config.validation_file_path, "Path to sampled reference results")->check(CLI::ExistingFile)->required();
    app.add_option("--in_table_name", config.in_table_name);
    
    CLI11_PARSE(app, argc, argv);

    config.display();
    
    // 2 > load the data
    
    RowStore table;
    load_data(config, table);
    std::vector<AggResRow> agg_res; // where to write results to
    
    std::function<void(ExpConfig &config, RowStore &table, int trial_idx, bool do_print_stats, std::vector<AggResRow> &agg_res)> selected_alg;
    
    if (config.algorithm == "sequential") {
        selected_alg = sequential_sol;
    } else if (config.algorithm == "two-phase-central-merge") {
        selected_alg = two_phase_centralised_merge_sol;
    } else if (config.algorithm == "two-phase-tree-merge") {
        selected_alg = two_phase_tree_merge_sol;
    } else if (config.algorithm == "two-phase-central-merge-xxhash") {
        selected_alg = two_phase_centralised_merge_xxhash_sol;
    } else if (config.algorithm == "global-lock") {
        selected_alg = global_lock_sol;
    } else if (config.algorithm == "two-phase-radix") {
        selected_alg = two_phase_radix_sol;
    } else if (config.algorithm == "two-phase-radix-xxhash") {
        selected_alg = two_phase_radix_xxhash_sol;
    } else if (config.algorithm == "duckdbish-two-phase") {
        selected_alg = duckdbish_two_phase_sol;
    } else if (config.algorithm == "implicit-repartitioning") {
        selected_alg = implicit_repartitioning_sol;
    } else if (config.algorithm == "three-phase-radix") {
        selected_alg = three_phase_radix_sol;
    } else if (config.algorithm == "omp-lock-free-hash-table") {
        selected_alg = omp_lock_free_hash_table_sol;
    } else if (config.algorithm == "lock-free-hash-table") {
        selected_alg = lock_free_hash_table_sol;
    } else if (config.algorithm == "adaptive-alg1") {
        selected_alg = adaptive_alg1_sol;
    } else if (config.algorithm == "adaptive-alg2") {
        selected_alg = adaptive_alg2_sol;
    } else if (config.algorithm == "adaptive-alg3") {
        selected_alg = adaptive_alg3_sol;
    // } else if (config.algorithm == "adaptive-alg4") {
    //     selected_alg = adaptive_alg4_sol;
    } else {
        throw std::runtime_error("Unsupported algorithm");
    }
    
    // 3 > run the experiment
    std::cout << "Running " << config.num_dryruns << " warm-up iteration(s) to stabilize performance" << std::endl;
    for (int dryrun_idx = 0; dryrun_idx < config.num_dryruns; dryrun_idx++) {
        agg_res.clear();
        printf(">> --- running dryrun %d ---\n", dryrun_idx);
        selected_alg(config, table, dryrun_idx, false, agg_res);
    }

    std::cout << "Running " << config.num_trials << " evaluation iteration(s) for benchmarking" << std::endl;
    for (int trial_idx = 0; trial_idx < config.num_trials; trial_idx++) {
        printf(">> --- running trial %d ---\n", trial_idx);
        agg_res.clear();
        selected_alg(config, table, trial_idx, true, agg_res);
    }
        
    std::cout << "Validating results against reference" << std::endl;
    auto reference_agg_map = load_valiadtion_data(config);
    validate_results(agg_res, reference_agg_map);
    std::cout << "Validation passes" << std::endl;
    
    return 0;
}