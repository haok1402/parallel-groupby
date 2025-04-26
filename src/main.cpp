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
#include "duckdb/main/settings.hpp"
typedef std::chrono::time_point<std::chrono::steady_clock> chrono_time_point;

// experiment config, including input file, what to group, what to aggregate, etc.
class ExpConfig {
public:
    int num_threads;
    int radix_partition_cnt_ratio;
    int batch_size;
    int duckdb_style_adaptation_threshold;
    std::string algorithm;
    int num_dryruns;
    int num_trials;
    int cardinality_reduction;
    std::string dataset_file_path;
    std::string in_table_name;
    std::string group_key_col_name;
    std::vector<std::string> data_col_names;
    
    void display() {
        std::cout << "exp config:" << std::endl;
        std::cout << "num_threads = " << num_threads << std::endl;
        std::cout << "radix_partition_cnt_ratio = " << radix_partition_cnt_ratio << std::endl;
        std::cout << "batch_size = " << batch_size << std::endl;
        std::cout << "duckdb_style_adaptation_threshold = " << duckdb_style_adaptation_threshold << std::endl;
        std::cout << "algorithm = " << algorithm << std::endl;
        std::cout << "dataset_file_path = " << dataset_file_path << std::endl;
        std::cout << "in_table_name = " << in_table_name << std::endl;
        std::cout << "group_key_col_name = " << group_key_col_name << std::endl;
        std::cout << "data_col_names = [";
        for (const auto& col : data_col_names) {
            std::cout << col << ", ";
        }
        std::cout << "]" << std::endl;
    }
};

// using config specification, load stuff into table
// for now, assume one group column, and group key column is not any of the value columns
void load_data(ExpConfig &config, RowStore &table) {
    // duckdb::DuckDB db(config.dataset_file_path);
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    
    auto duckdb_res = con.Query("select key, val from '" + config.dataset_file_path + "'");

    int n_cols = config.data_col_names.size() + 1;
    table.init_table(n_cols, duckdb_res->RowCount());
    
    // go through all chunks
    int r = 0;
    while (auto chunk = duckdb_res->Fetch()) {
        // go through each col i in the chunk
        for (duckdb::idx_t r_in_chunk = 0; r_in_chunk < chunk->size(); r_in_chunk++) {
            for (duckdb::idx_t c = 0; c < n_cols; c++) {
                table.write_value(r, c, chunk->GetValue(c, r_in_chunk).GetValue<int64_t>());
            }
            r++;
        }
    }
    
    std::cout << "table.n_rows = " << table.n_rows << std::endl;
    std::cout << "table.n_cols = " << table.n_cols << std::endl;
}








typedef std::array<int64_t, 2> AggMapValue; // TODO is there a way to not hard code the size?
typedef std::array<int64_t, 2+1> AggResRow;   // TODO is there a way to not hard code the size?

void time_print(std::string title, int run_id, chrono_time_point start, chrono_time_point end) {
    std::cout << ">>> run=" << run_id << ", " << title << "=" << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms" << std::endl;
}

// requires table to be populated and in memory
void sequential_sol(ExpConfig &config, RowStore &table, int trial_idx, std::vector<AggResRow> &agg_res) {
    assert(table.n_rows > 0);
    assert(table.n_cols > 0);
    
    auto n_cols = table.n_cols;
    auto n_rows = table.n_rows;

    
    assert(n_cols == 3); // how to support dynamic col count?
    
    chrono_time_point t_overall_0;
    chrono_time_point t_overall_1;
    chrono_time_point t_agg_0;
    chrono_time_point t_agg_1;
    chrono_time_point t_output_0;
    chrono_time_point t_output_1;

    t_overall_0 = std::chrono::steady_clock::now();
    t_agg_0 = std::chrono::steady_clock::now();

    std::unordered_map<int64_t, AggMapValue> agg_map;
    for (size_t r = 0; r < n_rows; r++) {
        auto group_key = table.get(r, 0);
        
        // find existing entry, if not initialise
        AggMapValue agg_acc;
        if (auto search = agg_map.find(group_key); search != agg_map.end()) {
            agg_acc = search->second;
        } else {
            agg_acc = AggMapValue{0, 0};
        }

        for (size_t c = 1; c < n_cols; c++) {
            agg_acc[c - 1] = agg_acc[c - 1] + table.get(r, c);
        }
        agg_map[group_key] = agg_acc;
    }
    
    t_agg_1 = std::chrono::steady_clock::now();
    time_print("aggregation_time", trial_idx, t_agg_0, t_agg_1);

    {
        t_output_0 = std::chrono::steady_clock::now();

        // write output
        for (auto& [group_key, agg_acc] : agg_map) {
            agg_res.push_back(AggResRow{group_key, agg_acc[0], agg_acc[1]});
        }
        
        t_output_1 = std::chrono::steady_clock::now();
        time_print("write_output", trial_idx, t_output_0, t_output_1);
    }

    t_overall_1 = std::chrono::steady_clock::now();
    time_print("elapsed_time", trial_idx, t_overall_0, t_overall_1);

    // spot checking
    std::cout << 419 << " -> (" << agg_map[419][0] << ", " << agg_map[419][1] << ")" << std::endl;
    std::cout << 3488 << " -> (" << agg_map[3488][0] << ", " << agg_map[3488][1] << ")" << std::endl;
    std::cout << 5997667 << " -> (" << agg_map[5997667][0] << ", " << agg_map[5997667][1] << ")" << std::endl;
}


void naive_2phase_centralised_merge_sol(ExpConfig &config, RowStore &table, int trial_idx, std::vector<AggResRow> &agg_res) {
    omp_set_num_threads(config.num_threads);
    
    auto n_cols = table.n_cols;
    auto n_rows = table.n_rows;

    chrono_time_point t_overall_0;
    chrono_time_point t_overall_1;
    chrono_time_point t_phase1_0;
    chrono_time_point t_phase1_1;
    chrono_time_point t_phase2_0;
    chrono_time_point t_phase2_1;
    chrono_time_point t_agg_0;
    chrono_time_point t_agg_1;
    chrono_time_point t_output_0;
    chrono_time_point t_output_1;

    
    t_overall_0 = std::chrono::steady_clock::now();
    t_agg_0 = std::chrono::steady_clock::now();

    auto local_agg_maps = std::vector<std::unordered_map<int64_t, AggMapValue>>(config.num_threads);
    assert(local_agg_maps.size() == config.num_threads);
    std::unordered_map<int64_t, AggMapValue> agg_map; // where merged results go
    
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        int actual_num_threads = omp_get_num_threads();
        // printf("hello from thread %d among %d threads\n", tid, actual_num_threads);
        assert(actual_num_threads == config.num_threads);
        
        // PHASE 1: local aggregation map
        std::unordered_map<int64_t, AggMapValue> local_agg_map;
        
        if (tid == 0) { t_phase1_0 = std::chrono::steady_clock::now(); }
        
        // #pragma omp for schedule(dynamic, config.batch_size)
        #pragma omp for schedule(dynamic, config.batch_size)
        for (size_t r = 0; r < n_rows; r++) {
            auto group_key = table.get(r, 0);
            AggMapValue agg_acc;
            if (auto search = local_agg_map.find(group_key); search != local_agg_map.end()) {
                agg_acc = search->second;
            } else {
                agg_acc = AggMapValue{0, 0};
            }
    
            for (size_t c = 1; c < n_cols; c++) {
                agg_acc[c - 1] = agg_acc[c - 1] + table.get(r, c);
            }
            local_agg_map[group_key] = agg_acc;
        }
        local_agg_maps[tid] = local_agg_map;
        
        #pragma omp barrier
        
        if (tid == 0) {
            t_phase1_1 = std::chrono::steady_clock::now();
            time_print("phase_1", trial_idx, t_phase1_0, t_phase1_1);
        }

        // PHASE 2: thread 0 merges results
        if (tid == 0) {
            t_phase2_0 = std::chrono::steady_clock::now();
            
            agg_map = std::move(local_agg_maps[0]);
            
            for (int other_tid = 1; other_tid < actual_num_threads; other_tid++) {
                auto other_local_agg_map = local_agg_maps[other_tid];
                
                for (const auto& [group_key, other_agg_acc] : other_local_agg_map) {
                    AggMapValue agg_acc;
                    if (auto search = agg_map.find(group_key); search != agg_map.end()) {
                        agg_acc = search->second;
                        for (size_t c = 1; c < n_cols; c++) {
                            agg_acc[c - 1] = agg_acc[c - 1] + other_agg_acc[c - 1];
                        }
                    } else {
                        agg_acc = other_agg_acc;
                    }
                    agg_map[group_key] = agg_acc;
                }
            }
            
            t_phase2_1 = std::chrono::steady_clock::now();
            time_print("phase_2", trial_idx, t_phase2_0, t_phase2_1);

        }
    }
    
    t_agg_1 = std::chrono::steady_clock::now();
    time_print("aggregation_time", trial_idx, t_agg_0, t_agg_1);
    
    {
        t_output_0 = std::chrono::steady_clock::now();

        // write output
        for (auto& [group_key, agg_acc] : agg_map) {
            agg_res.push_back(AggResRow{group_key, agg_acc[0], agg_acc[1]});
        }
        
        t_output_1 = std::chrono::steady_clock::now();
        time_print("write_output", trial_idx, t_output_0, t_output_1);
    }
    
    t_overall_1 = std::chrono::steady_clock::now();
    time_print("elapsed_time", trial_idx, t_overall_0, t_overall_1);

}

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
    
    auto run_once = [&](int run_idx, std::vector<AggResRow>& agg_res) {
        if (config.algorithm == "sequential") {
            sequential_sol(config, table, run_idx, agg_res);
        } else if (config.algorithm == "two-phase-centralized-merge") {
            naive_2phase_centralised_merge_sol(config, table, run_idx, agg_res);
        // } else if (config.algorithm == "global-lock") {
        //     dumb_global_lock_sol(config, table, run_idx, agg_res);
        // } else if (config.algorithm == "simple-two-phase-radix") {
        //     simple_2phase_radix_partition_sol(config, table, run_idx, agg_res);
        // } else if (config.algorithm == "simple-three-phase-radix") {
        //     simple_3phase_radix_partition_sol(config, table, run_idx, agg_res);
        // } else if (config.algorithm == "implicit-repartitioning") {
        //     implicit_repartitioning_sol(config, table, run_idx, agg_res);
        // } else if (config.algorithm == "duckdbish-two_phase") {
        //     duckdb_style_2phase_adaptation_sol(config, table, run_idx, agg_res);
        } else {
            throw std::runtime_error("Unsupported algorithm");
        }
    };
    
    // 3 > run the experiment
    std::cout << "Running " << config.num_dryruns << " warm-up iteration(s) to stabilize performance" << std::endl;
    for (int dryrun_idx = 0; dryrun_idx < config.num_dryruns; dryrun_idx++) {
        agg_res.clear();
        printf(">> --- running dryrun %d ---\n", dryrun_idx);
        run_once(dryrun_idx, agg_res);
        if (dryrun_idx == 0) {
            print_spotcheck(agg_res);
        }
    }

    std::cout << "Running " << config.num_trials << " evaluation iteration(s) for benchmarking" << std::endl;
    for (int trial_idx = 0; trial_idx < config.num_trials; trial_idx++) {
        printf(">> --- running trial %d ---\n", trial_idx);
        agg_res.clear();
        run_once(trial_idx, agg_res);
    }
    
    print_agg_stats(agg_res);
    
    return 0;
}