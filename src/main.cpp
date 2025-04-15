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

typedef std::chrono::time_point<std::chrono::steady_clock> chrono_time_point;

struct Entry {
    int64_t l_orderkey;
    int64_t l_partkey;
    int64_t l_suppkey;
};

// struct AggMapValue {
//     int64_t v1;
//     int64_t v2;
// };


enum class AggFunc {
    SUM,
    AVG,
    MIN,
    MAX
};

enum class Strategy {
    SEQUENTIAL,
    GLOBAL_LOCK,
    TWO_PHASE_CENTRALIZED_MERGE,
    SIMPLE_TWO_PHASE_RADIX,
    SIMPLE_THREE_PHASE_RADIX,
};

// experiment config, including input file, what to group, what to aggregate, etc.
class ExpConfig {
public:
    int num_threads;
    int radix_partition_cnt_ratio;
    int batch_size;
    Strategy strategy;
    int num_dryruns;
    int num_trials;
    int cardinality_reduction;
    std::string in_db_file_path;
    std::string in_table_name;
    std::string group_key_col_name;
    std::vector<std::string> data_col_names;
    std::vector<AggFunc> agg_funcs; // TODO support diffeerent aggregation functions
    
    void display() {
        std::cout << "exp config:" << std::endl;
        std::cout << "num_threads = " << num_threads << std::endl;
        std::cout << "batch_size = " << batch_size << std::endl;
        std::cout << "strategy = " << ((int) strategy) << std::endl;
        std::cout << "in_db_file_path = " << in_db_file_path << std::endl;
        std::cout << "in_table_name = " << in_table_name << std::endl;
        std::cout << "group_key_col_name = " << group_key_col_name << std::endl;
        std::cout << "data_col_names = [";
        for (const auto& col : data_col_names) {
            std::cout << col << ", ";
        }
        std::cout << "]" << std::endl;
        std::cout << "agg_funcs = [";
        for (const auto& func : agg_funcs) {
            std::cout << ((int) func) << ", ";
        }
        std::cout << "]" << std::endl;
    }
};

// column major data storage
// usage: first reserve all memory, then write to each cell
class ColumnStore {
public:
    // std::vector<int64_t> data; 
    int64_t* data; 
    int n_cols;
    int n_rows;
    
    void init_table(int num_cols, int num_rows) {
        n_cols = num_cols;
        n_rows = num_rows;
        // data.resize(num_cols * num_rows);
        data = new int64_t[num_cols * num_rows];
    }
    
    inline int get_idx(int row_idx, int col_idx) {
        return col_idx * n_rows + row_idx;
    }
    
    inline int64_t get(int row_idx, int col_idx) {
        return data[get_idx(row_idx, col_idx)];
    }
    
    inline void write_value(int row_idx, int col_idx, int64_t value) {
        data[get_idx(row_idx, col_idx)] = value;
    }

};

class RowStore {
public:
    // std::vector<int64_t> data;
    int64_t* data; 
    int n_cols;
    int n_rows;
    
    void init_table(int num_cols, int num_rows) {
        n_cols = num_cols;
        n_rows = num_rows;
        // data.resize(num_cols * num_rows);
        data = new int64_t[num_cols * num_rows];
    }
    
    inline int get_idx(int row_idx, int col_idx) {
        return row_idx * n_cols + col_idx;
    }
    
    inline void write_value(int row_idx, int col_idx, int64_t value) {
        data[get_idx(row_idx, col_idx)] = value;
    }
    
    inline int64_t get(int row_idx, int col_idx) {
        return data[get_idx(row_idx, col_idx)];
    }
};


// using config specification, load stuff into table
// for now, assume one group column, and group key column is not any of the value columns
void load_data(ExpConfig &config, RowStore &table) {
    duckdb::DuckDB db(config.in_db_file_path);
    duckdb::Connection con(db);
    
    auto table_description = con.TableInfo(config.in_table_name);
    std::string sql_qry = "SELECT " + config.group_key_col_name;
    
    if (config.cardinality_reduction != -1) {
        sql_qry += " % " + std::to_string(config.cardinality_reduction);
    }
    
    for (const auto& col_name : config.data_col_names) {
        sql_qry += ", " + col_name;
    }
    sql_qry += " FROM " + config.in_table_name;
    std::cout << "query string = " << sql_qry << std::endl;
    auto duckdb_res = con.Query(sql_qry);

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
    std::cout << "### "
        << 419      << " -> (" << agg_map[419][0]      << ", " << agg_map[419][1]      << ") "
        << 3488     << " -> (" << agg_map[3488][0]     << ", " << agg_map[3488][1]     << ") "
        << 5997667  << " -> (" << agg_map[5997667][0]  << ", " << agg_map[5997667][1]  << ")"
        << std::endl;
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

// approach: go directly into radix partition when scanning
// phase 1: independent local scans directly into radix partitioning, with num partitions = num threads
// phase 3: assign partitions to threads and merge
void simple_2phase_radix_partition_sol(ExpConfig &config, RowStore &table, int trial_idx, std::vector<AggResRow> &agg_res) {
    omp_set_num_threads(config.num_threads);
    
    auto n_cols = table.n_cols;
    auto n_rows = table.n_rows;

    chrono_time_point t_overall_0;
    chrono_time_point t_overall_1;
    chrono_time_point t_agg_0;
    chrono_time_point t_agg_1;
    chrono_time_point t_phase1_0;
    chrono_time_point t_phase1_1;
    chrono_time_point t_phase2_0;
    chrono_time_point t_phase2_1;
    chrono_time_point t_output_0;
    chrono_time_point t_output_1;

    t_overall_0 = std::chrono::steady_clock::now();
    t_agg_0 = std::chrono::steady_clock::now();

    // auto local_agg_maps = std::vector<std::unordered_map<int64_t, AggMapValue>>(config.num_threads);
    // assert(local_agg_maps.size() == config.num_threads);
    // std::unordered_map<int64_t, AggMapValue> agg_map; // where merged results go
    
    // int n_bits_for_radix = round(log2(config.num_threads));
    // assert (0x1 << n_bits_for_radix == config.num_threads);
    int n_partitions = config.num_threads * config.radix_partition_cnt_ratio;
    
    // radix_partitions being a size n_thread array of n_thread array of local agg maps
    std::vector<std::vector<std::unordered_map<int64_t, AggMapValue>>> radix_partitions_local_maps(n_partitions, std::vector<std::unordered_map<int64_t, AggMapValue>>(config.num_threads));
    // radix_partitions[2][3] is thread 3's result for partition 2
    
    std::cout << "n_partitions = " << n_partitions << std::endl;
    std::cout << "done initialising all the partitions" << std::endl;
    
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        int actual_num_threads = omp_get_num_threads();
        // printf("hello from thread %d among %d threads\n", tid, actual_num_threads);
        assert(actual_num_threads == config.num_threads);
        
        // === PHASE 1: aggregate into partition and local aggregation map === 
        
        std::vector<std::unordered_map<int64_t, AggMapValue>> local_radix_partitions(n_partitions);
        
        if (tid == 0) { t_phase1_0 = std::chrono::steady_clock::now(); }
        
        // #pragma omp for schedule(dynamic, config.batch_size)
        #pragma omp for schedule(dynamic, config.batch_size)
        for (size_t r = 0; r < n_rows; r++) {
            int64_t group_key = table.get(r, 0);
            
            size_t group_key_hash = std::hash<int64_t>{}(group_key);
            size_t part_idx = group_key_hash % n_partitions;
            // std::cout << "part_idx = " << part_idx << std::endl;
            
            AggMapValue agg_acc;
            if (auto search = local_radix_partitions[part_idx].find(group_key); search != local_radix_partitions[part_idx].end()) {
                agg_acc = search->second;
            } else {
                agg_acc = AggMapValue{0, 0};
            }
    
            for (size_t c = 1; c < n_cols; c++) {
                agg_acc[c - 1] = agg_acc[c - 1] + table.get(r, c);
            }
            local_radix_partitions[part_idx][group_key] = agg_acc;
        }
        for (size_t part_idx = 0; part_idx < n_partitions; part_idx++) {
            radix_partitions_local_maps[part_idx][tid] = local_radix_partitions[part_idx];
        }
        #pragma omp barrier
        
        if (tid == 0) {
            t_phase1_1 = std::chrono::steady_clock::now();
            time_print("phase_1", trial_idx, t_phase1_0, t_phase1_1);
        }

        // === PHASE 2: merge within partition, in parallel === 

        if (tid == 0) {
            t_phase2_0 = std::chrono::steady_clock::now();
        }

        #pragma omp for schedule(dynamic, 1)
        for (size_t part_idx = 0; part_idx < n_partitions; part_idx++) {
            for (size_t other_tid = 1; other_tid < actual_num_threads; other_tid++) {
                auto other_local_agg_map = radix_partitions_local_maps[part_idx][other_tid];
                
                for (const auto& [group_key, other_agg_acc] : other_local_agg_map) {
                    AggMapValue agg_acc;
                    if (auto search = radix_partitions_local_maps[part_idx][0].find(group_key); search != radix_partitions_local_maps[part_idx][0].end()) {
                        agg_acc = search->second;
                        for (size_t c = 1; c < n_cols; c++) {
                            agg_acc[c - 1] = agg_acc[c - 1] + other_agg_acc[c - 1];
                        }
                    } else {
                        agg_acc = other_agg_acc;
                    }
                    radix_partitions_local_maps[part_idx][0][group_key] = agg_acc; // merge into tid 0's map for this partition
                }
            }
        }
        
        if (tid == 0) {
            t_phase2_1 = std::chrono::steady_clock::now();
            time_print("phase_2", trial_idx, t_phase2_0, t_phase2_1);
        }

    }
    
    t_agg_1 = std::chrono::steady_clock::now();
    time_print("aggregation_time", trial_idx, t_agg_0, t_agg_1);
    
    // === one thread write out the result === 
    {
        t_output_0 = std::chrono::steady_clock::now();
        
        // agg_map = std::move(radix_partitions_local_maps[0][0]);
        
        for (size_t part_idx = 0; part_idx < n_partitions; part_idx++) {
            // agg_map.merge(radix_partitions_local_maps[part_idx][0]);
            for (auto& [group_key, agg_acc] : radix_partitions_local_maps[part_idx][0]) {
                agg_res.push_back(AggResRow{group_key, agg_acc[0], agg_acc[1]});
            }
        }
        
        t_output_1 = std::chrono::steady_clock::now();
        time_print("write_output", trial_idx, t_output_0, t_output_1);
    }
    
    t_overall_1 = std::chrono::steady_clock::now();
    time_print("elapsed_time", trial_idx, t_overall_0, t_overall_1);
}

// approach: go directly into radix partition when scanning
// phase 1: independent local scans into single map
// phase 2: partition the local map into radix partitions
// phase 3: assign partitions to threads and merge
void simple_3phase_radix_partition_sol(ExpConfig &config, RowStore &table, int trial_idx, std::vector<AggResRow> &agg_res) {
    omp_set_num_threads(config.num_threads);
    
    auto n_cols = table.n_cols;
    auto n_rows = table.n_rows;

    chrono_time_point t_overall_0;
    chrono_time_point t_overall_1;
    chrono_time_point t_agg_0;
    chrono_time_point t_agg_1;
    chrono_time_point t_phase1_0;
    chrono_time_point t_phase1_1;
    chrono_time_point t_phase2_0;
    chrono_time_point t_phase2_1;
    chrono_time_point t_phase3_0;
    chrono_time_point t_phase3_1;
    chrono_time_point t_output_0;
    chrono_time_point t_output_1;

    t_overall_0 = std::chrono::steady_clock::now();
    t_agg_0 = std::chrono::steady_clock::now();
    
    int n_partitions = config.num_threads * config.radix_partition_cnt_ratio;
    
    // radix_partitions being a size n_thread array of n_thread array of local agg maps
    std::vector<std::vector<std::unordered_map<int64_t, AggMapValue>>> radix_partitions_local_maps(n_partitions, std::vector<std::unordered_map<int64_t, AggMapValue>>(config.num_threads));
    // radix_partitions[2][3] is thread 3's result for partition 2
    
    std::cout << "n_partitions = " << n_partitions << std::endl;
    std::cout << "done initialising all the partitions" << std::endl;
    
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        int actual_num_threads = omp_get_num_threads();
        // printf("hello from thread %d among %d threads\n", tid, actual_num_threads);
        assert(actual_num_threads == config.num_threads);
        
        // === PHASE 1: local aggregation map === 
        
        std::unordered_map<int64_t, AggMapValue> local_agg_map;
        
        if (tid == 0) { t_phase1_0 = std::chrono::steady_clock::now(); }
        
        // #pragma omp for schedule(dynamic, config.batch_size)
        #pragma omp for schedule(dynamic, config.batch_size)
        for (size_t r = 0; r < n_rows; r++) {
            int64_t group_key = table.get(r, 0);
           
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
        if (tid == 0) {
            t_phase1_1 = std::chrono::steady_clock::now();
            time_print("phase_1", trial_idx, t_phase1_0, t_phase1_1);
        }
        #pragma omp barrier
        
        // === PHASE 2: each thread break their map into partitions === 
        if (tid == 0) { t_phase2_0 = std::chrono::steady_clock::now(); }
        for (auto& [group_key, agg_acc] : local_agg_map) {
            size_t group_key_hash = std::hash<int64_t>{}(group_key);
            size_t part_idx = group_key_hash % n_partitions;
            radix_partitions_local_maps[part_idx][tid][group_key] = agg_acc;
        }
        #pragma omp barrier
        if (tid == 0) {
            t_phase2_1 = std::chrono::steady_clock::now();
            time_print("phase_2", trial_idx, t_phase2_0, t_phase2_1);
        }


        // === PHASE 3: merge within partition, in parallel === 

        if (tid == 0) { t_phase3_0 = std::chrono::steady_clock::now(); }

        #pragma omp for schedule(dynamic, 1)
        for (size_t part_idx = 0; part_idx < n_partitions; part_idx++) {
            for (size_t other_tid = 1; other_tid < actual_num_threads; other_tid++) {
                auto other_local_agg_map = radix_partitions_local_maps[part_idx][other_tid];
                
                for (const auto& [group_key, other_agg_acc] : other_local_agg_map) {
                    AggMapValue agg_acc;
                    if (auto search = radix_partitions_local_maps[part_idx][0].find(group_key); search != radix_partitions_local_maps[part_idx][0].end()) {
                        agg_acc = search->second;
                        for (size_t c = 1; c < n_cols; c++) {
                            agg_acc[c - 1] = agg_acc[c - 1] + other_agg_acc[c - 1];
                        }
                    } else {
                        agg_acc = other_agg_acc;
                    }
                    radix_partitions_local_maps[part_idx][0][group_key] = agg_acc; // merge into tid 0's map for this partition
                }
            }
        }
        
        if (tid == 0) {
            t_phase3_1 = std::chrono::steady_clock::now();
            time_print("phase_3", trial_idx, t_phase3_0, t_phase3_1);
        }

    }
    
    t_agg_1 = std::chrono::steady_clock::now();
    time_print("aggregation_time", trial_idx, t_agg_0, t_agg_1);
    
    // === one thread write out the result === 
    {
        t_output_0 = std::chrono::steady_clock::now();
        
        // agg_map = std::move(radix_partitions_local_maps[0][0]);
        
        for (size_t part_idx = 0; part_idx < n_partitions; part_idx++) {
            // agg_map.merge(radix_partitions_local_maps[part_idx][0]);
            for (auto& [group_key, agg_acc] : radix_partitions_local_maps[part_idx][0]) {
                agg_res.push_back(AggResRow{group_key, agg_acc[0], agg_acc[1]});
            }
        }
        
        t_output_1 = std::chrono::steady_clock::now();
        time_print("write_output", trial_idx, t_output_0, t_output_1);
    }
    
    t_overall_1 = std::chrono::steady_clock::now();
    time_print("elapsed_time", trial_idx, t_overall_0, t_overall_1);
}

void dumb_global_lock_sol(ExpConfig &config, RowStore &table, int trial_idx, std::vector<AggResRow> &agg_res) {
    omp_set_num_threads(config.num_threads);
    
    auto n_cols = table.n_cols;
    auto n_rows = table.n_rows;

    chrono_time_point t_overall_0;
    chrono_time_point t_overall_1;
    chrono_time_point t_agg_0;
    chrono_time_point t_agg_1;
    chrono_time_point t_output_0;
    chrono_time_point t_output_1;

    t_overall_0 = std::chrono::steady_clock::now();
    t_agg_0 = std::chrono::steady_clock::now();

    std::unordered_map<int64_t, AggMapValue> agg_map;
    
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        int actual_num_threads = omp_get_num_threads();
        assert(actual_num_threads == config.num_threads);
        
        // #pragma omp for schedule(dynamic, config.batch_size)
        #pragma omp for schedule(dynamic, config.batch_size)
        for (size_t r = 0; r < n_rows; r++) {
            #pragma omp critical 
            {
                auto group_key = table.get(r, 0);
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
    config.strategy = Strategy::SEQUENTIAL;
    config.in_db_file_path = "data/tpch-sf1.db";
    config.in_table_name = "lineitem";
    config.group_key_col_name = "l_orderkey";
    config.data_col_names = {"l_partkey", "l_suppkey"};
    config.agg_funcs = {AggFunc::SUM, AggFunc::SUM};
    
    CLI::App app{"Whatever"};
    
    app.add_option("--num_threads", config.num_threads);
    app.add_option("--num_dryruns", config.num_dryruns);
    app.add_option("--num_trials", config.num_trials);
    app.add_option("--cardinality_reduction", config.cardinality_reduction);
    app.add_option("--radix_partition_cnt_ratio", config.radix_partition_cnt_ratio);
    app.add_option("--batch_size", config.batch_size);
    std::string strat_str = "SEQUENTIAL";
    app.add_option("--strategy", strat_str);
    app.add_option("--in_db_file_path", config.in_db_file_path);
    app.add_option("--in_table_name", config.in_table_name);
    // app.add_option("--group_key_col_name", config.group_key_col_name);
    
    CLI11_PARSE(app, argc, argv);

    if (strat_str == "SEQUENTIAL") {
        config.strategy = Strategy::SEQUENTIAL;
    } else if (strat_str == "GLOBAL_LOCK") {
        config.strategy = Strategy::GLOBAL_LOCK;
    } else if (strat_str == "TWO_PHASE_CENTRALIZED_MERGE") {
        config.strategy = Strategy::TWO_PHASE_CENTRALIZED_MERGE;
    } else if (strat_str == "SIMPLE_TWO_PHASE_RADIX") {
        config.strategy = Strategy::SIMPLE_TWO_PHASE_RADIX;
    } else if (strat_str == "SIMPLE_THREE_PHASE_RADIX") {
        config.strategy = Strategy::SIMPLE_THREE_PHASE_RADIX;
    } else {
        throw std::runtime_error("Unsupported strategy");
    }

    config.display();
    
    // 2 > load the data
    
    RowStore table;
    load_data(config, table);
    std::vector<AggResRow> agg_res; // where to write results to
    
    auto run_once = [&](int run_idx, std::vector<AggResRow>& agg_res) {
        if (config.strategy == Strategy::SEQUENTIAL) {
            sequential_sol(config, table, run_idx, agg_res);
        } else if (config.strategy == Strategy::GLOBAL_LOCK) {
            dumb_global_lock_sol(config, table, run_idx, agg_res);
        } else if (config.strategy == Strategy::TWO_PHASE_CENTRALIZED_MERGE) {
            naive_2phase_centralised_merge_sol(config, table, run_idx, agg_res);
        } else if (config.strategy == Strategy::SIMPLE_TWO_PHASE_RADIX) {
            simple_2phase_radix_partition_sol(config, table, run_idx, agg_res);
        } else if (config.strategy == Strategy::SIMPLE_THREE_PHASE_RADIX) {
            simple_3phase_radix_partition_sol(config, table, run_idx, agg_res);
        } else {
            throw std::runtime_error("Unsupported strategy");
        }
    };
    
    // 3 > run the experiment
    for (int dryrun_idx = 0; dryrun_idx < config.num_dryruns; dryrun_idx++) {
        agg_res.clear();
        printf(">> --- running dryrun %d ---\n", dryrun_idx);
        run_once(dryrun_idx, agg_res);
        if (dryrun_idx == 0) {
            print_spotcheck(agg_res);
        }
    }

    for (int trial_idx = 0; trial_idx < config.num_trials; trial_idx++) {
        printf(">> --- running trial %d ---\n", trial_idx);
        agg_res.clear();
        run_once(trial_idx, agg_res);
    }
    
    print_agg_stats(agg_res);
    
    return 0;
}