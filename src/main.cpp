#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
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
};

// experiment config, including input file, what to group, what to aggregate, etc.
class ExpConfig {
public:
    int num_threads;
    int batch_size;
    Strategy strategy;
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
    // std::string sql_qry = "SELECT " + config.group_key_col_name + " % 100000";
    std::string sql_qry = "SELECT " + config.group_key_col_name;
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

// requires table to be populated and in memory
void sequential_sol(ExpConfig &config, RowStore &table) {
    assert(table.n_rows > 0);
    assert(table.n_cols > 0);
    
    auto n_cols = table.n_cols;
    auto n_rows = table.n_rows;
    assert(n_cols == 3); // how to support dynamic col count?
    
    auto t0 = std::chrono::steady_clock::now();

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

    auto t1 = std::chrono::steady_clock::now();
    std::cout << "Elapsed time: " << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << " ms" << std::endl;

    // spot checking
    std::cout << 419 << " -> (" << agg_map[419][0] << ", " << agg_map[419][1] << ")" << std::endl;
    std::cout << 3488 << " -> (" << agg_map[3488][0] << ", " << agg_map[3488][1] << ")" << std::endl;
    std::cout << 5997667 << " -> (" << agg_map[5997667][0] << ", " << agg_map[5997667][1] << ")" << std::endl;
}

typedef std::chrono::time_point<std::chrono::steady_clock> chrono_time_point;

void naive_2phase_centralised_merge_sol(ExpConfig &config, RowStore &table) {
    omp_set_num_threads(config.num_threads);
    
    auto n_cols = table.n_cols;
    auto n_rows = table.n_rows;

    chrono_time_point t_overall_0;
    chrono_time_point t_overall_1;
    chrono_time_point t_phase1_0;
    chrono_time_point t_phase1_1;
    chrono_time_point t_phase2_0;
    chrono_time_point t_phase2_1;
    
    t_overall_0 = std::chrono::steady_clock::now();

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
        
        if (tid == 0) {
            t_phase1_0 = std::chrono::steady_clock::now();
        }
        
        // #pragma omp for schedule(dynamic, config.batch_size)
        #pragma omp for schedule(static, config.batch_size)
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
            std::cout << "Phase 1 time: " << std::chrono::duration_cast<std::chrono::milliseconds>(t_phase1_1 - t_phase1_0).count() << " ms" << std::endl;
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
            std::cout << "Phase 2 time: " << std::chrono::duration_cast<std::chrono::milliseconds>(t_phase2_1 - t_phase2_0).count() << " ms" << std::endl;

        }
    }
    
    t_overall_1 = std::chrono::steady_clock::now();
    std::cout << "Elapsed time: " << std::chrono::duration_cast<std::chrono::milliseconds>(t_overall_1 - t_overall_0).count() << " ms" << std::endl;
    // spot checking
    std::cout << 419 << " -> (" << agg_map[419][0] << ", " << agg_map[419][1] << ")" << std::endl;
    std::cout << 3488 << " -> (" << agg_map[3488][0] << ", " << agg_map[3488][1] << ")" << std::endl;
    std::cout << 5997667 << " -> (" << agg_map[5997667][0] << ", " << agg_map[5997667][1] << ")" << std::endl;

}

void dumb_global_lock_sol(ExpConfig &config, RowStore &table) {
    omp_set_num_threads(config.num_threads);
    
    auto n_cols = table.n_cols;
    auto n_rows = table.n_rows;

    chrono_time_point t_overall_0;
    chrono_time_point t_overall_1;
    
    t_overall_0 = std::chrono::steady_clock::now();

    std::unordered_map<int64_t, AggMapValue> agg_map;
    
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        int actual_num_threads = omp_get_num_threads();
        assert(actual_num_threads == config.num_threads);
        
        // #pragma omp for schedule(dynamic, config.batch_size)
        #pragma omp for schedule(static, config.batch_size)
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
    
    t_overall_1 = std::chrono::steady_clock::now();
    std::cout << "Elapsed time: " << std::chrono::duration_cast<std::chrono::milliseconds>(t_overall_1 - t_overall_0).count() << " ms" << std::endl;
    // spot checking
    std::cout << 419 << " -> (" << agg_map[419][0] << ", " << agg_map[419][1] << ")" << std::endl;
    std::cout << 3488 << " -> (" << agg_map[3488][0] << ", " << agg_map[3488][1] << ")" << std::endl;
    std::cout << 5997667 << " -> (" << agg_map[5997667][0] << ", " << agg_map[5997667][1] << ")" << std::endl;

}

int main(int argc, char *argv[]) {
    
    // 1 > parse command line
    
    // set defaults
    ExpConfig config;
    config.num_threads = 1;
    config.batch_size = 10000;
    config.strategy = Strategy::SEQUENTIAL;
    config.in_db_file_path = "data/tpch-sf1.db";
    config.in_table_name = "lineitem";
    config.group_key_col_name = "l_orderkey";
    config.data_col_names = {"l_partkey", "l_suppkey"};
    config.agg_funcs = {AggFunc::SUM, AggFunc::SUM};
    
    CLI::App app{"Whatever"};
    
    app.add_option("--num_threads", config.num_threads);
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
    } else {
        throw std::runtime_error("Unsupported strategy");
    }

    config.display();
    
    // 2 > load the data
    
    RowStore table;
    load_data(config, table);
    
    // 3 > run the experiment
    if (config.strategy == Strategy::SEQUENTIAL) {
        sequential_sol(config, table);
    } else if (config.strategy == Strategy::GLOBAL_LOCK) {
        dumb_global_lock_sol(config, table);
    } else if (config.strategy == Strategy::TWO_PHASE_CENTRALIZED_MERGE) {
        naive_2phase_centralised_merge_sol(config, table);
    } else {
        throw std::runtime_error("Unsupported strategy");
    }
    
    return 0;
}