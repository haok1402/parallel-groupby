#include "duckdb/common/types/value.hpp"
#include <cassert>
#include <chrono>
#include <csignal>
#include <duckdb.hpp>
#include <iostream>
#include <omp.h>
#include <string>

struct Entry {
    int64_t l_orderkey;
    int64_t l_partkey;
    int64_t l_suppkey;
};

enum class AggFunc {
    SUM,
    AVG,
    MIN,
    MAX
};

enum class Strategy {
    SEQUENTIAL,
    TWO_PHASE_CENTRALIZED_MERGE,
};

// experiment config, including input file, what to group, what to aggregate, etc.
class ExpConfig {
public:
    int num_threads;
    Strategy strategy;
    std::string in_db_file_path;
    std::string in_table_name;
    std::string group_key_col_name;
    std::vector<std::string> data_col_names;
    std::vector<AggFunc> agg_funcs; // TODO support diffeerent aggregation functions
    
    void display() {
        std::cout << "exp config:" << std::endl;
        std::cout << "num_threads = " << num_threads << std::endl;
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
class DataStore {
public:
    std::vector<std::vector<duckdb::Value>> data;
    
    duckdb::Value get(int row_idx, int col_idx) {
        return data[col_idx][row_idx];
    }
    
    duckdb::Value& get_ref(int row_idx, int col_idx) {
        return data[col_idx][row_idx];
    }
    
    void init_table(int num_cols, int num_rows) {
        data.resize(num_cols);
        for (auto& col : data) {
            col.reserve(num_rows);
        }
    }
    
    int n_cols() const {
        return data.size();
    }
    
    int n_rows() const {
        if (data.empty()) {
            return 0;
        }
        return data[0].size();
    }
};

// using config specification, load stuff into table
// for now, assume one group column, and group key column is not any of the value columns
void load_data(ExpConfig &config, DataStore &table) {
    duckdb::DuckDB db(config.in_db_file_path);
    duckdb::Connection con(db);
    
    auto table_description = con.TableInfo(config.in_table_name);
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
    while (auto chunk = duckdb_res->Fetch()) {
        // go through each col i in the chunk
        for (duckdb::idx_t r_in_chunk = 0; r_in_chunk < chunk->size(); r_in_chunk++) {
            for (duckdb::idx_t c = 0; c < n_cols; c++) {
                table.data[c].push_back(chunk->GetValue(c, r_in_chunk));
            }
        }
    }
    
    std::cout << "table.n_rows() = " << table.n_rows() << std::endl;
    std::cout << "table.n_cols() = " << table.n_cols() << std::endl;
}

// requires table to be populated and in memory
void sequential_sol(ExpConfig &config, DataStore &table) {
    assert(table.n_rows() > 0);
    assert(table.n_cols() > 0);
    
    auto n_cols = table.n_cols();
    auto n_rows = table.n_rows();
    auto t0 = std::chrono::high_resolution_clock::now();

    // groupby with just some map
    std::unordered_map<int64_t, std::vector<int64_t>> agg_map;

    for (size_t r = 0; r < n_rows; r++) {
        auto group_key = table.get(r, 0).GetValue<int64_t>();
        
        // find existing entry, if not initialise
        std::vector<duckdb::Value> existing;
        if (auto search = agg_map.find(group_key); search != agg_map.end()) {
        } else {
            auto existing = std::vector<int64_t>(n_cols - 1, 0); // key |-> entry of all 0s
            agg_map[group_key] = existing;
        }

        for (size_t c = 1; c < n_cols; c++) {
            if (c == 0) continue;
            agg_map[group_key][c - 1] = agg_map[group_key][c - 1] + table.get(r, c).GetValue<int64_t>();
        }
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << "Elapsed time: " << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << " ms" << std::endl;

}

void hello_omp() {
    int num_threads = 8;
    omp_set_num_threads(num_threads);
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        int actual_num_threads = omp_get_num_threads();
        printf("hello from thread %d among %d threads\n", tid, actual_num_threads);
    }
}

int main(int argc, char *argv[]) {
    
    // 1 > parse command line
    
    // make a ExpConfig TODO parse from cli instead
    ExpConfig config;
    config.num_threads = 8;
    config.strategy = Strategy::SEQUENTIAL;
    config.in_db_file_path = "data/tpch-sf1.db";
    config.in_table_name = "lineitem";
    config.group_key_col_name = "l_orderkey";
    config.data_col_names = {"l_partkey", "l_suppkey"};
    config.agg_funcs = {AggFunc::SUM, AggFunc::SUM};
    config.display();
    
    // 2 > load the data
    DataStore table;
    load_data(config, table);
    
    // 3 > run the experiment
    if (config.strategy == Strategy::SEQUENTIAL) {
        sequential_sol(config, table);
    } else if (config.strategy == Strategy::TWO_PHASE_CENTRALIZED_MERGE) {
        hello_omp();
    } else {
        throw std::runtime_error("Unsupported strategy");
    }
    
    return 0;
}