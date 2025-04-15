#include "duckdb/common/types/value.hpp"
#include <chrono>
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
    std::string group_key_col_name;
    std::vector<std::string> data_col_names;
    std::vector<AggFunc> agg_funcs; // TODO support diffeerent aggregation functions
    
    void display() {
        std::cout << "exp config:" << std::endl;
        std::cout << "num_threads = " << num_threads << std::endl;
        std::cout << "strategy = " << ((int) strategy) << std::endl;
        std::cout << "in_db_file_path = " << in_db_file_path << std::endl;
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
    
    
};

void sequential_sol(ExpConfig &config) {
    std::string table_name = "lineitem";
    std::string db_fpath = "data/tpch-sf1.db";
    std::vector<std::string> group_key_cols = {"l_orderkey"};
    std::vector<std::string> val_cols = {"l_partkey", "l_suppkey"};

    duckdb::DuckDB db(db_fpath);
    duckdb::Connection con(db);

    /**
     * Populate the data in row-major format with limited columns.
     */
    std::vector<Entry> data;
    auto result = con.Query("SELECT l_orderkey, l_partkey, l_suppkey FROM lineitem");
    data.reserve(result->RowCount());

    while (auto chunk = result->Fetch()) {
        for (duckdb::idx_t i = 0; i < chunk->size(); i++) {
            Entry entry{
                chunk->GetValue(0, i).GetValue<int64_t>(),
                chunk->GetValue(1, i).GetValue<int64_t>(),
                chunk->GetValue(2, i).GetValue<int64_t>(),
            };
            data.push_back(entry);
        }
    }

    auto t0 = std::chrono::high_resolution_clock::now();

    /**
     * Run the single-threaded version of groupby.
     */
    std::unordered_map<int64_t, Entry> agg_map;
    for (auto &row : data) {
        /**
         * Lookup the previous value.
         */
        Entry prev;
        if (auto search = agg_map.find(row.l_orderkey); search != agg_map.end()) {
            prev = search->second;
        }
        /**
         * Do the aggregation. WARNING: SUM for now.
         */
        agg_map[row.l_orderkey] = prev;
        prev.l_orderkey = row.l_orderkey;
        prev.l_partkey += row.l_partkey;
        prev.l_suppkey += row.l_suppkey;
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
    config.in_db_file_path = "data/lineitem.tbl";
    config.group_key_col_name = "l_orderkey";
    config.data_col_names = {"l_partkey", "l_suppkey"};
    config.agg_funcs = {AggFunc::SUM, AggFunc::SUM};
    
    // 2 > load the data
    
    // 3 > run the experiment
    
    return 0;
}