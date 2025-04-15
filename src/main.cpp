#include <chrono>
#include <duckdb.hpp>
#include <iostream>
#include <omp.h>

struct Entry {
    int64_t l_orderkey;
    int64_t l_partkey;
    int64_t l_suppkey;
};

int main() {
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

    return 0;
}
