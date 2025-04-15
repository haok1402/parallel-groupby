#include "duckdb/common/typedefs.hpp"
#include <chrono>
#include <cstddef>
#include <duckdb.hpp>
#include <fstream>
#include <iostream>

struct ValueHash {
    std::size_t operator()(const duckdb::Value &value) const {
        return value.Hash();
    }
};

int main() {
    std::string tname = "lineitem";
    std::string fpath = "data/tpch-sf1.db";
    std::vector<std::string> keys = {"l_orderkey"};
    std::vector<std::string> vals = {"l_partkey", "l_suppkey"};
    std::vector<std::string> funs = {"SUM", "SUM"};

    /**
    * For the purpose of illustration, let's support SUM only.
    */
    for (auto &f : funs) {
        if (f != "SUM") {
            throw std::runtime_error("Unsupported aggregate function: expected SUM, but got " + f);
        }
    }

    duckdb::DuckDB db(fpath);
    duckdb::Connection con(db);

    /**
    * Determine the columns required for execution.
    */
    std::vector<duckdb::idx_t> cols;
    auto table_description = con.TableInfo(tname);
    for (duckdb::idx_t i = 0; i < table_description->columns.size(); i++) {
        auto &column_description = table_description->columns[i];

        auto name = column_description.GetName();
        if (
            (std::find(keys.begin(), keys.end(), name) == keys.end()) && 
            (std::find(vals.begin(), vals.end(), name) == vals.end())
        ) { continue; }
        
        auto type = column_description.GetType();
        if (type.id() != duckdb::LogicalTypeId::BIGINT && type.id() != duckdb::LogicalTypeId::DECIMAL) {
            throw std::runtime_error("Unsupported column type: expected BIGINT, but got " + type.ToString());
        }
        cols.push_back(i);
    }

    // initialise column major storage
    std::vector<std::vector<duckdb::Value>> data; 
    
    // query to get results
    auto duckdb_res = con.Query("SELECT * FROM " + tname);
    data.reserve(cols.size());
    
    // make empty column
    for (duckdb::idx_t j : cols) {
        data.push_back({});
        data[j].reserve(duckdb_res->RowCount());
    }
    
    // go through all chunks
    while (auto chunk = duckdb_res->Fetch()) {
        // go through each col i in the chunk
        for (duckdb::idx_t i = 0; i < chunk->size(); i++) {
            // std::vector<duckdb::Value> row;
            for (duckdb::idx_t j : cols) {
                // push onto each column
                data[j].push_back(chunk->GetValue(j, i));
            }
        }
    }
    
    auto n_rows = data[0].size();
    std::cout << "data.size() = " << data.size() << std::endl;
    std::cout << "n_rows = " << n_rows << std::endl;

    std::cout << "Starting aggregation" << std::endl;
    auto t0 = std::chrono::high_resolution_clock::now();

    /**
    * Run the single-threaded version of groupby.
    */
    std::unordered_map<duckdb::Value, std::vector<int64_t>, ValueHash> agg_map;
    for (size_t r = 0; r < n_rows; r++) {
        
        auto group_key = data[0][r];
        
        // find existing entry, if not initialise
        std::vector<duckdb::Value> existing;
        if (auto search = agg_map.find(group_key); search != agg_map.end()) {
        } else {
            auto existing = std::vector<int64_t>(vals.size(), 0); // key |-> entry of all 0s
            agg_map[group_key] = existing;
        }
        
        // do the sum
        // for (duckdb::idx_t i = 0; i < vals.size(); i++) {
        //     existing[i] = duckdb::Value(
        //         existing[i].GetValue<int64_t>() + row[1 + i].GetValue<int64_t>()
        //     );
        // }
        for (duckdb::idx_t c : cols) {
            if (c == 0) continue;
            agg_map[group_key][c - 1] = agg_map[group_key][c - 1] + data[c][r].GetValue<int64_t>();
        }
        
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << "Elapsed time: " << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << " ms" << std::endl;


    return 0;
}
