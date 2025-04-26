#include "lib.hpp"
#include <duckdb.hpp>
#include <iostream>
#include <omp.h>
#include <string>

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

void time_print(std::string title, int run_id, chrono_time_point start, chrono_time_point end) {
    std::cout << ">>> run=" << run_id << ", " << title << "=" << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms" << std::endl;
}