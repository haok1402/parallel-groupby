#include "lib.hpp"
#include <cmath>
#include <duckdb.hpp>
#include <iostream>
#include <omp.h>
#include <string>

void load_data(ExpConfig &config, RowStore &table) {
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

std::unordered_map<int64_t, AggMapValue> load_valiadtion_data(ExpConfig &config) {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    
    std::unordered_map<int64_t, AggMapValue> reference_agg_map;
    
    auto duckdb_res = con.Query("select key, \"count\", \"sum\", \"min\", \"max\" from '" + config.validation_file_path + "'");
    int n_cols = 5;
    
    // go through all chunks
    int r = 0;
    while (auto chunk = duckdb_res->Fetch()) {
        // go through each col i in the chunk
        for (duckdb::idx_t r_in_chunk = 0; r_in_chunk < chunk->size(); r_in_chunk++) {
            for (duckdb::idx_t c = 1; c < n_cols; c++) {
                auto key = chunk->GetValue(0, r_in_chunk).GetValue<int64_t>();
                reference_agg_map[key][c-1] = chunk->GetValue(c, r_in_chunk).GetValue<int64_t>();
            }
            r++;
        }
    }
    return reference_agg_map;
}

void time_print(std::string title, int run_id, chrono_time_point start, chrono_time_point end, bool do_print_stats) {
    if (do_print_stats) {
        std::cout << ">>> run=" << run_id << ", " << title << "=" << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms" << std::endl;
    }
}



// cost estimation stuff

// compute expected number of unique group keys seen if there are G total keys and we take k samples
inline float expected_g(float k, float G) {
    return G * (
        1.0f -
        std::pow((G - 1.0f) / G, k)
    );
}

// estimate the total number of keys G given a sample size k and seeing g_tilde
float estimate_G(float k, float g_tilde) {
    // to avoid numerical issue, clamp g_tilde to k - 1
    g_tilde = std::min(g_tilde, k - 1.0f);
    
    float lo = g_tilde;
    float hi = lo;
    
    // find an upper bound
    while (expected_g(k, hi) < g_tilde) {
        std::cout << "hi = " << hi << std::endl;
        if (hi > 10000000.0f) { // avoid overflow and inifinite loop... sometimes expected_g is not precise enough to find the right hi...
            break;
        }
        hi *= 2.0f;
    }

    const float epsilon = 1.0;
    
    // now we're sure target is between lo and hi
    while (std::abs(hi - lo) > epsilon) {
        float mid = (lo + hi) / 2.0f;
        if (expected_g(k, mid) < g_tilde) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    return lo;
}

// below... require knowing S...

// // estimate central merge cost if there are G keys, we have seen S rows, and we have p processors
// float central_merge_cost_model(float G, int S_int, int p_int) {
//     float groups_per_thread = static_cast<float>(S_int);
//     float p = static_cast<float>(p_int);
//     return 
//         (p - 1.0f) * 
//         G * 
//         (1.0f - std::pow((1.0f - G) / G, groups_per_thread));
// }

// // estimate tree merge cost if there are G keys, we have seen S rows, and we have p processors
// float tree_merge_cost_model(float G, int S_int, int p_int) {
//     float groups_per_thread = static_cast<float>(S_int);
//     const float lambda = 1.2f;
//     float p = static_cast<float>(p_int);
    
//     float sum = 0.0f;
//     for (int l = 0; l < p_int; l *= 2) {
//         sum += (1.0f - std::pow((1.0f - G) / G, groups_per_thread * (0x1 << l)));
//     }
    
//     return 
//         lambda
//         * std::log2(p)
//         * sum;
// }

// // estimate radix merge cost if there are G keys, we have seen S rows, and we have p processors
// float radix_merge_cost_model(float G, int S_int, int p_int) {
//     float groups_per_thread = static_cast<float>(S_int);
//     float p = static_cast<float>(p_int);
//     return 
//         (p - 1.0f) * 
//         G * 
//         (1.0f / p);
// }

// float noradix_scan_cost_model(float G, int S_int, int p_int) {
//     float groups_per_thread = static_cast<float>(S_int);
//     return 1.0f * groups_per_thread + G * log2(G);
// }

// float radix_scan_cost_model(float G, int S_int, int p_int, int num_partitions) {
//     float groups_per_thread = static_cast<float>(S_int);
//     float N = static_cast<float>(num_partitions);
//     return 2.0f * groups_per_thread + G * log2(G / N);
// }
