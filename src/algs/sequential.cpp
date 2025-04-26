//! sequential solution as baseline. no omp parallel section

#include "../lib.hpp"

void sequential_sol(ExpConfig &config, RowStore &table, int trial_idx, std::vector<AggResRow> &agg_res) {
    assert(table.n_rows > 0);
    assert(table.n_cols > 0);
    
    auto n_cols = table.n_cols;
    auto n_rows = table.n_rows;
    assert(n_cols == 2);
    
    chrono_time_point t_overall_0;
    chrono_time_point t_overall_1;
    chrono_time_point t_agg_0;
    chrono_time_point t_agg_1;
    chrono_time_point t_output_0;
    chrono_time_point t_output_1;
    t_overall_0 = std::chrono::steady_clock::now();
    
    
    
    // doing sequential aggregation
    t_agg_0 = std::chrono::steady_clock::now();
    
    SimpleHashAggMap agg_map;
    for (size_t r = 0; r < n_rows; r++) {
        agg_map.aggregate_into(table, r);
    }
    
    t_agg_1 = std::chrono::steady_clock::now();
    time_print("aggregation_time", trial_idx, t_agg_0, t_agg_1);
    
    
    
    // write output to vector
    {
        t_output_0 = std::chrono::steady_clock::now();
        for (auto& [group_key, agg_acc] : agg_map) {
            agg_res.push_back(AggResRow{group_key, agg_acc[0], agg_acc[1], agg_acc[2], agg_acc[3]});
        }
        t_output_1 = std::chrono::steady_clock::now();
        time_print("write_output", trial_idx, t_output_0, t_output_1);
    }

    
    
    t_overall_1 = std::chrono::steady_clock::now();
    time_print("elapsed_time", trial_idx, t_overall_0, t_overall_1);

}
