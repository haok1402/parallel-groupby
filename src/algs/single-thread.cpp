#include "../lib.hpp"

void single_thread_sol(ExpConfig &config, RowStore &table, int trial_idx, std::vector<AggResRow> &agg_res) {
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
