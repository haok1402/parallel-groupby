// approach: every thread scan whole input table and only aggregate stuff that end up on their partition
// use num num partitions = num threads

#include "../lib.hpp"

void implicit_repartitioning_sol(ExpConfig &config, RowStore &table, int trial_idx, bool do_print_stats, std::vector<AggResRow> &agg_res) {
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
    auto local_agg_maps = std::vector<SimpleHashAggMap>(config.num_threads);
    assert(local_agg_maps.size() == config.num_threads);
    
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        int actual_num_threads = omp_get_num_threads();
        assert(actual_num_threads == config.num_threads);
        
        SimpleHashAggMap local_agg_map;
        
        for (size_t r = 0; r < n_rows; r++) {
            auto group_key = table.get(r, 0);
            
            size_t group_key_hash = std::hash<int64_t>{}(group_key);
            size_t part_idx = group_key_hash % config.num_threads;
            if (part_idx != tid) { continue; }
            
            local_agg_map.accumulate_from_row(table, r);
        }
        local_agg_maps[tid] = local_agg_map;
        
        #pragma omp barrier
    }
    
    t_agg_1 = std::chrono::steady_clock::now();
    time_print("aggregation_time", trial_idx, t_agg_0, t_agg_1, do_print_stats);

    
    
    // write output to vector
    {
        t_output_0 = std::chrono::steady_clock::now();
        for (size_t part_idx = 0; part_idx < config.num_threads; part_idx++) {
            for (auto& [group_key, agg_acc] : local_agg_maps[part_idx]) {
                agg_res.push_back(AggResRow{group_key, agg_acc[0], agg_acc[1], agg_acc[2], agg_acc[3]});
            }
        }
        t_output_1 = std::chrono::steady_clock::now();
        time_print("write_output", trial_idx, t_output_0, t_output_1, do_print_stats);
    }
    
    t_overall_1 = std::chrono::steady_clock::now();
    time_print("elapsed_time", trial_idx, t_overall_0, t_overall_1, do_print_stats);

}

