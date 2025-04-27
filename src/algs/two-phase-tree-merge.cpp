#include "../lib.hpp"

// phase 1: each thread does local aggregation
// phase 2: threads go merge
void two_phase_tree_merge_sol(ExpConfig &config, RowStore &table, int trial_idx, bool do_print_stats, std::vector<AggResRow> &agg_res) {
    omp_set_num_threads(config.num_threads);
    
    auto n_cols = table.n_cols;
    auto n_rows = table.n_rows;

    chrono_time_point t_overall_0;
    chrono_time_point t_overall_1;
    chrono_time_point t_phase1_0;
    chrono_time_point t_phase1_1;
    chrono_time_point t_phase2_0;
    chrono_time_point t_phase2_1;
    chrono_time_point t_agg_0;
    chrono_time_point t_agg_1;
    chrono_time_point t_output_0;
    chrono_time_point t_output_1;
    t_overall_0 = std::chrono::steady_clock::now();
    
    
    t_agg_0 = std::chrono::steady_clock::now();

    auto local_agg_maps = std::vector<XXHashAggMap>(config.num_threads);
    assert(local_agg_maps.size() == config.num_threads);
    XXHashAggMap agg_map; // where merged results go
    
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        int actual_num_threads = omp_get_num_threads();
        assert(actual_num_threads == config.num_threads);
        
        // PHASE 1: local aggregation map
        XXHashAggMap local_agg_map;
        
        if (tid == 0) { t_phase1_0 = std::chrono::steady_clock::now(); }
        
        #pragma omp for schedule(dynamic, config.batch_size)
        for (size_t r = 0; r < n_rows; r++) {
            local_agg_map.accumulate_from_row(table, r);
        }
        local_agg_maps[tid] = local_agg_map;
        
        #pragma omp barrier
        if (tid == 0) {
            t_phase1_1 = std::chrono::steady_clock::now();
            time_print("phase_1", trial_idx, t_phase1_0, t_phase1_1, do_print_stats);
            t_phase2_0 = std::chrono::steady_clock::now();
        }
        
        
        
        // PHASE 2: merges results
        for (int merge_step = 2; merge_step <= actual_num_threads; merge_step *= 2) {
            if (tid % merge_step == 0) {
                int other_tid = tid + (merge_step / 2);
                if (other_tid < actual_num_threads) {
                    local_agg_maps[tid].merge_from(local_agg_maps[other_tid]);
                }
            }
            #pragma omp barrier
        }
        
        if (tid == 0) {
            t_phase2_1 = std::chrono::steady_clock::now();
            time_print("phase_2", trial_idx, t_phase2_0, t_phase2_1, do_print_stats);
        }
    }
    
    
    t_agg_1 = std::chrono::steady_clock::now();
    time_print("aggregation_time", trial_idx, t_agg_0, t_agg_1, do_print_stats);
    
    
    
    // write output to vector
    {
        t_output_0 = std::chrono::steady_clock::now();
        for (auto& [group_key, agg_acc] : agg_map) {
            agg_res.push_back(AggResRow{group_key, agg_acc[0], agg_acc[1], agg_acc[2], agg_acc[3]});
        }
        t_output_1 = std::chrono::steady_clock::now();
        time_print("write_output", trial_idx, t_output_0, t_output_1, do_print_stats);
    }
    
    t_overall_1 = std::chrono::steady_clock::now();
    time_print("elapsed_time", trial_idx, t_overall_0, t_overall_1, do_print_stats);

}
