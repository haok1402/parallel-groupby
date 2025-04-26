#include "../lib.hpp"

void two_phase_centralised_merge_sol(ExpConfig &config, RowStore &table, int trial_idx, std::vector<AggResRow> &agg_res) {
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

    auto local_agg_maps = std::vector<std::unordered_map<int64_t, AggMapValue>>(config.num_threads);
    assert(local_agg_maps.size() == config.num_threads);
    std::unordered_map<int64_t, AggMapValue> agg_map; // where merged results go
    
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        int actual_num_threads = omp_get_num_threads();
        // printf("hello from thread %d among %d threads\n", tid, actual_num_threads);
        assert(actual_num_threads == config.num_threads);
        
        // PHASE 1: local aggregation map
        std::unordered_map<int64_t, AggMapValue> local_agg_map;
        
        if (tid == 0) { t_phase1_0 = std::chrono::steady_clock::now(); }
        
        // #pragma omp for schedule(dynamic, config.batch_size)
        #pragma omp for schedule(dynamic, config.batch_size)
        for (size_t r = 0; r < n_rows; r++) {
            auto group_key = table.get(r, 0);
            AggMapValue agg_acc;
            if (auto search = local_agg_map.find(group_key); search != local_agg_map.end()) {
                agg_acc = search->second;
            } else {
                agg_acc = AggMapValue{0, 0};
            }
    
            for (size_t c = 1; c < n_cols; c++) {
                agg_acc[c - 1] = agg_acc[c - 1] + table.get(r, c);
            }
            local_agg_map[group_key] = agg_acc;
        }
        local_agg_maps[tid] = local_agg_map;
        
        #pragma omp barrier
        
        if (tid == 0) {
            t_phase1_1 = std::chrono::steady_clock::now();
            time_print("phase_1", trial_idx, t_phase1_0, t_phase1_1);
        }

        // PHASE 2: thread 0 merges results
        if (tid == 0) {
            t_phase2_0 = std::chrono::steady_clock::now();
            
            agg_map = std::move(local_agg_maps[0]);
            
            for (int other_tid = 1; other_tid < actual_num_threads; other_tid++) {
                auto other_local_agg_map = local_agg_maps[other_tid];
                
                for (const auto& [group_key, other_agg_acc] : other_local_agg_map) {
                    AggMapValue agg_acc;
                    if (auto search = agg_map.find(group_key); search != agg_map.end()) {
                        agg_acc = search->second;
                        for (size_t c = 1; c < n_cols; c++) {
                            agg_acc[c - 1] = agg_acc[c - 1] + other_agg_acc[c - 1];
                        }
                    } else {
                        agg_acc = other_agg_acc;
                    }
                    agg_map[group_key] = agg_acc;
                }
            }
            
            t_phase2_1 = std::chrono::steady_clock::now();
            time_print("phase_2", trial_idx, t_phase2_0, t_phase2_1);

        }
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

}
