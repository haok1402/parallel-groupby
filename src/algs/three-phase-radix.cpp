// approach: go directly into radix partition when scanning
// phase 1: independent local scans into single map
// phase 2: partition the local map into radix partitions
// phase 3: assign partitions to threads and merge

#include "../lib.hpp"

void three_phase_radix_sol(ExpConfig &config, RowStore &table, int trial_idx, bool do_print_stats, std::vector<AggResRow> &agg_res) {
    omp_set_num_threads(config.num_threads);
    
    auto n_cols = table.n_cols;
    auto n_rows = table.n_rows;

    chrono_time_point t_overall_0;
    chrono_time_point t_overall_1;
    chrono_time_point t_agg_0;
    chrono_time_point t_agg_1;
    chrono_time_point t_phase1_0;
    chrono_time_point t_phase1_1;
    chrono_time_point t_phase2_0;
    chrono_time_point t_phase2_1;
    chrono_time_point t_phase3_0;
    chrono_time_point t_phase3_1;
    chrono_time_point t_output_0;
    chrono_time_point t_output_1;
    t_overall_0 = std::chrono::steady_clock::now();
    
    
    
    t_agg_0 = std::chrono::steady_clock::now();
    int n_partitions = config.num_threads * config.radix_partition_cnt_ratio;
    std::vector<std::vector<SimpleHashAggMap>> radix_partitions_local_maps(n_partitions, std::vector<SimpleHashAggMap>(config.num_threads));
    // radix_partitions[2][3] is thread 3's result for partition 2
    
    std::cout << "n_partitions = " << n_partitions << std::endl;
    std::cout << "done initialising all the partitions" << std::endl;
    
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        int actual_num_threads = omp_get_num_threads();
        assert(actual_num_threads == config.num_threads);
        
        
        // === PHASE 1: local aggregation map === 
        
        SimpleHashAggMap local_agg_map;
        
        if (tid == 0) { t_phase1_0 = std::chrono::steady_clock::now(); }
        
        // #pragma omp for schedule(dynamic, config.batch_size)
        #pragma omp for schedule(dynamic, config.batch_size)
        for (size_t r = 0; r < n_rows; r++) {
            int64_t group_key = table.get(r, 0);
            local_agg_map.accumulate_from_row(table, r);
        }
        if (tid == 0) {
            t_phase1_1 = std::chrono::steady_clock::now();
            time_print("phase_1", trial_idx, t_phase1_0, t_phase1_1, do_print_stats);
        }
        #pragma omp barrier
        
        
        // === PHASE 2: each thread break their map into partitions === 
        if (tid == 0) { t_phase2_0 = std::chrono::steady_clock::now(); }
        for (auto& [group_key, agg_acc] : local_agg_map) {
            size_t group_key_hash = std::hash<int64_t>{}(group_key);
            size_t part_idx = group_key_hash % n_partitions;
            radix_partitions_local_maps[part_idx][tid][group_key] = agg_acc;
        }
        #pragma omp barrier
        if (tid == 0) {
            t_phase2_1 = std::chrono::steady_clock::now();
            time_print("phase_2", trial_idx, t_phase2_0, t_phase2_1, do_print_stats);
        }


        // === PHASE 3: merge within partition, in parallel === 

        if (tid == 0) { t_phase3_0 = std::chrono::steady_clock::now(); }

        #pragma omp for schedule(dynamic, 1)
        for (size_t part_idx = 0; part_idx < n_partitions; part_idx++) {
            for (size_t other_tid = 1; other_tid < actual_num_threads; other_tid++) {
                auto other_local_agg_map = radix_partitions_local_maps[part_idx][other_tid];
                radix_partitions_local_maps[part_idx][0].merge_from(other_local_agg_map);
            }
        }
        
        if (tid == 0) {
            t_phase3_1 = std::chrono::steady_clock::now();
            time_print("phase_3", trial_idx, t_phase3_0, t_phase3_1, do_print_stats);
        }

    }
    
    t_agg_1 = std::chrono::steady_clock::now();
    time_print("aggregation_time", trial_idx, t_agg_0, t_agg_1, do_print_stats);
    
    // === one thread write out the result === 
    {
        t_output_0 = std::chrono::steady_clock::now();
        
        for (size_t part_idx = 0; part_idx < n_partitions; part_idx++) {
            for (auto& [group_key, agg_acc] : radix_partitions_local_maps[part_idx][0]) {
                agg_res.push_back(AggResRow{group_key, agg_acc[0], agg_acc[1], agg_acc[2], agg_acc[3]});
            }
        }
        
        t_output_1 = std::chrono::steady_clock::now();
        time_print("write_output", trial_idx, t_output_0, t_output_1, do_print_stats);
    }
    
    t_overall_1 = std::chrono::steady_clock::now();
    time_print("elapsed_time", trial_idx, t_overall_0, t_overall_1, do_print_stats);
}
