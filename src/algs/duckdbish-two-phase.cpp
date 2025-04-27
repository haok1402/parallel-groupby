//! one lock on whole agg hash table

#include "../lib.hpp"

void duckdbish_two_phase_sol(ExpConfig &config, RowStore &table, int trial_idx, bool do_print_stats, std::vector<AggResRow> &agg_res) {
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
    chrono_time_point t_output_0;
    chrono_time_point t_output_1;
    t_overall_0 = std::chrono::steady_clock::now();
    
    
    
    t_agg_0 = std::chrono::steady_clock::now();
    int n_partitions = config.num_threads * config.radix_partition_cnt_ratio;
    
    // radix_partitions being a size n_thread array of n_thread array of local agg maps
    std::vector<std::vector<XXHashAggMap>> radix_partitions_local_maps(n_partitions, std::vector<XXHashAggMap>(config.num_threads));
    // radix_partitions[2][3] is thread 3's result for partition 2
    auto local_agg_maps = std::vector<XXHashAggMap>(config.num_threads);
    assert(local_agg_maps.size() == config.num_threads);
    XXHashAggMap agg_map; // where merged results go if we don't end up partitioning

    std::cout << "n_partitions = " << n_partitions << std::endl;
    std::cout << "done initialising all the partitions" << std::endl;
    
    bool do_partition = false;
    
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        int actual_num_threads = omp_get_num_threads();
        assert(actual_num_threads == config.num_threads);
        bool local_done_partition = false;
        
        
        
        // === PHASE 1: local aggregation map === 
        
        XXHashAggMap local_agg_map;
        
        if (tid == 0) { t_phase1_0 = std::chrono::steady_clock::now(); }
        
        #pragma omp for schedule(dynamic, config.batch_size)
        for (size_t r = 0; r < n_rows; r++) {
            int64_t group_key = table.get(r, 0);
           
            local_agg_map.accumulate_from_row(table, r);
        }
        if (local_agg_map.size() > config.duckdb_style_adaptation_threshold) {
            #pragma omp critical
            {
                std::cout << "switch to partitioning" << std::endl;
                do_partition = true;
            }
            // do partition early
            for (auto& [group_key, agg_acc] : local_agg_map) {
                size_t group_key_hash = std::hash<int64_t>{}(group_key);
                size_t part_idx = group_key_hash % n_partitions;
                radix_partitions_local_maps[part_idx][tid][group_key] = agg_acc;
            }
            local_done_partition = true;
        }
        #pragma omp barrier
        if (do_partition && !local_done_partition) { 
            // some thread told global to partition, but we haven't done so
            for (auto& [group_key, agg_acc] : local_agg_map) {
                size_t group_key_hash = std::hash<int64_t>{}(group_key);
                size_t part_idx = group_key_hash % n_partitions;
                radix_partitions_local_maps[part_idx][tid][group_key] = agg_acc;
            }
        }
        if (!do_partition) {
            local_agg_maps[tid] = local_agg_map;
        }
        #pragma omp barrier
        if (tid == 0) {
            t_phase1_1 = std::chrono::steady_clock::now();
            time_print("phase_1", trial_idx, t_phase1_0, t_phase1_1, do_print_stats);
        }

        
        
        // === PHASE 2: if partitioned, merge within partition in parallel, else do centralized merge === 
        
        if (tid == 0) { t_phase2_0 = std::chrono::steady_clock::now(); }
        
        if (do_partition) {        
            #pragma omp for schedule(dynamic, 1)
            for (size_t part_idx = 0; part_idx < n_partitions; part_idx++) {
                for (size_t other_tid = 1; other_tid < actual_num_threads; other_tid++) {
                    auto other_local_agg_map = radix_partitions_local_maps[part_idx][other_tid];
                    radix_partitions_local_maps[part_idx][0].merge_from(other_local_agg_map);
                }
            }
        } else {
            if (tid == 0) {                
                agg_map = std::move(local_agg_maps[0]);
                
                for (int other_tid = 1; other_tid < actual_num_threads; other_tid++) {
                    auto other_local_agg_map = local_agg_maps[other_tid];
                    agg_map.merge_from(other_local_agg_map);
                }    
            }
        }
        
        if (tid == 0) {
            t_phase2_1 = std::chrono::steady_clock::now();
            time_print("phase_2", trial_idx, t_phase2_0, t_phase2_1, do_print_stats);
        }

    }
    
    t_agg_1 = std::chrono::steady_clock::now();
    time_print("aggregation_time", trial_idx, t_agg_0, t_agg_1, do_print_stats);
    
    // === one thread write out the result === 
    {
        t_output_0 = std::chrono::steady_clock::now();
        
        if (do_partition) {
            for (size_t part_idx = 0; part_idx < n_partitions; part_idx++) {
                // agg_map.merge(radix_partitions_local_maps[part_idx][0]);
                for (auto& [group_key, agg_acc] : radix_partitions_local_maps[part_idx][0]) {
                    agg_res.push_back(AggResRow{group_key, agg_acc[0], agg_acc[1], agg_acc[2], agg_acc[3]});
                }
            }
        } else {
            for (auto& [group_key, agg_acc] : agg_map) {
                agg_res.push_back(AggResRow{group_key, agg_acc[0], agg_acc[1], agg_acc[2], agg_acc[3]});
            }
        }
        
        t_output_1 = std::chrono::steady_clock::now();
        time_print("write_output", trial_idx, t_output_0, t_output_1, do_print_stats);
    }
    
    t_overall_1 = std::chrono::steady_clock::now();
    time_print("elapsed_time", trial_idx, t_overall_0, t_overall_1, do_print_stats);
}
