#include "../lib.hpp"

const int sample_prefix_len = 10000;

enum class StratEnum {
    RADIX,
    TREE,
    CENTRAL,
};

// estimate central merge cost if there are G keys, we have seen S rows, and we have p processors
float central_merge_cost_model(float G, int p_int) {
    float p = static_cast<float>(p_int);
    return (p - 1.0f) * G;
}

// estimate tree merge cost if there are G keys, we have seen S rows, and we have p processors
float tree_merge_cost_model(float G, int p_int) {
    const float lambda = 1.1f;
    float p = static_cast<float>(p_int);
    
    return 
        lambda
        * std::log2(p)
        * G;
}

// estimate radix merge cost if there are G keys, we have seen S rows, and we have p processors
float radix_merge_cost_model(float G, int p_int) {
    float p = static_cast<float>(p_int);
    return 
        (p - 1.0f) * 
        G * 
        (1.0f / p);
}

// because there is no way of knowing how many new rows to expect, alg2 will unfortunately guess S = 8 G
float noradix_scan_cost_model(float G, int p_int) {
    return 8.0f * 1.0f * G + G * log2(G);
}

float radix_scan_cost_model(float G, int p_int, int num_partitions) {
    float N = static_cast<float>(num_partitions);
    return 8.0f * 2.0f * G + G * log2(G / N);
}



// phase 0: do sampling and decide on strategy
// phase 1: each thread does local aggregation
// phase 2: threads go merge
void adaptive_alg2_sol(ExpConfig &config, RowStore &table, int trial_idx, bool do_print_stats, std::vector<AggResRow> &agg_res) {
    omp_set_num_threads(config.num_threads);

    auto n_cols = table.n_cols;
    auto n_rows = table.n_rows;

    chrono_time_point t_overall_0;
    chrono_time_point t_overall_1;
    chrono_time_point t_phase0_0;
    chrono_time_point t_phase0_1;
    chrono_time_point t_phase1_0;
    chrono_time_point t_phase1_1;
    chrono_time_point t_phase2_0;
    chrono_time_point t_phase2_1;
    chrono_time_point t_agg_0;
    chrono_time_point t_agg_1;
    chrono_time_point t_output_0;
    chrono_time_point t_output_1;
    t_overall_0 = std::chrono::steady_clock::now();
    auto hasher = I64Hasher{};
    
    t_agg_0 = std::chrono::steady_clock::now();
    t_phase0_0 = std::chrono::steady_clock::now();

    auto sample_phase_agg_map = XXHashAggMap();
    
    // === PHASE 0: do sampling ===
    
    int n_sampled_row = std::min(sample_prefix_len, n_rows);
    for (size_t r = 0; r < n_sampled_row; r++) {
        sample_phase_agg_map.accumulate_from_row(table, r);
    }
    
    // float group_to_row_ratio = static_cast<float>(sample_phase_agg_map.size()) / static_cast<float>(n_sampled_row);
    float g_tilde = static_cast<float>(sample_phase_agg_map.size());
    float G_hat = estimate_G(static_cast<float>(n_sampled_row), g_tilde);
    int G_hat_int = static_cast<int>(G_hat);
    StratEnum strat_decision;
    
    // apply cost models
    
    std::cout << "g_tilde = " << g_tilde << std::endl;
    std::cout << "G_hat = " << G_hat << std::endl;
    std::cout << "config.num_threads = " << config.num_threads << std::endl;
    
    
    float central_merge_cost = central_merge_cost_model(G_hat, config.num_threads);
    float tree_merge_cost = tree_merge_cost_model(G_hat, config.num_threads);
    float radix_merge_cost = radix_merge_cost_model(G_hat, config.num_threads);
    float noradix_scan_cost = noradix_scan_cost_model(G_hat, config.num_threads);
    float radix_scan_cost = radix_scan_cost_model(G_hat, config.num_threads, config.num_threads * config.radix_partition_cnt_ratio);
    
    std::cout << "central_merge_cost = " << central_merge_cost << std::endl;
    std::cout << "tree_merge_cost = " << tree_merge_cost << std::endl;
    std::cout << "radix_merge_cost = " << radix_merge_cost << std::endl;
    std::cout << "noradix_scan_cost = " << noradix_scan_cost << std::endl;
    std::cout << "radix_scan_cost = " << radix_scan_cost << std::endl;
    
    float two_phase_central_cost = central_merge_cost + noradix_scan_cost;
    float two_phase_radix_cost = radix_merge_cost + radix_scan_cost;
    float two_phase_tree_cost = tree_merge_cost + noradix_scan_cost;
    
    std::cout << "two_phase_central_cost = " << two_phase_central_cost << std::endl;
    std::cout << "two_phase_tree_cost = " << two_phase_tree_cost << std::endl;
    std::cout << "two_phase_radix_cost = " << two_phase_radix_cost << std::endl;
    
    float min_strat_cost = std::min(two_phase_central_cost, std::min(two_phase_radix_cost, two_phase_tree_cost));

    if (min_strat_cost == two_phase_central_cost) {
        std::cout << ">> strat-decided=centralized-merge" << std::endl;
        strat_decision = StratEnum::CENTRAL;
    } else if (min_strat_cost == two_phase_tree_cost) {
        std::cout << ">> strat-decided=tree-merge" << std::endl;
        strat_decision = StratEnum::TREE;
    } else if (min_strat_cost == two_phase_radix_cost) {
        std::cout << ">> strat-decided=two-phase-radix" << std::endl;
        strat_decision = StratEnum::RADIX;
    } else {
        throw std::runtime_error("unreachable");
    }

    t_phase0_1 = std::chrono::steady_clock::now();
    time_print("phase_0", trial_idx, t_phase0_0, t_phase0_1, do_print_stats);
    
        

    // Use the decided strategy
    
    if (strat_decision == StratEnum::CENTRAL) {
        // data structure if we were to do local hmap based things
        auto local_agg_maps = std::vector<XXHashAggMap>(config.num_threads);
        assert(local_agg_maps.size() == config.num_threads);
        XXHashAggMap agg_map; // where merged results go
        // agg_map.reserve(G_hat_int);
        
        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            int actual_num_threads = omp_get_num_threads();
            assert(actual_num_threads == config.num_threads);
            
            // PHASE 1: local aggregation map
            XXHashAggMap local_agg_map;
            // local_agg_map.reserve(G_hat_int);
            
            if (tid == 0) { t_phase1_0 = std::chrono::steady_clock::now(); }
            
            #pragma omp for schedule(dynamic, config.batch_size)
            for (size_t r = n_sampled_row; r < n_rows; r++) {
                local_agg_map.accumulate_from_row(table, r);
            }
            local_agg_maps[tid] = local_agg_map;
            
            #pragma omp barrier
            if (tid == 0) {
                t_phase1_1 = std::chrono::steady_clock::now();
                time_print("phase_1", trial_idx, t_phase1_0, t_phase1_1, do_print_stats);
            }
            
            // PHASE 2: thread 0 merges results
            if (tid == 0) {
                t_phase2_0 = std::chrono::steady_clock::now();
                
                agg_map = std::move(local_agg_maps[0]);
                
                for (int other_tid = 1; other_tid < actual_num_threads; other_tid++) {
                    agg_map.merge_from(local_agg_maps[other_tid]);
                }
                
                t_phase2_1 = std::chrono::steady_clock::now();
                time_print("phase_2", trial_idx, t_phase2_0, t_phase2_1, do_print_stats);
            }
        }
        
        // merge results from sampled agg table into main results
        agg_map.merge_from(sample_phase_agg_map);
        
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
        
    } else if (strat_decision == StratEnum::TREE) {
        // data structure if we were to do local hmap based things
        auto local_agg_maps = std::vector<XXHashAggMap>(config.num_threads);
        assert(local_agg_maps.size() == config.num_threads);
        XXHashAggMap agg_map; // where merged results go        
        // agg_map.reserve(G_hat_int);
        
        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            int actual_num_threads = omp_get_num_threads();
            assert(actual_num_threads == config.num_threads);
            
            // PHASE 1: local aggregation map
            XXHashAggMap local_agg_map;
            // local_agg_map.reserve(G_hat_int);
            
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
            for (auto& [group_key, agg_acc] : local_agg_maps[0]) {
                agg_res.push_back(AggResRow{group_key, agg_acc[0], agg_acc[1], agg_acc[2], agg_acc[3]});
            }
            t_output_1 = std::chrono::steady_clock::now();
            time_print("write_output", trial_idx, t_output_0, t_output_1, do_print_stats);
        }
    } else if (strat_decision == StratEnum::RADIX) {
        // if we were to do radix
        int n_partitions = config.num_threads * config.radix_partition_cnt_ratio;
        std::vector<std::vector<XXHashAggMap>> radix_partitions_local_maps(n_partitions, std::vector<XXHashAggMap>(config.num_threads));    

        
        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            int actual_num_threads = omp_get_num_threads();
            assert(actual_num_threads == config.num_threads);
            
            // === PHASE 1: aggregate into partition and local aggregation map === 
            
            std::vector<XXHashAggMap> local_radix_partitions(n_partitions);
            for (size_t i = 0; i < n_partitions; i++) {
                local_radix_partitions[i] = XXHashAggMap();
                // local_radix_partitions[i].reserve(G_hat_int);
            }
            
            if (tid == 0) { t_phase1_0 = std::chrono::steady_clock::now(); }
            
            #pragma omp for schedule(dynamic, config.batch_size)
            for (size_t r = 0; r < n_rows; r++) {
                int64_t group_key = table.get(r, 0);
                size_t group_key_hash = std::hash<int64_t>{}(group_key);
                size_t part_idx = group_key_hash % n_partitions;
                
                local_radix_partitions[part_idx].accumulate_from_row(table, r);
            }
            for (size_t part_idx = 0; part_idx < n_partitions; part_idx++) {
                radix_partitions_local_maps[part_idx][tid] = local_radix_partitions[part_idx];
            }
            #pragma omp barrier
            
            if (tid == 0) {
                t_phase1_1 = std::chrono::steady_clock::now();
                time_print("phase_1", trial_idx, t_phase1_0, t_phase1_1, do_print_stats);
            }
            
            
            
            // === PHASE 2: merge within partition, in parallel === 
    
            if (tid == 0) {
                t_phase2_0 = std::chrono::steady_clock::now();
            }
    
            #pragma omp for schedule(dynamic, 1)
            for (size_t part_idx = 0; part_idx < n_partitions; part_idx++) {
                for (size_t other_tid = 1; other_tid < actual_num_threads; other_tid++) {
                    auto other_local_agg_map = radix_partitions_local_maps[part_idx][other_tid];
                    radix_partitions_local_maps[part_idx][0].merge_from(other_local_agg_map);
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
            
            for (size_t part_idx = 0; part_idx < n_partitions; part_idx++) {
                for (auto& [group_key, agg_acc] : radix_partitions_local_maps[part_idx][0]) {
                    agg_res.push_back(AggResRow{group_key, agg_acc[0], agg_acc[1], agg_acc[2], agg_acc[3]});
                }
            }
            
            t_output_1 = std::chrono::steady_clock::now();
            time_print("write_output", trial_idx, t_output_0, t_output_1, do_print_stats);
        }
    } else {
        throw std::runtime_error("unreachable");
    }

    t_overall_1 = std::chrono::steady_clock::now();
    time_print("elapsed_time", trial_idx, t_overall_0, t_overall_1, do_print_stats);

    
    
    // write output to vector
    // {
    //     t_output_0 = std::chrono::steady_clock::now();
    //     for (auto& [group_key, agg_acc] : agg_map) {
    //         agg_res.push_back(AggResRow{group_key, agg_acc[0], agg_acc[1], agg_acc[2], agg_acc[3]});
    //     }
    //     t_output_1 = std::chrono::steady_clock::now();
    //     time_print("write_output", trial_idx, t_output_0, t_output_1, do_print_stats);
    // }
    
    t_overall_1 = std::chrono::steady_clock::now();
    time_print("elapsed_time", trial_idx, t_overall_0, t_overall_1, do_print_stats);

}
