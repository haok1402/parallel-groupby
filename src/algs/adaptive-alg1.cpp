#include "../lib.hpp"
#include "_all_algs.hpp"

const int sample_prefix_len = 10000;

enum class StratEnum {
    RADIX,
    TREE,
    CENTRAL,
    LOCKFREE,
};


// phase 0: do sampling and decide on strategy
// phase 1: each thread does local aggregation
// phase 2: threads go merge
void adaptive_alg1_sol(ExpConfig &config, RowStore &table, int trial_idx, bool do_print_stats, std::vector<AggResRow> &agg_res) {
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
    
    
    t_agg_0 = std::chrono::steady_clock::now();
    t_phase0_0 = std::chrono::steady_clock::now();

    auto sample_phase_agg_map = XXHashAggMap();
    auto hasher = I64Hasher{};
    
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
    
    // do decision tree
    
    std::cout << "g_tilde = " << g_tilde << std::endl;
    std::cout << "G_hat = " << G_hat << std::endl;
    std::cout << "config.num_threads = " << config.num_threads << std::endl;
    
    if (G_hat < 500000 && config.num_threads < 32) {
        if (config.num_threads <= 4) {
            // use centralized merge
            std::cout << ">> strat-decided=centralized-merge" << std::endl;
            strat_decision = StratEnum::CENTRAL;
        } else {
            // use tree merge
            std::cout << ">> strat-decided=tree-merge" << std::endl;
            strat_decision = StratEnum::TREE;
        }
    } else {
        if (config.num_threads < 16 && (100 * g_tilde < 95 * sample_prefix_len)) { // to be safe, we can only be confident about potential htable size if the prefix isn't saturated by new keys
            // use lock free
            std::cout << ">> strat-decided=lock-free" << std::endl;
            strat_decision = StratEnum::LOCKFREE;
        } else {
            // use two phase radix
            std::cout << ">> strat-decided=two-phase-radix" << std::endl;
            strat_decision = StratEnum::RADIX;
        }
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
            for (auto& [group_key, agg_acc] : agg_map) {
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
    } else if (strat_decision == StratEnum::LOCKFREE) {

        t_agg_0 = std::chrono::steady_clock::now();
        LockFreeAggMap map(static_cast<size_t>(G_hat) * 4);

        bool htable_overflow = false;
        
        #pragma omp parallel num_threads(config.num_threads)
        {
            #pragma omp for schedule(static)
            for (size_t r = sample_prefix_len; r < n_rows; r++)
            {
                bool succeeded = map.upsert(table.get(r, 0), table.get(r, 1));
                if (!succeeded) {
                    htable_overflow = true;
                }
            }
        }

        if (htable_overflow) {
            std::cout << "un oh... blew up lock free htable... don't know what to do... fall back to two-phase-radix\n" << std::endl;
            two_phase_radix_xxhash_sol(config, table, trial_idx, do_print_stats, agg_res);
            return;
        }

        for (auto& [key, val] : sample_phase_agg_map) {
            map.accumulate_from_accval(key, val);
        }

        t_agg_1 = std::chrono::steady_clock::now();
        time_print("aggregation_time", trial_idx, t_agg_0, t_agg_1, do_print_stats);

        t_output_0 = std::chrono::steady_clock::now();
    
        for (auto& entry : map.data)
        {
            if (entry.key.load() == INT64_MIN) continue;
            agg_res.push_back(AggResRow{entry.key.load(), entry.cnt.load(), entry.sum.load(), entry.min.load(), entry.max.load()});
        }
    
        t_output_1 = std::chrono::steady_clock::now();
        time_print("write_output", trial_idx, t_output_0, t_output_1, do_print_stats);

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
