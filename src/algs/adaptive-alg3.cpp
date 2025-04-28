#include "../lib.hpp"
#include <cmath>
#include <flat_hash_map.hpp>
#include "xxhash.h"

// TODO don't use a funny number
const int L3Size = 256000000;  // PSC should be this
// const int L3Size = 256000;  // for testing

enum class StratEnum {
    RADIX,
    TREE,
    CENTRAL,
    LOCKFREE,
};

// estimate central merge cost if there are G keys, we have seen S rows, and we have p processors
float central_merge_cost_model(float G, int S_int, int p_int) {
    float groups_per_thread = static_cast<float>(S_int);
    float p = static_cast<float>(p_int);
    return 
        (p - 1.0f) * 
        G * 
        (1.0f - std::pow((1.0f - G) / G, groups_per_thread));
}

// estimate tree merge cost if there are G keys, we have seen S rows, and we have p processors
float tree_merge_cost_model(float G, int S_int, int p_int) {
    float groups_per_thread = static_cast<float>(S_int);
    const float lambda = 1.2f;
    float p = static_cast<float>(p_int);
    
    float sum = 0.0f;
    for (int l = 0; l < p_int; l += 1) {
        sum += (1.0f - std::pow((1.0f - G) / G, groups_per_thread * (0x1 << l)));
    }
    
    return 
        lambda
        * std::log2(p)
        * sum;
}

// estimate radix merge cost if there are G keys, we have seen S rows, and we have p processors
float radix_merge_cost_model(float G, int S_int, int p_int) {
    float groups_per_thread = static_cast<float>(S_int);
    float p = static_cast<float>(p_int);
    return 
        (p - 1.0f) * 
        G * 
        (1.0f / p);
}

float noradix_scan_cost_model(float G, int S_int, int p_int) {
    float groups_per_thread = static_cast<float>(S_int);
    return 1.0f * groups_per_thread + G * log2(G);
}

float radix_scan_cost_model(float G, int S_int, int p_int, int num_partitions) {
    float groups_per_thread = static_cast<float>(S_int);
    float N = static_cast<float>(num_partitions);
    return 2.0f * groups_per_thread + G * log2(G / N);
}


// phase 0: do sampling and decide on strategy
// phase 1: each thread does local aggregation
// phase 2: threads go merge
void adaptive_alg3_sol(ExpConfig &config, RowStore &table, int trial_idx, bool do_print_stats, std::vector<AggResRow> &agg_res) {

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
    
    int B = config.batch_size;
    int S = B;
    int p = config.num_threads;
    int p_hat = std::min(4, p);
    int adaptation_step = 0;
    StratEnum a_hat = StratEnum::CENTRAL;
    
    // === init data structures ===
    bool touched_per_therad_maps = true;
    int num_per_threads_map_used = p_hat;
    bool touched_radix = false;
    bool touched_lock_free = false;
    
    // data structure if we were to do local hmap based things
    auto local_agg_maps = std::vector<XXHashAggMap>(config.num_threads);
    assert(local_agg_maps.size() == config.num_threads);
    XXHashAggMap local_agg_maps_merged; // where merged results go
    
    // if we were to do radix
    int n_partitions = config.num_threads * config.radix_partition_cnt_ratio;
    std::vector<std::vector<XXHashAggMap>> radix_partitions_local_maps(n_partitions, std::vector<XXHashAggMap>(config.num_threads));    
    auto hasher = I64Hasher{};
    
    // if we do lock free hash table later... for now, size = 0
    LockFreeAggMap lock_free_map(0);
    
    
    // === interatively process larger and larger number of rows
    int row_lb = 0;
    int row_ub = S;

    // G sampling
    int g_tilde_sum = 0;
    int n_sampled_row = 0;
    float max_G_hat = 1.0f;
    ska::flat_hash_map<int64_t, int64_t, I64Hasher> g_sample_map;
    
    do {
        std::cout << "adaptation_step = " << adaptation_step << std::endl;
        std::cout << "row_lb = " << row_lb << std::endl;
        std::cout << "row_ub = " << row_ub << std::endl;
        std::cout << "S = " << S << std::endl;
        std::cout << "p_hat = " << p_hat << std::endl;
        
        
        // do scanning using that strategy
        std::cout << "start parallel scan" << std::endl;
        omp_set_num_threads(p_hat);
        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            int actual_num_threads = omp_get_num_threads();
            assert(actual_num_threads == config.num_threads);
            
            #pragma omp for schedule(dynamic, config.batch_size)
            for (size_t r = row_lb; r < std::min(row_ub, n_rows); r++) {
                if (a_hat == StratEnum::CENTRAL) {
                    local_agg_maps[tid].accumulate_from_row(table, r);
                } else if (a_hat == StratEnum::TREE) {
                    local_agg_maps[tid].accumulate_from_row(table, r);
                } else if (a_hat == StratEnum::RADIX) {
                    int64_t group_key = table.get(r, 0);
                    size_t group_key_hash = std::hash<int64_t>{}(group_key);
                    size_t part_idx = group_key_hash % n_partitions;
                    radix_partitions_local_maps[part_idx][tid].accumulate_from_row(table, r);
                } else if (a_hat == StratEnum::LOCKFREE) {
                    bool succeeded = lock_free_map.upsert(table.get(r, 0), table.get(r, 1));
                    assert(succeeded);
                } else {
                    throw std::runtime_error("unreachable");
                }
                if (tid == 0 && r % (128 / p) < 4) { // sample once in a while
                    n_sampled_row += 1;
                    g_sample_map[table.get(r, 0)] = 0;
                }
            }
            
            if (tid == 0) {
                g_tilde_sum = g_sample_map.size();
            }

            // #pragma omp atomic
            // g_tilde_sum += local_g_tilde_sum;
            // #pragma omp atomic
            // n_sampled_row += local_n_sampled_row;
        
            // each thread add the num of group keys they saw to g_tilde_sum
        }
        std::cout << "end" << std::endl;
        
        // maybe we're done, in which case exit
        if (row_ub >= n_rows) {
            break;
        }
        
        // perofrm adaptation
        std::cout << "<sampling> n_sampled_row = " << n_sampled_row << std::endl;
        std::cout << "<sampling> g_tilde_sum = " << g_tilde_sum << std::endl;
        float G_hat = estimate_G(static_cast<float>(n_sampled_row), static_cast<float>(g_tilde_sum));
        max_G_hat = std::max(max_G_hat, G_hat);
        G_hat = max_G_hat;
        std::cout << "<sampling> G_hat = " << G_hat << std::endl;
        // float G_hat = 2000.0f;
        int G_hat_int = static_cast<int>(G_hat);
        
        if ((row_ub >= 10000000 && p * G_hat_int * (5 * 8) >= L3Size && 4 * G_hat_int <= 2 * row_ub) || a_hat == StratEnum::LOCKFREE) {
            std::cout << ">> adaption-step=" << adaptation_step << ", adapt-to=lock-free" << std::endl;
            a_hat = StratEnum::LOCKFREE;
            p_hat = p;
            int want_lock_free_map_size = G_hat_int * 12;
            int acceptable_lock_free_map_size = G_hat_int * 4;
            
            if (lock_free_map.size < acceptable_lock_free_map_size && (!touched_lock_free)) {
                std::cout << "resizing lock free hmap" << std::endl;
                // resize it to want_lock_free_map_size
                LockFreeAggMap new_lock_free_map(want_lock_free_map_size);
                for (auto& entry : lock_free_map.data) {
                    if (entry.key.load() == INT64_MIN) { continue; }
                    new_lock_free_map.accumulate_from_accval_elems(entry.key.load(), entry.cnt.load(), entry.sum.load(), entry.min.load(), entry.max.load());
                }
                lock_free_map = std::move(new_lock_free_map);
            }
            
            touched_lock_free = true;
        } else {
            StratEnum a_best = a_hat;
            int p_best = p;
            float cost_best = MAXFLOAT;

            int p_hat_candidate=p;
            // for (int p_hat_candidate = 1; p_hat_candidate <= p; p *= 2) {
                float central_merge_cost = central_merge_cost_model(G_hat, 2 * S, p_hat_candidate);
                float tree_merge_cost = tree_merge_cost_model(G_hat, 2 * S, p_hat_candidate);
                float radix_merge_cost = radix_merge_cost_model(G_hat, 2 * S, p_hat_candidate);
                float noradix_scan_cost = noradix_scan_cost_model(G_hat, 2 * S, p_hat_candidate);
                float radix_scan_cost = radix_scan_cost_model(G_hat, 2 * S, p_hat_candidate, n_partitions);
                float two_phase_central_cost = central_merge_cost + noradix_scan_cost;
                float two_phase_radix_cost = radix_merge_cost + radix_scan_cost;
                float two_phase_tree_cost = tree_merge_cost + noradix_scan_cost;
                float min_strat_cost = std::min(two_phase_central_cost, std::min(two_phase_radix_cost, two_phase_tree_cost));
                if (min_strat_cost < cost_best) {
                    cost_best = min_strat_cost;
                    p_best = p_hat_candidate;
                    if (min_strat_cost == two_phase_central_cost) {
                        a_best = StratEnum::CENTRAL;
                    } else if (min_strat_cost == two_phase_tree_cost) {
                        a_best = StratEnum::TREE;
                    } else if (min_strat_cost == two_phase_radix_cost) {
                        a_best = StratEnum::RADIX;
                    } else {
                        throw std::runtime_error("unreachable");
                    }
                }
            // }
            a_hat = a_best;
            p_hat = p_best;
            
            if (a_hat == StratEnum::CENTRAL) {
                std::cout << ">> adaption-step=" << adaptation_step << ", adapt-to=centralized-merge" << std::endl;
                std::cout << ">> adaption-step=" << adaptation_step << ", set-p-to=" << p_hat << std::endl;
                num_per_threads_map_used = std::max(num_per_threads_map_used, p_hat);
            } else if (a_hat == StratEnum::TREE) {
                std::cout << ">> adaption-step=" << adaptation_step << ", adapt-to=tree-merge" << std::endl;
                std::cout << ">> adaption-step=" << adaptation_step << ", set-p-to=" << p_hat << std::endl;
                num_per_threads_map_used = std::max(num_per_threads_map_used, p_hat);
            } else if (a_hat == StratEnum::RADIX) {
                std::cout << ">> adaption-step=" << adaptation_step << ", adapt-to=two-phase-radix" << std::endl;
                std::cout << ">> adaption-step=" << adaptation_step << ", set-p-to=" << p_hat << std::endl;
                touched_radix = true;
            } else {
                throw std::runtime_error("unreachable");
            }
        }
        
        row_lb = row_ub;
        S *= 2;
        row_ub = row_lb + S;
        adaptation_step += 1;
    } while (row_lb < n_rows);
    
    // parallel merge radix
    if (touched_radix) {
        #pragma omp parallel
        {
            #pragma omp for schedule(dynamic, 1)
            for (size_t part_idx = 0; part_idx < n_partitions; part_idx++) {
                for (size_t other_tid = 1; other_tid < p; other_tid++) {
                    auto other_local_agg_map = radix_partitions_local_maps[part_idx][other_tid];
                    radix_partitions_local_maps[part_idx][0].merge_from(other_local_agg_map);
                }
            }
        }
        // merged into radix_partitions_local_maps[any part_idx][0]
    }
    
    // merge the local maps
    if (touched_per_therad_maps) {
        if (a_hat == StratEnum::CENTRAL) {
            for (int other_tid = 1; other_tid < num_per_threads_map_used; other_tid++) {
                local_agg_maps[0].merge_from(local_agg_maps[other_tid]);
            }
        } else {
            omp_set_num_threads(p);
            #pragma omp parallel
            {
                int tid = omp_get_thread_num();
                for (int merge_step = 2; merge_step <= num_per_threads_map_used; merge_step *= 2) {
                    if (tid % merge_step == 0) {
                        int other_tid = tid + (merge_step / 2);
                        if (other_tid < p) {
                            local_agg_maps[tid].merge_from(local_agg_maps[other_tid]);
                        }
                    }
                    #pragma omp barrier
                }
            }
        }
        // merged into local_agg_maps[0]
    }
    
    
    // combine results in the multiple data structures
    if (touched_lock_free) {
        // merge everything into lock free...
        if (touched_radix) {
            #pragma omp parallel
            {
                #pragma omp for schedule(dynamic, 1)
                for (size_t part_idx = 0; part_idx < n_partitions; part_idx++) {
                    for (auto& [key, val] : radix_partitions_local_maps[part_idx][0]) {
                        bool succeeded = lock_free_map.accumulate_from_accval(key, val);
                        assert(succeeded);
                    }
                }
            }
        }
        if (touched_per_therad_maps) {
            for (auto& [key, val] : local_agg_maps[0]) {
                bool succeeded = lock_free_map.accumulate_from_accval(key, val);
                assert(succeeded);
            }
        }
        
        std::cout << "results are in lock free hash table with max size " << lock_free_map.size << std::endl; 

    } else if (touched_radix) {
        if (touched_per_therad_maps) {
            // tree merge the 
            
            // merge thread local into radix and done
            for (const auto& [group_key, other_agg_acc] : local_agg_maps[0]) {
                size_t group_key_hash = std::hash<int64_t>{}(group_key);
                size_t part_idx = group_key_hash % n_partitions;
                radix_partitions_local_maps[part_idx][0].accumulate_from_agg_acc(group_key, other_agg_acc);
            }
        }
        std::cout << "result in all radix_partitions_local_maps[any part_idx][0]" << std::endl; 

    } else {
        // result lives in local_agg_maps[0]
        std::cout << "result in local_agg_maps[0] with size " << local_agg_maps[0].size() << std::endl; 
    }
    
    if (touched_lock_free) {
        for (auto& entry : lock_free_map.data) {
            if (entry.key.load() == INT64_MIN) continue;
            agg_res.push_back(AggResRow{entry.key.load(), entry.cnt.load(), entry.sum.load(), entry.min.load(), entry.max.load()});
        }
    } else if (touched_radix) {
        for (size_t part_idx = 0; part_idx < n_partitions; part_idx++) {
            for (auto& [group_key, agg_acc] : radix_partitions_local_maps[part_idx][0]) {
                agg_res.push_back(AggResRow{group_key, agg_acc[0], agg_acc[1], agg_acc[2], agg_acc[3]});
            }
        }
    } else {
        for (auto& [group_key, agg_acc] : local_agg_maps[0]) {
            agg_res.push_back(AggResRow{group_key, agg_acc[0], agg_acc[1], agg_acc[2], agg_acc[3]});
        }
    }
    
    t_agg_1 = std::chrono::steady_clock::now();
    time_print("aggregation_time", trial_idx, t_agg_0, t_agg_1, do_print_stats);
    
    t_overall_1 = std::chrono::steady_clock::now();
    time_print("elapsed_time", trial_idx, t_overall_0, t_overall_1, do_print_stats);
}
