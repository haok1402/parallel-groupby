//! one lock on whole agg hash table

#include "../lib.hpp"

void global_lock_sol(ExpConfig &config, RowStore &table, int trial_idx, bool do_print_stats, std::vector<AggResRow> &agg_res) {
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
    SimpleHashAggMap agg_map;
    
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        int actual_num_threads = omp_get_num_threads();
        assert(actual_num_threads == config.num_threads);
        
        // #pragma omp for schedule(dynamic, config.batch_size)
        #pragma omp for schedule(dynamic, config.batch_size)
        for (size_t r = 0; r < n_rows; r++) {
            #pragma omp critical 
            {
                agg_map.accumulate_from_row(table, r);
            }
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
