/**
 * @file lock-free-hash-table.cpp
 * @brief Lock-free concurrent group-by aggregation using a linear probing hash map with atomic upserts.
 * @author Hao Kang <haok@andrew.cmu.edu>, Leon Lu <lianglu@andrew.cmu.edu>
 * @date April 23, 2025
 */

#include <thread>
#include <atomic>
#include <cstdint>
#include <vector>
#include <optional>

#include "../lib.hpp"

void lock_free_hash_table_sol(ExpConfig &config, RowStore &table, int trial_idx, bool do_print_stats, std::vector<AggResRow> &agg_res)
{
    chrono_time_point t_overall_0;
    chrono_time_point t_overall_1;
    chrono_time_point t_aggregate_0;
    chrono_time_point t_aggregate_1;
    chrono_time_point t_output_0;
    chrono_time_point t_output_1;

    t_overall_0 = std::chrono::steady_clock::now();

    t_aggregate_0 = std::chrono::steady_clock::now();

    auto n_rows = table.n_rows;
    AggMap map(n_rows);

    int num_threads = config.num_threads;

    std::vector<std::thread> threads;

    for (int t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([&, t]() {
            for (size_t r = t; r < n_rows; r += num_threads)
            {
                map.upsert(table.get(r, 0), table.get(r, 1));
            }
        });
    }

    for (auto& th : threads)
    {
        th.join();
    }

    t_aggregate_1 = std::chrono::steady_clock::now();
    time_print("aggregation_time", trial_idx, t_aggregate_0, t_aggregate_1, do_print_stats);

    t_output_0 = std::chrono::steady_clock::now();

    for (auto& entry : map.data)
    {
        agg_res.push_back(AggResRow{entry.key.load(), entry.cnt.load(), entry.sum.load(), entry.min.load(), entry.max.load()});
    }

    t_output_1 = std::chrono::steady_clock::now();
    time_print("write_output", trial_idx, t_output_0, t_output_1, do_print_stats);

    t_overall_1 = std::chrono::steady_clock::now();
    time_print("elapsed_time", trial_idx, t_overall_0, t_overall_1, do_print_stats);
}
