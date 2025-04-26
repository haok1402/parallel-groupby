#pragma once

#include "../lib.hpp"

void single_thread_sol(ExpConfig &config, RowStore &table, int trial_idx, std::vector<AggResRow> &agg_res);
void two_phase_centralised_merge_sol(ExpConfig &config, RowStore &table, int trial_idx, std::vector<AggResRow> &agg_res);