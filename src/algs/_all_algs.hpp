//! one header to rule the algorithms all
//! include any algorithm here with the same signature

#pragma once

#include "../lib.hpp"

void single_thread_sol(ExpConfig &config, RowStore &table, int trial_idx, std::vector<AggResRow> &agg_res);
void two_phase_centralised_merge_sol(ExpConfig &config, RowStore &table, int trial_idx, std::vector<AggResRow> &agg_res);