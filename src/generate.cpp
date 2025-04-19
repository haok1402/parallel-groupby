/**
 * @file generate.cpp
 * @brief Generate synthetic dataset for group-by query execution.
 * @author Hao Kang, Leon Lu
 * @date April 19, 2025
 */

#include <regex>
#include <string>
#include <iostream>
#include <fstream>
#include <CLI11.hpp>

int main(int argc, char **argv)
{
    CLI::App app{
        "Generate synthetic dataset with configurable distributions"
        "for benchmarking the performance of group-by query execution."};

    std::string distribution = "uniform";
    app.add_option("-d,--distribution", distribution, "Distribution: uniform, normal, or exponential")
        ->default_val("uniform");

    size_t num_rows = 1 * 1000 * 1000;
    app.add_option("--num-rows", num_rows, "Number of rows in the table")
        ->default_val("1000000");

    size_t num_groups = 1 * 1000;
    app.add_option("--num-groups", num_groups, "Number of groups in the distribution")
        ->default_val("1000");

    CLI11_PARSE(app, argc, argv);
    std::cout << std::left;
    std::cout << std::setw(30) << "Distribution type"         << ": " << distribution << std::endl;
    std::cout << std::setw(30) << "Total rows to generate"    << ": " << num_rows     << std::endl;
    std::cout << std::setw(30) << "Number of distinct groups" << ": " << num_groups   << std::endl;
}
