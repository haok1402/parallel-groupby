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

enum class Distribution
{
    Uniform,
};

std::ostream& operator<<(std::ostream& os, const Distribution& distribution)
{
    switch (distribution)
    {
        case Distribution::Uniform: return os << "uniform";
    }
    return os;
}

int main(int argc, char **argv)
{
    CLI::App app{
        "Generate synthetic dataset with configurable distributions"
        "for benchmarking the performance of group-by query execution."};

    Distribution distribution = Distribution::Uniform;
    std::map<std::string, Distribution> distribution_map
    {
        {"uniform", Distribution::Uniform},
    };

    app.add_option("-d,--distribution", distribution, "Distribution: uniform, normal, or exponential")
        ->transform(CLI::CheckedTransformer(distribution_map, CLI::ignore_case))
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
