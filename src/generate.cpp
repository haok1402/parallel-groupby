/**
 * @file generate.cpp
 * @brief Generate synthetic dataset for group-by query execution, compressed with gzip.
 * @author Hao Kang <haok@andrew.cmu.edu>, Leon Lu <lianglu@andrew.cmu.edu>
 * @date April 19, 2025
 */

#include <random>
#include <string>
#include <sstream>
#include <iomanip>
#include <iostream>
#include <filesystem>

#include <zlib.h>
#include <CLI11.hpp>

int main(int argc, char **argv) {
    // Parse command line arguments
    CLI::App app{
        "Generate synthetic dataset with configurable distributions "
        "for benchmarking the performance of group-by query execution."};

    std::string distribution = "uniform";
    app.add_option("--distribution", distribution, "Distribution: uniform, normal, or exponential")
        ->check(CLI::IsMember({"uniform", "normal", "exponential"}))
        ->default_val("uniform");

    size_t num_rows = 1'000'000;
    app.add_option("--num-rows", num_rows, "Number of rows in the table")
        ->default_val("1000000");

    size_t num_groups = 1'000;
    app.add_option("--num-groups", num_groups, "Number of groups in the distribution")
        ->default_val("1000");

    CLI11_PARSE(app, argc, argv);

    if (num_groups > num_rows) {
        std::cerr << "Error: --num-groups cannot be greater than --num-rows!" << std::endl;
        return 1;
    }

    std::filesystem::create_directory("data");
    std::ostringstream oss;
    oss << "data/" << distribution << "-" << num_rows << "-" << num_groups << ".csv.gz";
    std::string path = oss.str();

    std::cout << std::left;
    std::cout << std::setw(30) << "Distribution type"         << ": " << distribution << "\n";
    std::cout << std::setw(30) << "Total rows to generate"    << ": " << num_rows     << "\n";
    std::cout << std::setw(30) << "Number of distinct groups" << ": " << num_groups   << "\n";
    std::cout << std::setw(30) << "Output file path"          << ": " << path         << "\n";

    // Prepare for the insertion
    gzFile gz_file = gzopen(path.c_str(), "wb");
    if (!gz_file) {
        std::cerr << "Error: Failed to open " << path << " for gzip writing.\n";
        return 1;
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int16_t> val_distribution(0, std::numeric_limits<int16_t>::max());

    // Write CSV header
    gzprintf(gz_file, "key,val\n");

    // Ensure each group has at least one row
    for (size_t i = 0; i < num_groups; ++i) {
        gzprintf(gz_file, "%zu,%d\n", i, val_distribution(gen));
    }

    // Remaining rows follow specified distribution
    if (distribution == "uniform") {
        std::uniform_int_distribution<size_t> key_distribution(0, num_groups - 1);
        for (size_t i = 0; i < num_rows - num_groups; ++i) {
            gzprintf(gz_file, "%zu,%d\n", key_distribution(gen), val_distribution(gen));
        }
    } else if (distribution == "normal") {
        double mean = (num_groups - 1) / 2.0;
        double stddev = num_groups / 6.0;
        std::normal_distribution<> key_distribution(mean, stddev);

        for (size_t i = 0; i < num_rows - num_groups; ++i) {
            int key = std::clamp(static_cast<int>(std::round(key_distribution(gen))), 0, static_cast<int>(num_groups - 1));
            gzprintf(gz_file, "%d,%d\n", key, val_distribution(gen));
        }
    } else if (distribution == "exponential") {
        std::exponential_distribution<> key_distribution(1.0 / (num_groups / 3.0));
        for (size_t i = 0; i < num_rows - num_groups; ++i) {
            int key = std::min(static_cast<int>(key_distribution(gen)), static_cast<int>(num_groups - 1));
            gzprintf(gz_file, "%d,%d\n", key, val_distribution(gen));
        }
    }

    gzclose(gz_file);
    return 0;
}
