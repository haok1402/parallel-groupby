/**
 * @file generate.cpp
 * @brief Generate synthetic dataset for group-by query execution.
 * @author Hao Kang <haok@andrew.cmu.edu>, Leon Lu <lianglu@andrew.cmu.edu>
 * @date April 19, 2025
 */

#include <random>
#include <string>
#include <sstream>
#include <fstream>
#include <filesystem>

#include <CLI11.hpp>

int main(int argc, char **argv)
{
    // Parse command line arguments.
    CLI::App app{
        "Generate synthetic dataset with configurable distributions"
        "for benchmarking the performance of group-by query execution."};

    std::string distribution = "uniform";
    app.add_option("--distribution", distribution, "Distribution: uniform, normal, or exponential")
        ->check(CLI::IsMember({"uniform","normal", "exponential"}))
        ->default_val("uniform");

    size_t num_rows = 1 * 1000 * 1000;
    app.add_option("--num-rows", num_rows, "Number of rows in the table")
        ->default_val("1000000");

    size_t num_groups = 1 * 1000;
    app.add_option("--num-groups", num_groups, "Number of groups in the distribution")
        ->default_val("1000");

    CLI11_PARSE(app, argc, argv);

    // Run sanity check.
    if (num_groups > num_rows)
    {
        std::cerr << "Error: --num-groups cannot be greater than --num-rows!" << std::endl;
        return 1;
    }

    // Determine where to store the CSV file for import by DuckDB.
    std::ostringstream oss;
    std::filesystem::create_directory("data/");
    oss << "data/" << distribution << "-" << num_rows << "-" << num_groups << ".csv";
    std::string path = oss.str();

    // Display the arguments for logging purposes.
    std::cout << std::left;
    std::cout << std::setw(30) << "Distribution type"         << ": " << distribution << std::endl;
    std::cout << std::setw(30) << "Total rows to generate"    << ": " << num_rows     << std::endl;
    std::cout << std::setw(30) << "Number of distinct groups" << ": " << num_groups   << std::endl;
    std::cout << std::setw(30) << "Output file path"          << ": " << path   << std::endl;

    // Open the CSV file.
    std::ofstream file(path);
    if (!file.is_open()) {
        std::cerr << "Failed to write to" << path << std::endl;
        return 1;
    }

    // Run the insertion.
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int16_t> val_distribution(0, std::numeric_limits<int16_t>::max());

    // Ensure that each group key has a value.
    file << "key,val" << std::endl;
    for (size_t i = 0; i < num_groups; i++)
    {
        file << i << "," << val_distribution(gen) << std::endl;
    }

    // All other rows receive a random group key according to the distribution.
    if (distribution == "uniform")
    {
        std::uniform_int_distribution<int64_t> key_distribution(0, num_groups - 1);
        for (size_t i = 0; i < num_rows - num_groups; i++)
        {
            file
            << key_distribution(gen)
            << ","
            << val_distribution(gen)
            << std::endl;
        }
    }
    else if (distribution == "normal")
    {
        double mean = (num_groups - 1) / 2.0;
        double stddev = num_groups / 6.0;
        std::normal_distribution<> key_distribution(mean, stddev);
        for (size_t i = 0; i < num_rows - num_groups; i++) {
            file
            << std::clamp(static_cast<int>(std::round(key_distribution(gen))), 0, static_cast<int>(num_groups - 1))
            << ","
            << val_distribution(gen)
            << std::endl;
        }
    }

    file.close();
    return 0;
}
