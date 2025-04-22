/**
 * @file generate.cpp
 * @brief Generate synthetic dataset for group-by query execution, compressed with gzip.
 * @author Hao Kang <haok@andrew.cmu.edu>, Leon Lu <lianglu@andrew.cmu.edu>
 * @date April 19, 2025
 */

#include <regex>
#include <random>
#include <string>
#include <sstream>
#include <iomanip>
#include <iostream>
#include <filesystem>

#include <zlib.h>
#include <CLI11.hpp>
#include <indicators.hpp>

size_t parse_count(const std::string &s)
{
    double num;
    char suffix = '\0';
    std::stringstream ss(s);
    ss >> num >> suffix;

    switch (toupper(suffix))
    {
        case 'K': return static_cast<size_t>(num * 1e3);
        case 'M': return static_cast<size_t>(num * 1e6);
        case 'B': return static_cast<size_t>(num * 1e9);
        case 'T': return static_cast<size_t>(num * 1e12);
        default : return static_cast<size_t>(num);
    }
}

int main(int argc, char **argv)
{
    // Parse command line arguments
    CLI::App app{
        "Generate synthetic dataset with configurable distributions "
        "for benchmarking the performance of group-by query execution."};

    std::string distribution = "uniform";
    app.add_option("--distribution", distribution, "Distribution: uniform, normal, or exponential")
        ->check(CLI::IsMember({"uniform", "normal", "exponential"}))
        ->default_val("uniform");

    auto valid_count = CLI::Validator(
        [](std::string &input) {
            static const std::regex pattern(R"(^\d+[KMBT]?$)", std::regex::icase);
            return std::regex_match(input, pattern) ? "" : std::string("Invalid format: must be an integer optionally ending in K, M, B, or T (e.g., 100K, 1M)");
        }, 
        "Must be an integer with optional K/M/B/T suffix", 
        "valid_count"
    );

    std::string num_rows_str = "1M";
    app.add_option("--num-rows", num_rows_str, "Number of rows in the table")
        ->check(valid_count)
        ->default_val("1M");

    std::string num_groups_str = "1K";
    app.add_option("--num-groups", num_groups_str, "Number of groups in the distribution")
        ->check(valid_count)
        ->default_val("1K");

    CLI11_PARSE(app, argc, argv);

    size_t num_rows = parse_count(num_rows_str);
    size_t num_groups = parse_count(num_groups_str);

    if (num_groups > num_rows)
    {
        std::cerr << "Error: --num-groups cannot be greater than --num-rows!" << std::endl;
        return 1;
    }

    std::filesystem::create_directory("data");
    std::ostringstream oss;
    oss << "data/" << distribution << "-" << num_rows_str << "-" << num_groups_str << ".csv.gz";
    std::string path = oss.str();

    std::cout << std::left;
    std::cout << std::setw(30) << "Distribution type"         << ": " << distribution     << "\n";
    std::cout << std::setw(30) << "Total rows to generate"    << ": " << num_rows_str     << "\n";
    std::cout << std::setw(30) << "Number of distinct groups" << ": " << num_groups_str   << "\n";
    std::cout << std::setw(30) << "Output file path"          << ": " << path             << "\n";

    // Prepare for the insertion
    gzFile gz_file = gzopen(path.c_str(), "wb");
    if (!gz_file)
    {
        std::cerr << "Error: Failed to open " << path << " for gzip writing.\n";
        return 1;
    }

    // Seed the random number generator.
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int16_t> val_distribution(0, std::numeric_limits<int16_t>::max());

    // Initialize the progress bar.
    indicators::ProgressBar bar
    {
        indicators::option::BarWidth{50},
        indicators::option::Start{"["},
        indicators::option::Fill{"="},
        indicators::option::Lead{">"},
        indicators::option::Remainder{" "},
        indicators::option::End{"]"},
        indicators::option::MaxProgress{num_rows},
        indicators::option::ShowElapsedTime{true},
        indicators::option::ShowRemainingTime{true},
        indicators::option::PrefixText{"Generating Dataset"},
    };

    // Write CSV header
    gzprintf(gz_file, "key,val\n");

    // Ensure each group has at least one row
    for (size_t i = 0; i < num_groups; ++i)
    {
        gzprintf(gz_file, "%zu,%d\n", i, val_distribution(gen));
        bar.tick();
    }

    // Remaining rows follow specified distribution
    if (distribution == "uniform")
    {
        std::uniform_int_distribution<size_t> key_distribution(0, num_groups - 1);
        for (size_t i = 0; i < num_rows - num_groups; ++i)
        {
            gzprintf
            (
                gz_file,
                "%zu,%d\n",
                key_distribution(gen),
                val_distribution(gen)
            );
            bar.tick();
        }
    }
    else if (distribution == "normal")
    {
        std::normal_distribution<> key_distribution(num_groups / 2.0, num_groups / 5.0);
        for (size_t i = 0; i < num_rows - num_groups; ++i)
        {
            gzprintf
            (
                gz_file, 
                "%zu,%d\n",
                static_cast<size_t>(std::clamp(std::llround(key_distribution(gen)), 0LL, static_cast<long long>(num_groups - 1))), 
                val_distribution(gen)
            );
            bar.tick();
        }
    }
    else if (distribution == "exponential")
    {
        std::exponential_distribution<> key_distribution(num_groups / 5.0);
        for (size_t i = 0; i < num_rows - num_groups; ++i)
        {
            gzprintf
            (
                gz_file,
                "%zu,%d\n",
                std::min(static_cast<size_t>(key_distribution(gen)), num_groups - 1),
                val_distribution(gen)
            );
            bar.tick();
        }
    }

    gzclose(gz_file);
    return 0;
}
