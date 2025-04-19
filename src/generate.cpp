/**
 * @file generate.cpp
 * @brief Generate synthetic dataset for group-by query execution.
 * @author Hao Kang, Leon Lu
 * @date April 19, 2025
 */

#include <random>
#include <string>
#include <sstream>
#include <fstream>
#include <filesystem>

#include <CLI11.hpp>
#include <duckdb.hpp>

int main(int argc, char **argv)
{
    // Parse command line arguments.
    CLI::App app{
        "Generate synthetic dataset with configurable distributions"
        "for benchmarking the performance of group-by query execution."};

    std::string distribution = "uniform";
    app.add_option("-d,--distribution", distribution, "Distribution: uniform, normal, or exponential")
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

    std::cout << std::left;
    std::cout << std::setw(30) << "Distribution type"         << ": " << distribution << std::endl;
    std::cout << std::setw(30) << "Total rows to generate"    << ": " << num_rows     << std::endl;
    std::cout << std::setw(30) << "Number of distinct groups" << ": " << num_groups   << std::endl;

    // Specify where to store the dataset.
    std::ostringstream oss;
    std::filesystem::create_directory("data/");
    oss << "data/" << distribution << "-" << num_rows << "-" << num_groups << ".db";

    // Connect to DuckDB.
    duckdb::DuckDB db(oss.str());
    duckdb::Connection con(db);

    // Create the table.
    auto result = con.Query("CREATE TABLE main (key BIGINT, val SMALLINT)");
    if (result->HasError())
    {
        std::cerr << result->GetError() << std::endl;
        return 1;
    }

    // Seed the random number generator.
    std::random_device rd;
    std::mt19937 gen(rd());

    // Run the insertion.
    if (distribution == "uniform")
    {
        std::uniform_int_distribution<int64_t> key_distribution(0, num_groups);
        std::uniform_int_distribution<int16_t> val_distribution(0, std::numeric_limits<int16_t>::max());
    }
}
