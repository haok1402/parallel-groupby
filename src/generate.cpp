/**
 * @file generate.cpp
 * @brief Generate synthetic dataset with configurable distributions for
 * benchmarking the performance of group-by query execution.
 * @author Hao Kang <haok@andrew.cmu.edu>, Leon Lu <lianglu@andrew.cmu.edu>
 * @date April 19, 2025
 */

#include <regex>

#include <CLI11.hpp>

/**
 * @brief Parse a string representing an integer count with an optional suffix (K, M, B, T).
 *
 * Only supports integer prefixes (e.g., "10K", "5M", "2000"). No decimals allowed.
 * Recognized suffixes:
 * - K (thousand, 1e3)
 * - M (million, 1e6)
 * - B (billion, 1e9)
 * - T (trillion, 1e12)
 *
 * @param s Input string, e.g., "10K", "5M", "2000".
 * @return Parsed count as a size_t.
 */
size_t parse_count(const std::string &s)
{
    size_t num = 0;
    char suffix = '\0';
    std::stringstream ss(s);
    ss >> num >> suffix;

    switch (toupper(suffix))
    {
        case 'K': return num * static_cast<size_t>(1e3);
        case 'M': return num * static_cast<size_t>(1e6);
        case 'B': return num * static_cast<size_t>(1e9);
        case 'T': return num * static_cast<size_t>(1e12);
        default : return num;
    }
}

int main(int argc, char** argv)
{
    /**
     * Parse command line arguments
     */
    CLI::App app
    {
        "Generate synthetic dataset with configurable distributions for "
        "benchmarking the performance of group-by query execution."
    };

    std::string distribution = "uniform";
    app.add_option("--distribution", distribution, "Distribution type: uniform, normal, or exponential")
        ->check(CLI::IsMember({"uniform", "normal", "exponential"}))
        ->default_val("uniform");

    auto valid_count = CLI::Validator(
        [](std::string &input) {
            static const std::regex pattern(R"(^\d+[KMBT]?$)", std::regex::icase);
            return std::regex_match(input, pattern) ? "" : "Invalid format: must be an integer optionally ending in K, M, B, or T (e.g., 100K, 1M)";
        },
        "Integer with optional K/M/B/T suffix",
        "valid_count"
    );

    std::string num_rows_str = "1M";
    app.add_option("--num-rows", num_rows_str, "Number of rows (e.g., 1M, 500K)")
       ->check(valid_count)
       ->default_val("1M");

    std::string num_groups_str = "1K";
    app.add_option("--num-groups", num_groups_str, "Number of groups (e.g., 1K, 100)")
        ->check(valid_count)
        ->default_val("1K");

    CLI11_PARSE(app, argc, argv);

    size_t num_rows = parse_count(num_rows_str);
    size_t num_groups = parse_count(num_groups_str);

    if (num_groups > num_rows)
    {
        std::cerr << "Error: --num-groups cannot be greater than --num-rows." << std::endl;
        return 1;
    }

}
