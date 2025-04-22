#include <CLI11.hpp>
#include <duckdb.hpp>

int main(int argc, char** argv)
{
    // Parse command line arguments
    CLI::App app
    {
        "Run the group-by query using the two-phase central merge algorithm. "
        "This program benchmarks its performance and reports detailed timing results."
    };

    std::string read_from;
    app.add_option("--read-from", read_from)
        ->check(CLI::ExistingFile)
        ->required();

    int num_threads;
    app.add_option("--num-threads", num_threads)
        ->required();

    int warmup_steps = 3;
    app.add_option("--warmup-steps", warmup_steps)
        ->default_val(3);

    int measure_steps = 5;
    app.add_option("--measure-steps", measure_steps)
        ->default_val(5);

    CLI11_PARSE(app, argc, argv);

    std::cout << std::left;
    std::cout << std::setw(30) << "Dataset"         << ": " << read_from     << "\n";
    std::cout << std::setw(30) << "Threads"         << ": " << num_threads   << "\n";
    std::cout << std::setw(30) << "Warmup Steps"    << ": " << warmup_steps  << "\n";
    std::cout << std::setw(30) << "Measure Steps"   << ": " << measure_steps << "\n";

    // Read the dataset.
}
