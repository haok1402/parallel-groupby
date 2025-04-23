#include <zlib.h>
#include <CLI11.hpp>

int main(int argc, char** argv)
{
    /**
     * Parse command line arguments.
     */
    CLI::App app
    {
        "Run the group-by benchmark using a two-phase central merge algorithm. "
        "This program loads a gzipped CSV with two integer columns, executes the "
        "algorithm, and reports timing results."
    };

    std::string dataset;
    app.add_option("--dataset", dataset, "Path to the gzipped CSV input file (with two integer columns)")
        ->check(CLI::ExistingFile)
        ->required();

    int num_threads;
    app.add_option("--num-threads", num_threads, "Number of threads to use during execution")
        ->required();

    int warmup_steps = 3;
    app.add_option("--warmup-steps", warmup_steps, "Number of warmup iterations before measurement begins")
        ->default_val(3);

    int measure_steps = 5;
    app.add_option("--measure-steps", measure_steps, "Number of timed iterations to run for benchmarking")
        ->default_val(5);

    CLI11_PARSE(app, argc, argv);

    /**
     * Load dataset into memory.
     */
    std::cout << "Loading table from " << dataset << " into memory" << std::endl;
    gzFile gz_input = gzopen(dataset.c_str(), "rb");
    if (!gz_input) {
        std::cerr << "Error: Failed to open input file: " << dataset << std::endl;
        return 1;
    }

    constexpr size_t buffer_size = 4096;
    char buffer[buffer_size];
    std::string line;
    std::vector<int> data;

    if (gzgets(gz_input, buffer, buffer_size) == nullptr) {
        std::cerr << "Error: File is empty or failed to read header line." << std::endl;
        gzclose(gz_input);
        return 1;
    }

    while (gzgets(gz_input, buffer, buffer_size)) {
        line = buffer;
        std::istringstream iss(line);
        std::string col1, col2;

        if (std::getline(iss, col1, ',') && std::getline(iss, col2)) {
            int key = std::stoi(col1);
            data.push_back(key);
            int val = std::stoi(col2);
            data.push_back(val);
        }
    }

    gzclose(gz_input);

    /**
     * Run the warmup steps.
     */
    std::cout << "Running " << warmup_steps << " warm-up iteration(s) to stabilize performance" << std::endl;
}
