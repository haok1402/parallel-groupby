/**
 * @file two-phase-central-merge.cpp
 * @brief Each thread maintains a local aggregation map which gets merged by a single thread eventually.
 * @author Hao Kang <haok@andrew.cmu.edu>, Leon Lu <lianglu@andrew.cmu.edu>
 * @date April 19, 2025
 */

#include <omp.h>
#include <chrono>

#include <zlib.h>
#include <CLI11.hpp>

void aggregate(std::vector<int64_t>& data, int num_threads)
{
    omp_set_num_threads(num_threads);
    auto states = std::vector<std::unordered_map<int64_t, int64_t>>(num_threads);
    std::unordered_map<int64_t, int64_t> merged;

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        std::unordered_map<int64_t, int64_t>& map = states[tid];

        #pragma omp for schedule(static)
        for (size_t i = 0; i < data.size(); i += 2)
        {
            int64_t key = data[i], val = data[i + 1];
            map[key] += val;
        }

        #pragma omp barrier
        #pragma omp single
        {
            merged = std::move(states[0]);
            for (int i = 1; i < num_threads; ++i)
            {
                for (auto& [key, val] : states[i])
                {
                    merged[key] += val;
                }
            }
        }
    }
}

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
    std::vector<int64_t> data;

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
            int64_t key = std::stoll(col1);
            data.push_back(key);
            int64_t val = std::stoll(col2);
            data.push_back(val);
        }
    }

    gzclose(gz_input);
    data.shrink_to_fit();

    /**
     * Run the warmup steps.
     */
    std::cout << "Running " << warmup_steps << " warm-up iteration(s) to stabilize performance" << std::endl;
    for (int i = 0; i < warmup_steps; i++) { aggregate(data, num_threads); }

    /**
     * Run the measure steps.
     */
    std::cout << "Running " << measure_steps << " evaluation iteration(s) for benchmarking" << std::endl;
    auto t0 = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < measure_steps; i++) { aggregate(data, num_threads); }
    auto t1 = std::chrono::high_resolution_clock::now();

    /**
     * Report the average timing across all steps.
     */
    std::chrono::duration<double, std::milli> elapsed = (t1 - t0) / measure_steps;
    std::cout << std::fixed << std::setprecision(8)
              << "Benchmark completed: Elapsed " << elapsed.count() / 1000.0
              << "s" << std::endl;
}
