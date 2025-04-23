#include <thread>
#include <atomic>
#include <cstdint>
#include <vector>
#include <optional>

#include <zlib.h>
#include <CLI11.hpp>

struct SumAggEntry
{
    std::atomic<int64_t> key;
    std::atomic<int64_t> val;
    SumAggEntry() : key(INT64_MIN), val(0) {}
};

class SumAggMap
{
    public:
        explicit SumAggMap(size_t n)
            : size(n), data(n) {}

        bool upsert(int64_t k, int64_t d)
        {
            size_t i = std::hash<int64_t>{} (k);
            for (size_t probe = 0; probe < size; probe++)
            {
                size_t j = (i + probe) % size;
                int64_t expected = INT64_MIN;

                if (data[j].key.compare_exchange_strong(expected, k))
                {
                    data[j].val.fetch_add(d);
                    return true;
                }
                if (expected == k)
                {
                    data[j].val.fetch_add(d);
                    return true;
                }
            }
            return false;
        }

        std::optional<int64_t> lookup(int64_t k)
        {
            size_t i = std::hash<int64_t>{} (k);
            for (size_t probe = 0; probe < size; probe++)
            {
                size_t j = (i + probe) % size;
                int64_t stored = data[j].key.load();

                if (stored == k)
                {
                    return data[j].val.load();
                }
                if (stored == INT64_MIN)
                {
                    return std::nullopt;
                }
            }
            return std::nullopt;
        }

    private:
        size_t size;
        std::vector<SumAggEntry> data;
};

void worker(std::vector<int64_t>& data, int num_threads, SumAggMap& map, int rank)
{
    size_t n = 0;
    for (size_t i = 0; i < data.size(); i+= 2)
    {
        if (i / 2 % num_threads == static_cast<size_t>(rank))
        {
            n += 1;
            map.upsert(data[i], data[i + 1]);
        }
    }
}

void aggregate(std::vector<int64_t>& data, int num_threads)
{
    SumAggMap map(data.size());
    std::vector<std::thread> pool;

    for (int rank = 0; rank < num_threads; rank++)
    {
        pool.emplace_back([&data, num_threads, &map, rank]() { worker(data, num_threads, map, rank); });
    }
    for (auto& t : pool)
    {
        t.join(); 
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

    std::cout << "Testing with " << num_threads << " thread(s)" << std::endl;

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
