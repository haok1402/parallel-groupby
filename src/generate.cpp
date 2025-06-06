/**
 * @file generate.cpp
 * @brief Generate synthetic dataset with configurable distributions for
 * benchmarking the performance of group-by query execution.
 * @author Hao Kang <haok@andrew.cmu.edu>, Leon Lu <lianglu@andrew.cmu.edu>
 * @date April 19, 2025
 */

 #include <regex>
 #include <random>
 
 #include <omp.h>
 #include <zlib.h>
 #include <CLI11.hpp>
 #include <indicators.hpp>
 
 const int SEED = 42;
 
 /**
  * @brief Flush the cached output to a gzip file safely.
  *
  * Writes the stringstream (oss) to the gzip file (file) inside an OpenMP critical section,
  * updates the progress bar (bar), then clears the cache (oss) and resets cache_size.
  *
  * @param file Gzip file handle.
  * @param oss  Output stringstream holding buffered data.
  * @param bar  Progress bar to update.
  * @param p    Progress counter (number of rows written).
  * @param cache_size Number of rows currently in cache.
  */
 #define FLUSH_CACHE(file, oss, bar, p, cache_size)     \
     _Pragma("omp critical")                            \
     {                                                  \
         std::string buffer = (oss).str();              \
         gzwrite((file), buffer.data(), buffer.size()); \
         (bar).set_progress((p) += (cache_size));       \
     }                                                  \
     (oss).str(std::string());                          \
     (oss).clear();                                     \
     (cache_size) = 0;                                  \
 
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
     app.add_option("--distribution", distribution, "Distribution type: uniform, normal, biuniform, exponential, or adversarial")
         ->check(CLI::IsMember({"uniform", "normal", "exponential", "biuniform", "adversarial"}))
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
 
     /**
      * Construct output file path
      */
     std::ostringstream oss;
 
     oss << "data" << "/" << distribution;
     std::filesystem::create_directories(oss.str());
 
     oss << "/" << num_rows_str << "-" << num_groups_str << ".csv.gz";
     std::string path = oss.str();
 
     /**
      * Display the configuration
      */
     std::cout << std::left;
     std::cout << std::setw(30) << "Distribution type"         << ": " << distribution     << "\n";
     std::cout << std::setw(30) << "Total rows to generate"    << ": " << num_rows_str     << "\n";
     std::cout << std::setw(30) << "Number of distinct groups" << ": " << num_groups_str   << "\n";
     std::cout << std::setw(30) << "Output file path"          << ": " << path             << "\n";
 
     /**
      * Setup the progress bar
      */
     size_t p = 0;
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
 
     /**
      * Open the gzip file for writing
      */
     gzFile file = gzopen(path.c_str(), "wb");
     if (file == nullptr)
     {
         std::cerr << "Error: Failed to open " << path << " for gzip writing.\n";
         return 1;
     }
 
     /**
      * Write CSV header
      */
     gzprintf(file, "key,val\n");
 
     /**
      * Run the sampling in parallel
      */
     size_t batch_size = 1024 * 1024;
     omp_set_num_threads(4);
 
     if (distribution == "uniform")
     {
         #pragma omp parallel
         {
             std::mt19937 gen(SEED + omp_get_thread_num());
             std::uniform_int_distribution<int64_t> key_distribution(0, num_groups - 1);
             std::uniform_int_distribution<int16_t> val_distribution(0, std::numeric_limits<int16_t>::max());
             std::ostringstream oss;
             size_t cache_size = 0;
 
             // Remaining rows follow specified distribution
             #pragma omp for
             for (size_t i = 0; i < num_rows - num_groups; ++i)
             {
                 int64_t key = key_distribution(gen);
                 int16_t val = val_distribution(gen);
                 oss << key << "," << val << "\n";
                 if (++cache_size >= batch_size) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
             }
             if (cache_size > 0) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
 
             // Ensure each group has at least one row
             #pragma omp for
             for (size_t i = 0; i < num_groups; i++)
             {
                 int64_t key = i;
                 int16_t val = val_distribution(gen);
                 oss << key << "," << val << "\n";
                 if (++cache_size >= batch_size) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
             }
             if (cache_size > 0) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
 
         }
     }
     else if (distribution == "normal")
     {
         #pragma omp parallel
         {
             std::mt19937 gen(SEED + omp_get_thread_num());
             std::normal_distribution<> key_distribution(static_cast<double>(num_groups) / 2.0, static_cast<double>(num_groups) / 8.0);
             std::uniform_int_distribution<int16_t> val_distribution(0, std::numeric_limits<int16_t>::max());
             std::ostringstream oss;
             size_t cache_size = 0;
 
             // Remaining rows follow specified distribution
             #pragma omp for
             for (size_t i = 0; i < num_rows - num_groups; ++i)
             {
                 size_t key;
                 do { key = static_cast<size_t>(std::llround(key_distribution(gen))); } while (key >= num_groups); 
                 int16_t val = val_distribution(gen);
                 oss << key << "," << val << "\n";
                 if (++cache_size >= batch_size) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
             }
             if (cache_size > 0) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
             
             // Ensure each group has at least one row
             #pragma omp for
             for (size_t i = 0; i < num_groups; i++)
             {
                 int64_t key = i;
                 int16_t val = val_distribution(gen);
                 oss << key << "," << val << "\n";
                 if (++cache_size >= batch_size) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
             }
             if (cache_size > 0) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
         }
     }
     else if (distribution == "exponential")
     {
         #pragma omp parallel
         {
             std::mt19937 gen(SEED + omp_get_thread_num());
             std::exponential_distribution<> key_distribution(1.0 / (0.3 * num_groups));
             std::uniform_int_distribution<int16_t> val_distribution(0, std::numeric_limits<int16_t>::max());
             std::ostringstream oss;
             size_t cache_size = 0;
 
             // Remaining rows follow specified distribution
             #pragma omp for
             for (size_t i = 0; i < num_rows - num_groups; ++i)
             {
                 size_t key;
                 do { key = static_cast<size_t>(key_distribution(gen)); } while (key >= num_groups);
                 int16_t val = val_distribution(gen);
                 oss << key << "," << val << "\n";
                 if (++cache_size >= batch_size) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
             }
             if (cache_size > 0) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
             
             // Ensure each group has at least one row
             #pragma omp for
             for (size_t i = 0; i < num_groups; i++)
             {
                 size_t key = i;
                 int16_t val = val_distribution(gen);
                 oss << key << "," << val << "\n";
                 if (++cache_size >= batch_size) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
             }
             if (cache_size > 0) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
 
         }
     }
     else if (distribution == "biuniform")
     {
         #pragma omp parallel
         {
             const int n_dist2_groups = 100;
             std::mt19937 gen(SEED + omp_get_thread_num());
             std::uniform_int_distribution<> key_distribution1(0, num_groups - 1);
             std::uniform_int_distribution<> key_distribution2(0, n_dist2_groups); // the small number of keys that get sampled very often
             std::uniform_int_distribution<> coin_flip(0, 1); // decide which dist to draw from
             std::uniform_int_distribution<int16_t> val_distribution(0, std::numeric_limits<int16_t>::max());
             std::ostringstream oss;
             size_t cache_size = 0;
 
             // Remaining rows follow specified distribution
             #pragma omp for
             for (size_t i = 0; i < num_rows - num_groups; ++i)
             {
                 int64_t coin = coin_flip(gen);
                 int64_t key;
                 if (coin == 1) {
                     key = key_distribution1(gen);
                 } else {
                     key = key_distribution2(gen) * num_groups / n_dist2_groups;
                 }
                 int16_t val = val_distribution(gen);
                 oss << key << "," << val << "\n";
                 if (++cache_size >= batch_size) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
             }
             if (cache_size > 0) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
             
             // Ensure each group has at least one row
             #pragma omp for
             for (size_t i = 0; i < num_groups; i++)
             {
                 size_t key = i;
                 int16_t val = val_distribution(gen);
                 oss << key << "," << val << "\n";
                 if (++cache_size >= batch_size) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
             }
             if (cache_size > 0) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
 
         }
     }
     else if (distribution == "adversarial")
     {
         /**
          *  0M-20M: ENSURED (0-20M)
          * 20M-40M: UNIFORM (0-200K)
          * 40M-60M: UNIFORM (0-20K)
          * 60M-80M: UNIFROM (0-2K)
          */
         #pragma omp parallel
         {
             std::mt19937 gen(SEED + omp_get_thread_num());
             std::uniform_int_distribution<int16_t> val_distribution(0, std::numeric_limits<int16_t>::max());
             std::ostringstream oss;
             size_t cache_size = 0;
 
             // 0M-20M: ENSURED (0-20M)
             std::vector<int64_t> vec(20000000);
             std::iota(vec.begin(), vec.end(), 1);
             std::shuffle(vec.begin(), vec.end(), gen);
 
             #pragma omp for
             for (size_t i = 0; i < vec.size(); i++)
             {
                 size_t key = vec[i];
                 int16_t val = val_distribution(gen);
                 oss << key << "," << val << "\n";
                 if (++cache_size >= batch_size) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
             }
             if (cache_size > 0) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
 
             // 20M-40M: UNIFORM (0-200K)
             std::uniform_int_distribution<int64_t> key_distribution1(0, 20);
             #pragma omp for
             for (size_t i = 0; i < 20000000; ++i)
             {
                 int64_t key = key_distribution1(gen);
                 int16_t val = val_distribution(gen);
                 oss << key << "," << val << "\n";
                 if (++cache_size >= batch_size) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
             }
             if (cache_size > 0) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
 
             // 40M-60M: UNIFORM (0-20K)
             std::uniform_int_distribution<int64_t> key_distribution2(0, 2000000);
             #pragma omp for
             for (size_t i = 0; i < 20000000; ++i)
             {
                 int64_t key = key_distribution2(gen);
                 int16_t val = val_distribution(gen);
                 oss << key << "," << val << "\n";
                 if (++cache_size >= batch_size) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
             }
             if (cache_size > 0) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
 
             // 60M-80M: UNIFROM (0-2K)
             std::uniform_int_distribution<int64_t> key_distribution3(0, 20);
             #pragma omp for
             for (size_t i = 0; i < 20000000; ++i)
             {
                 int64_t key = key_distribution3(gen);
                 int16_t val = val_distribution(gen);
                 oss << key << "," << val << "\n";
                 if (++cache_size >= batch_size) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
             }
             if (cache_size > 0) { FLUSH_CACHE(file, oss, bar, p, cache_size); }
         }
     }
 
     /**
      * Close the gzip file
      */
     gzclose(file);
 }