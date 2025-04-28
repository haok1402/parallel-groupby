#pragma once

//! Shared library for all other things

#include <cstdint>
#include <duckdb.hpp>
#include <iostream>
#include <omp.h>
#include <string>
#include <flat_hash_map.hpp>
#include "xxhash.h"

typedef std::chrono::time_point<std::chrono::steady_clock, std::chrono::steady_clock::duration> chrono_time_point;
typedef std::array<int64_t, 4> AggMapValue; // stores count, sum, min, max
typedef std::array<int64_t, 4+1> AggResRow; 

// column major data storage
// usage: first reserve all memory, then write to each cell
class ColumnStore {
public:
    // std::vector<int64_t> data; 
    int64_t* data; 
    int n_cols;
    int n_rows;
    
    void init_table(int num_cols, int num_rows) {
        n_cols = num_cols;
        n_rows = num_rows;
        // data.resize(num_cols * num_rows);
        data = new int64_t[num_cols * num_rows];
    }
    
    inline int get_idx(int row_idx, int col_idx) {
        return col_idx * n_rows + row_idx;
    }
    
    inline int64_t get(int row_idx, int col_idx) {
        return data[get_idx(row_idx, col_idx)];
    }
    
    inline void write_value(int row_idx, int col_idx, int64_t value) {
        data[get_idx(row_idx, col_idx)] = value;
    }

};

class RowStore {
public:
    // std::vector<int64_t> data;
    int64_t* data; 
    int n_cols;
    int n_rows;
    
    void init_table(int num_cols, int num_rows) {
        n_cols = num_cols;
        n_rows = num_rows;
        // data.resize(num_cols * num_rows);
        data = new int64_t[num_cols * num_rows];
    }
    
    inline int get_idx(int row_idx, int col_idx) {
        return row_idx * n_cols + col_idx;
    }
    
    inline void write_value(int row_idx, int col_idx, int64_t value) {
        data[get_idx(row_idx, col_idx)] = value;
    }
    
    inline int64_t get(int row_idx, int col_idx) {
        return data[get_idx(row_idx, col_idx)];
    }
};

// wrapper around hash map with some useful row-level features
class SimpleHashAggMap {
public:
    std::unordered_map<int64_t, AggMapValue> agg_map;
    
    inline AggMapValue entry_or_default(int64_t group_key) {
        if (auto search = agg_map.find(group_key); search != agg_map.end()) {
            return search->second;
        } else {
            return AggMapValue{0, 0, INT64_MAX, INT64_MIN };
        }
    }
    
    inline AggMapValue& operator[](int64_t group_key) {
        return agg_map[group_key];
    }
    
    inline void accumulate_from_row(RowStore &table, int r) {
        auto group_key = table.get(r, 0);
        
        // find existing entry, if not initialise
        AggMapValue agg_acc = entry_or_default(group_key);

        // do the aggregation
        agg_acc[0] = agg_acc[0] + 1; // count
        agg_acc[1] = agg_acc[1] + table.get(r, 1); // sum
        agg_acc[2] = std::min(agg_acc[2], table.get(r, 1)); // min
        agg_acc[3] = std::max(agg_acc[3], table.get(r, 1)); // max
        
        agg_map[group_key] = agg_acc;
    }
    
    inline void merge_from(const SimpleHashAggMap &other_agg_map) {
        for (const auto& [group_key, other_agg_acc] : other_agg_map) {
            AggMapValue agg_acc = entry_or_default(group_key);
            
            agg_acc[0] = agg_acc[0] + other_agg_acc[0]; // count
            agg_acc[1] = agg_acc[1] + other_agg_acc[1]; // sum
            agg_acc[2] = std::min(agg_acc[2], other_agg_acc[2]); // min
            agg_acc[3] = std::max(agg_acc[3], other_agg_acc[3]); // max
            
            agg_map[group_key] = agg_acc;
        }
    }
    
    // iterator wrapper implementation referenced https://stackoverflow.com/questions/20681150/should-i-write-iterators-for-a-class-that-is-just-a-wrapper-of-a-vector
    typedef typename std::unordered_map<int64_t, AggMapValue>::iterator iterator;
    typedef typename std::unordered_map<int64_t, AggMapValue>::const_iterator const_iterator;
    iterator begin() { return agg_map.begin(); }
    const_iterator begin() const { return agg_map.begin(); }
    const_iterator cbegin() const { return agg_map.cbegin(); }
    iterator end() { return agg_map.end(); }
    const_iterator end() const { return agg_map.end(); }
    const_iterator cend() const { return agg_map.cend(); }
    
    iterator find(int64_t key) {
        return agg_map.find(key);
    }
    
    void display() {
        std::cout << "Map Data:" << std::endl;
        for (const auto& entry : agg_map) {
            std::cout << "\t" << entry.first << " |-> ";
            for (int i = 0; i < 4; i++) {
                std::cout << entry.second[i] << " ";
            }
            std::cout << std::endl;
        }
    }
    
    size_t size() {
        return agg_map.size();
    }
};

struct I64Hasher {
    size_t operator()(int64_t key) const {
        return XXH3_64bits(&key, sizeof(key));
    }
};

class XXHashAggMap {
public:
    ska::flat_hash_map<int64_t, AggMapValue, I64Hasher> agg_map;
    
    inline AggMapValue entry_or_default(int64_t group_key) {
        if (auto search = agg_map.find(group_key); search != agg_map.end()) {
            return search->second;
        } else {
            return AggMapValue{0, 0, INT64_MAX, INT64_MIN };
        }
    }
    
    inline AggMapValue& operator[](int64_t group_key) {
        return agg_map[group_key];
    }
    
    inline void accumulate_from_row(RowStore &table, int r) {
        auto group_key = table.get(r, 0);
        
        // find existing entry, if not initialise
        AggMapValue agg_acc = entry_or_default(group_key);

        // do the aggregation
        agg_acc[0] = agg_acc[0] + 1; // count
        agg_acc[1] = agg_acc[1] + table.get(r, 1); // sum
        agg_acc[2] = std::min(agg_acc[2], table.get(r, 1)); // min
        agg_acc[3] = std::max(agg_acc[3], table.get(r, 1)); // max
        
        agg_map[group_key] = agg_acc;
    }
    
    inline void accumulate_from_agg_acc(int64_t group_key, AggMapValue other_agg_acc) {

        // find existing entry, if not initialise
        AggMapValue agg_acc = entry_or_default(group_key);

        // do the aggregation
        agg_acc[0] = agg_acc[0] + other_agg_acc[0]; // count
        agg_acc[1] = agg_acc[1] + other_agg_acc[1]; // sum
        agg_acc[2] = std::min(agg_acc[2], other_agg_acc[2]); // min
        agg_acc[3] = std::max(agg_acc[3], other_agg_acc[3]); // max
        
        agg_map[group_key] = agg_acc;
    }
    
    inline void merge_from(const XXHashAggMap &other_agg_map) {
        for (const auto& [group_key, other_agg_acc] : other_agg_map) {
            AggMapValue agg_acc = entry_or_default(group_key);
            
            agg_acc[0] = agg_acc[0] + other_agg_acc[0]; // count
            agg_acc[1] = agg_acc[1] + other_agg_acc[1]; // sum
            agg_acc[2] = std::min(agg_acc[2], other_agg_acc[2]); // min
            agg_acc[3] = std::max(agg_acc[3], other_agg_acc[3]); // max
            
            agg_map[group_key] = agg_acc;
        }
    }
    
    // iterator wrapper implementation referenced https://stackoverflow.com/questions/20681150/should-i-write-iterators-for-a-class-that-is-just-a-wrapper-of-a-vector
    typedef typename ska::flat_hash_map<int64_t, AggMapValue, I64Hasher>::iterator iterator;
    typedef typename ska::flat_hash_map<int64_t, AggMapValue, I64Hasher>::const_iterator const_iterator;
    iterator begin() { return agg_map.begin(); }
    const_iterator begin() const { return agg_map.begin(); }
    const_iterator cbegin() const { return agg_map.cbegin(); }
    iterator end() { return agg_map.end(); }
    const_iterator end() const { return agg_map.end(); }
    const_iterator cend() const { return agg_map.cend(); }
    
    iterator find(int64_t key) {
        return agg_map.find(key);
    }
    
    void display() {
        std::cout << "Map Data:" << std::endl;
        for (const auto& entry : agg_map) {
            std::cout << "\t" << entry.first << " |-> ";
            for (int i = 0; i < 4; i++) {
                std::cout << entry.second[i] << " ";
            }
            std::cout << std::endl;
        }
    }
    
    size_t size() {
        return agg_map.size();
    }
    
    void reserve(size_t n) {
        agg_map.reserve(n);
    }
};

// experiment config, including input file, what to group, what to aggregate, etc.
class ExpConfig {
public:
    int num_threads;
    int radix_partition_cnt_ratio;
    int batch_size;
    int duckdb_style_adaptation_threshold;
    std::string algorithm;
    int num_dryruns;
    int num_trials;
    int cardinality_reduction;
    std::string dataset_file_path;
    std::string validation_file_path;
    std::string in_table_name;
    std::string group_key_col_name;
    std::vector<std::string> data_col_names;
    
    void display() {
        std::cout << "exp config:" << std::endl;
        std::cout << "num_threads = " << num_threads << std::endl;
        std::cout << "radix_partition_cnt_ratio = " << radix_partition_cnt_ratio << std::endl;
        std::cout << "batch_size = " << batch_size << std::endl;
        std::cout << "duckdb_style_adaptation_threshold = " << duckdb_style_adaptation_threshold << std::endl;
        std::cout << "algorithm = " << algorithm << std::endl;
        std::cout << "dataset_file_path = " << dataset_file_path << std::endl;
        std::cout << "validation_file_path = " << validation_file_path << std::endl;
        std::cout << "in_table_name = " << in_table_name << std::endl;
        std::cout << "group_key_col_name = " << group_key_col_name << std::endl;
        std::cout << "data_col_names = [";
        for (const auto& col : data_col_names) {
            std::cout << col << ", ";
        }
        std::cout << "]" << std::endl;
    }
};

// using config specification, load stuff into table
// for now, assume one group column, and group key column is not any of the value columns
void load_data(ExpConfig &config, RowStore &table);



void time_print(std::string title, int run_id, chrono_time_point start, chrono_time_point end, bool do_print_stats);
std::unordered_map<int64_t, AggMapValue> load_valiadtion_data(ExpConfig &config);

struct AggEntry
{
    std::atomic<int64_t> key;
    std::atomic<int64_t> cnt;
    std::atomic<int64_t> sum;
    std::atomic<int64_t> min;
    std::atomic<int64_t> max;
    AggEntry() : key(INT64_MIN), cnt(0), sum(0), min(INT64_MAX), max(INT64_MIN) {}
};

struct AggEntrySnapshot
{
    int64_t key;
    int64_t cnt;
    int64_t sum;
    int64_t min;
    int64_t max;
};

class LockFreeAggMap
{
    public:
        explicit LockFreeAggMap(size_t n)
            : size(n), data(n) {}

        inline bool upsert(int64_t k, int64_t v)
        {
            size_t i = std::hash<int64_t>{} (k);
            for (size_t probe = 0; probe < size; probe++)
            {
                size_t j = (i + probe) % size;
                int64_t expected = INT64_MIN;

                if (data[j].key.compare_exchange_strong(expected, k, std::memory_order_acq_rel, std::memory_order_acquire) || expected == k)
                {
                    data[j].cnt.fetch_add(1, std::memory_order_relaxed);
                    data[j].sum.fetch_add(v, std::memory_order_relaxed);
                    int64_t cur_min = data[j].min.load(std::memory_order_relaxed);
                    while (v < cur_min && !data[j].min.compare_exchange_weak(cur_min, v, std::memory_order_relaxed));
                    int64_t cur_max = data[j].max.load(std::memory_order_relaxed);
                    while (v > cur_max && !data[j].max.compare_exchange_weak(cur_max, v, std::memory_order_relaxed));
                    return true;
                }
            }
            return false;
        }
        
        inline bool accumulate_from_accval(int64_t k, AggMapValue val) {
            size_t i = std::hash<int64_t>{} (k);
            for (size_t probe = 0; probe < size; probe++)
            {
                size_t j = (i + probe) % size;
                int64_t expected = INT64_MIN;

                if (data[j].key.compare_exchange_strong(expected, k, std::memory_order_acq_rel, std::memory_order_acquire) || expected == k)
                {
                    data[j].cnt.fetch_add(val[0], std::memory_order_relaxed);
                    data[j].sum.fetch_add(val[1], std::memory_order_relaxed);
                    int64_t cur_min = data[j].min.load(std::memory_order_relaxed);
                    while (val[2] < cur_min && !data[j].min.compare_exchange_weak(cur_min, val[2], std::memory_order_relaxed));
                    int64_t cur_max = data[j].max.load(std::memory_order_relaxed);
                    while (val[3] > cur_max && !data[j].max.compare_exchange_weak(cur_max, val[3], std::memory_order_relaxed));
                    return true;
                }
            }
            return false;
        }

        inline bool accumulate_from_accval_elems(int64_t k, int64_t cnt, int64_t sum, int64_t min, int64_t max) {
            size_t i = std::hash<int64_t>{} (k);
            for (size_t probe = 0; probe < size; probe++)
            {
                size_t j = (i + probe) % size;
                int64_t expected = INT64_MIN;

                if (data[j].key.compare_exchange_strong(expected, k, std::memory_order_acq_rel, std::memory_order_acquire) || expected == k)
                {
                    data[j].cnt.fetch_add(cnt, std::memory_order_relaxed);
                    data[j].sum.fetch_add(sum, std::memory_order_relaxed);
                    int64_t cur_min = data[j].min.load(std::memory_order_relaxed);
                    while (min < cur_min && !data[j].min.compare_exchange_weak(cur_min, min, std::memory_order_relaxed));
                    int64_t cur_max = data[j].max.load(std::memory_order_relaxed);
                    while (max > cur_max && !data[j].max.compare_exchange_weak(cur_max, max, std::memory_order_relaxed));
                    return true;
                }
            }
            return false;
        }

    public:
        size_t size;
        std::vector<AggEntry> data;
};




inline float expected_g(float k, float G);
float estimate_G(float k, float g_tilde);

// float central_merge_cost_model(float G, int S_int, int p_int);
// float tree_merge_cost_model(float G, int S_int, int p_int);
// float radix_merge_cost_model(float G, int S_int, int p_int);
// float noradix_scan_cost_model(float G, int S_int, int p_int);
// float radix_scan_cost_model(float G, int S_int, int p_int, int num_partitions);