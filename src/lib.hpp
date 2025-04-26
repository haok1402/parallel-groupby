#pragma once

//! Shared library for all other things

#include <duckdb.hpp>
#include <iostream>
#include <omp.h>
#include <string>

typedef std::chrono::time_point<std::chrono::steady_clock> chrono_time_point;
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