#pragma once

//! Shared library for all other things

#include <duckdb.hpp>
#include <iostream>
#include <omp.h>
#include <string>

typedef std::chrono::time_point<std::chrono::steady_clock> chrono_time_point;

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

typedef std::array<int64_t, 4> AggMapValue; // stores count, sum, min, max
typedef std::array<int64_t, 4+1> AggResRow; 


void time_print(std::string title, int run_id, chrono_time_point start, chrono_time_point end);
std::unordered_map<int64_t, AggMapValue> load_valiadtion_data(ExpConfig &config);