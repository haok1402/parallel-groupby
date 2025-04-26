#include <cstdint>

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
