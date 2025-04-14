#include <chrono>
#include <fstream>
#include <iostream>
#include <duckdb.hpp>

struct ValueHash
{
    std::size_t operator()(const duckdb::Value& value) const
    {
        return value.Hash();
    }
};

int main()
{
    std::string tname = "lineitem";
    std::string fpath = "data/tpch-sf1.db";
    std::vector<std::string> keys = {"l_orderkey"};
    std::vector<std::string> vals = {"l_partkey", "l_suppkey"};
    std::vector<std::string> funs = {"SUM", "SUM"};

    /**
     * For the purpose of illustration, let's support SUM only.
     */
    for (auto& f : funs)
    {
        if (f != "SUM")
        {
            throw std::runtime_error("Unsupported aggregate function: expected SUM, but got " + f);
        }
    }

    duckdb::DuckDB db(fpath);
    duckdb::Connection con(db);

    /**
     * Determine the columns required for execution.
     */
    std::vector<duckdb::idx_t> cols;
    auto table_description = con.TableInfo(tname);
    for (duckdb::idx_t i = 0; i < table_description->columns.size(); i++)
    {
        auto& column_description = table_description->columns[i];

        auto name = column_description.GetName();
        if ((std::find(keys.begin(), keys.end(), name) == keys.end()) && (std::find(vals.begin(), vals.end(), name) == vals.end()))
        {
            continue;
        }
        auto type = column_description.GetType();
        if (type.id() != duckdb::LogicalTypeId::BIGINT && type.id() != duckdb::LogicalTypeId::DECIMAL)
        {
            throw std::runtime_error("Unsupported column type: expected BIGINT, but got " + type.ToString());
        }

        cols.push_back(i);
    }

    /**
     * Populate the data in row-major format with limited columns.
     */
    std::vector<std::vector<duckdb::Value>> data;
    auto result = con.Query("SELECT * FROM " + tname);
    data.reserve(result->RowCount());

    while (auto chunk = result->Fetch())
    {
        for (duckdb::idx_t i = 0; i < chunk->size(); i++)
        {
            std::vector<duckdb::Value> temp;
            for (duckdb::idx_t j : cols)
            {
                temp.push_back(chunk->GetValue(j, i));
            }
            data.push_back(temp);
        }
    }

    auto t0 = std::chrono::high_resolution_clock::now();

    /**
     * Run the single-threaded version of groupby.
     */
    std::unordered_map<duckdb::Value, std::vector<duckdb::Value>, ValueHash> record;
    for (auto& row : data)
    {
        /**
         * Lookup the previous value.
         */
        std::vector<duckdb::Value> prev;
        if (auto search = record.find(row[0]); search != record.end())
        {
            prev = search->second;
        }
        else
        {
            prev = std::vector<duckdb::Value>(vals.size(), duckdb::Value(0));
        }
        /**
         * Do the aggregation. WARNING: SUM for now.
         */
        for (duckdb::idx_t i = 0; i < vals.size(); i++)
        {
            prev[i] = duckdb::Value(prev[i].GetValue<int64_t>() + row[1 + i].GetValue<int64_t>());
        }
        record[row[0]] = prev;
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << "Elapsed time: " << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << " ms" << std::endl;

    /**
     * Save to disk in CSV format for a quick sanity check.
     */
    std::ofstream outfile("out1.csv");

    if (!outfile) {
        std::cerr << "Error opening file for writing!" << std::endl;
        return 1;
    }

    for (const auto& [key, values] : record)
    {
        outfile << key.GetValue<int64_t>();

        for (duckdb::idx_t i = 0; i < values.size(); i++)
        {
            outfile << "," << values[i].GetValue<int64_t>();
        }

        outfile << "\n";
    }

    outfile.close();

    return 0;
}
