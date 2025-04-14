#include <iostream>
#include <duckdb.hpp>

int main()
{
    std::string tname = "lineitem";
    std::string fpath = "data/tpch-sf1.db";
    std::vector<std::string> keys = {"l_orderkey"};
    std::vector<std::string> vals = {"l_extendedprice", "l_discount"};
    std::vector<std::string> funs = {"SUM", "AVG"};

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
            throw std::runtime_error("Unsupported column type: expected BIGINT or DECIMAL, but got " + type.ToString());
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

    return 0;
}
