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

    auto table_description = con.TableInfo(tname);
    for (auto& column_description : table_description->columns)
    {
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
        std::cout << name << std::endl;
    }

    std::vector<std::vector<duckdb::Value>> data;
    auto result = con.Query("SELECT * FROM " + tname);
    data.reserve(result->RowCount());

    return 0;
}
