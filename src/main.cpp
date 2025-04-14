#include <iostream>
#include <duckdb.hpp>

int main()
{
    duckdb::DuckDB db("data/tpch-sf1.db");
    duckdb::Connection con(db);

    auto result = con.Query("SELECT COUNT(*) FROM lineitem;");
    if (result->HasError())
    {
        std::cerr << result->GetError() << std::endl;
    }
    else
    {
        std::cout << result->ToString() << std::endl;
    }

    return 0;
}
