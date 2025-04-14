#include <iostream>
#include <duckdb.hpp>

int main()
{
    duckdb::DuckDB db("data/tpch-sf1.db");
    duckdb::Connection con(db);

    auto result = con.Query("SELECT * FROM lineitem;");
    if (result->HasError())
    {
        std::cerr << result->GetError() << std::endl;
        return 1;
    }

    while (auto chunk = result->Fetch()) {
        std::cout << "chunk->size() = " << chunk->size() << std::endl;
        std::cout << "chunk->ColumnCount() = " << chunk->ColumnCount() << std::endl;
    }

    std::cout << "result->RowCount() = " << result->RowCount() << std::endl;

    return 0;
}
