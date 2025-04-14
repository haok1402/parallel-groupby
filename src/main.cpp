#include <iostream>
#include <duckdb.hpp>

int main()
{
    /**
     * 1. Read the database
     */
    duckdb::DuckDB db("data/tpch-sf1.db");
    duckdb::Connection con(db);

    /**
     * 2. Load the columns required for execution
     */
    auto result = con.Query("SELECT * FROM lineitem LIMIT 3;");
    if (result->HasError())
    {
        std::cerr << result->GetError() << std::endl;
        return 1;
    }

    while (auto chunk = result->Fetch())
    {
        std::cout << chunk->ToString() << std::endl;
        break;
    }

    /**
     * 3. Execute the groupby query (i.e. our implementation)
     */

    return 0;
}
