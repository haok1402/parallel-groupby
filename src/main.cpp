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
    auto result = con.Query("SELECT * FROM lineitem;");
    if (result->HasError())
    {
        std::cerr << result->GetError() << std::endl;
        return 1;
    }

    auto collection = result->Collection();

    auto column_types = collection.Types();
    for (duckdb::idx_t i = 0; i < column_types.size(); i++)
    {
        std::cout << "type: " << column_types[i].ToString() << std::endl;
    }

    /**
     * 3. Execute the groupby query (i.e. our implementation)
     */

    return 0;
}
