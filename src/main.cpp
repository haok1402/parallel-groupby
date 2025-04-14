#include <iostream>
#include <duckdb.hpp>

int main()
{
    /**
     * 1. Read the database
     */
    duckdb::DuckDB db("data/tpch-sf1.db");
    duckdb::Connection con(db);

    std::string query = "SELECT l_orderkey, COUNT(*), SUM(l_extendedprice), AVG(l_discount) FROM lineitem GROUP BY l_orderkey";

    auto root = con.ExtractPlan(query);
    std::cout << root->ToString() << std::endl;

    return 0;
}
