package main

// reference https://duckdb.org/docs/stable/clients/go.html
import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

type row struct {
	gk int64 // group key
	v1 int64
	v2 int64
}

type agg_entry struct {
	v1sum int64
	v2sum int64
}

func main() {
    
    var in_file = "../data/tpch-sf1.db"
    fmt.Printf("input file is %s\n", in_file)
    db, _ := sql.Open("duckdb", in_file)
    duckdb_rows, _ := db.Query("select l_orderkey, l_partkey, l_suppkey from lineitem") // gk, v1, v2

    // var data = []row{
    //     {1, 2, 1},
    //     {2, 4, 4},
    //     {1, 6, 3},
    // }
    
    var data []row
    for duckdb_rows.Next() {
        var r row
        duckdb_rows.Scan(&r.gk, &r.v1, &r.v2)
        data = append(data, r)
    }
    
    duckdb_rows.Close()
    db.Close()
    
    fmt.Printf("got %d rows\n", len(data))
    fmt.Printf("starting aggregation\n")
    
    var t_start = time.Now()
    
    var m = make(map[int64]agg_entry)
    
    for _, r := range data {
        entry, ok := m[r.gk]
        if !ok {
            entry = agg_entry{}
        }
        entry.v1sum += r.v1
        entry.v2sum += r.v2
        m[r.gk] = entry
    }
    
    var t_end = time.Now()
    
    // fmt.Println("map:", m)
    var t_duration = t_end.Sub(t_start)
    fmt.Printf("duration is %s\n", t_duration)
    
}