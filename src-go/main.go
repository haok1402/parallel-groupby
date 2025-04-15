package main

import (
    "fmt"
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
    
    var data = []row{
        {1, 2, 1},
        {2, 4, 4},
        {1, 6, 3},
    }
    
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
    
    fmt.Println("map:", m)
    
}