#![allow(warnings)]
use arrow::compute::kernels::cast_utils::parse_interval_month_day_nano_config;
use duckdb::{params, Connection, Result};

// referenced https://github.com/duckdb/duckdb-rs

// export DYLD_LIBRARY_PATH=/opt/homebrew/lib:$DYLD_LIBRARY_PATH

// In your project, we need to keep the arrow version same as the version used in duckdb.
// Refer to https://github.com/wangfenjin/duckdb-rs/issues/92
// You can either:
use duckdb::arrow::record_batch::RecordBatch;
// Or in your Cargo.toml, use * as the version; features can be toggled according to your needs
// arrow = { version = "*", default-features = false, features = ["prettyprint"] }
// Then you can:
// use arrow::record_batch::RecordBatch;

use duckdb::arrow::util::pretty::print_batches;
use fxhash::FxHashMap;

#[derive(Debug)]
struct Row {
    l_orderkey: i64,
    l_partkey: i64,
    l_suppkey: i64,
}

fn main() -> Result<()> {
    let in_file_path = "../data/tpch-sf1.db";
    println!("in_file_path = {}", in_file_path);
    
    let conn = Connection::open(in_file_path)?;
    
    println!("reading table into memory...");
    
    let mut stmt = conn.prepare("select l_orderkey, l_partkey, l_suppkey from lineitem")?;
    let rows_iter = stmt.query_map([], |row| {
        Ok(Row {
            l_orderkey: row.get(0)?,
            l_partkey: row.get(1)?,
            l_suppkey: row.get(2)?,
        })
    })?;

    let rows: Vec<Row> = rows_iter.map(|row| row.unwrap()).collect();
    
    println!("read table into memory, starting to aggregate");
    
    let t_start = std::time::Instant::now();

    // let mut l_extendedprice_sum_map: FxHashMap<i64, i64> = FxHashMap::default();
    // let mut l_discount_sum_map: FxHashMap<i64, i64> = FxHashMap::default();
    // let mut count_map: FxHashMap<i64, usize> = FxHashMap::default();
    // condense above into single hash map
    let mut combined_map: FxHashMap<i64, (i64, i64, usize, i64)> = FxHashMap::default();
    for row in rows {
        // l_extendedprice_sum_map.entry(row.l_orderkey).and_modify(|x| *x += row.l_extendedprice).or_insert(row.l_extendedprice);
        // l_discount_sum_map.entry(row.l_orderkey).and_modify(|x| *x += row.l_discount).or_insert(row.l_discount);
        // count_map.entry(row.l_orderkey).and_modify(|x| *x += 1).or_insert(1);
        combined_map.entry(row.l_orderkey).and_modify(|x| {
            *x = (x.0 + row.l_partkey, x.1 + row.l_suppkey, x.2 + 1, (x.1 + row.l_suppkey) / ((x.2 + 1) as i64))
        }).or_insert((row.l_partkey, row.l_suppkey, 1, row.l_suppkey));
    }
    // for (k, (l_extendedprice_sum, l_discount, count)) in combined_map.iter() {
    //     l_discount_avg_map.insert(*k, *v / (count_map.get(k).unwrap().clone() as i64));
    // }
    // let mut l_discount_avg_map: FxHashMap<i64, i64> = FxHashMap::default();
    // for (k, (l_extendedprice_sum, l_discount, count)) in combined_map.iter() {
    //     l_discount_avg_map.insert(*k, l_discount / (*count as i64));
    // }
    
    // dbg!(l_discount_avg_map);
    // dbg!(count_map);
    
    let t_end = std::time::Instant::now();
    
    let t_total = t_end - t_start;
    println!("total time {:?}", t_total);

    println!("correctness spot check");
    println!("419 -> {:?}", combined_map.get(&419));
    println!("3488 -> {:?}", combined_map.get(&3488));
    println!("5997667 -> {:?}", combined_map.get(&5997667));
    
    Ok(())
}