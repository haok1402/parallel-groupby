#!/bin/bash

for distribution in normal exponential biuniform uniform; do
    for num_rows in 80M; do
        for num_groups in 20K 200K 2M 20M; do
            ./generate --distribution $distribution --num-rows $num_rows --num-groups $num_groups
            duckdb -c "COPY (select * from (select key, count(val) as 'count', sum(val) as 'sum', min(val) as 'min', max(val) as 'max' from 'data/$distribution/$num_rows-$num_groups.csv.gz' group by key order by key) using sample 100 rows (reservoir, 42)) to 'data/$distribution/val-$num_rows-$num_groups.csv'"
        done
    done
done
