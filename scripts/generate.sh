#!/bin/bash

for distribution in normal exponential biuniform uniform; do
    for num_rows in 80M; do
        for num_groups in 20K 200K 2M 20M; do
            ./generate --distribution $distribution --num-rows $num_rows --num-groups $num_groups
        done
    done
done
