#! given an <exp_id>
#! extract all results from a log directory looking like /logs/<exp_id>/<any machine_id> (e.g. logs/dev0/m1)
#! expect each .log file to have name looking like $dist,$size_config,$algorithm,np$np.log, where size_config = <nrows>-<ngroups>
#! expect the .log files to all contain rows looking like >>> run=<trial_id : int>, <attribute>=<value>
#! write result set to results/<exp_id>.parquet with each row containing exp_id, machine_id, dist, size_config, n_rows, n_groups, algorithm, np, trial_id, attribute, value

import subprocess
import os
from loguru import logger
from rich import inspect as rinspect
import polars as pl
import argparse
import glob

def parse_size_repr(s: str):
    s = s.replace("K", "000").replace("M", "000000").replace("B", "000000000")
    return int(s)

def parse_one_log(args, log_filepath: str):
    
    machine_id = log_filepath.split('/')[-2]
    filestem = log_filepath.split('/')[-1]
    dist, size_config, algorithm, np_repr_dot_log = filestem.split(',')
    np = int(np_repr_dot_log.split('.')[0].replace('np', ''))
    n_rows = parse_size_repr(size_config.split('-')[0])
    n_groups = parse_size_repr(size_config.split('-')[1])
    
    entries = []
    with open(log_filepath, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if not line.startswith('>>>'): continue
            line = line.replace('\n', '').replace('>>>', '').strip()
            
            trial_id = int(line.split('run=')[1].split(',')[0].strip())
            attribute_str = line.split('run=')[1].split(',')[1]
            attribute_name = attribute_str.split('=')[0].strip()
            attribute_value = attribute_str.split('=')[1].strip()
            
            entries.append({
                'exp_id': args.experiment_id,
                'machine_id': machine_id,
                'dist': dist,
                'size_config': size_config,
                'n_rows': n_rows,
                'n_groups': n_groups,
                'algorithm': algorithm,
                'np': np,
                'trial_id': trial_id,
                'attribute': attribute_name,
                'value': attribute_value
            })
    df = pl.DataFrame(entries)
    return df

def main():
    
    parser = argparse.ArgumentParser(description='Extract results from logs')
    parser.add_argument('-eid', '--experiment_id', type=str, required=True)
    args = parser.parse_args()
    rinspect(args,  title="args")
    
    dfs = []
    pattern = f'logs/{args.experiment_id}/*/*.log'
    for log_filepath in glob.glob(pattern):
        logger.info(f'parsing: {log_filepath}')
        parsed_df = parse_one_log(args, log_filepath)
        if (len(parsed_df) == 0):
            logger.warning(f"didn't parse any data from {log_filepath} ... might still be running")
            continue
        dfs.append(parsed_df)
        
    df = pl.concat(dfs)
    df.write_parquet(f'results/{args.experiment_id}.parquet')
    logger.success(f'parsed {len(dfs)}, combined into results/{args.experiment_id}.parquet')
    with pl.Config(tbl_cols=999):
        print(df)

if __name__ == "__main__":
    main()