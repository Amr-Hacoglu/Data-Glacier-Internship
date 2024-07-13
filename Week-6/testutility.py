import logging
import os
import subprocess
import yaml
import pandas as pd
import datetime 
import gc
import re
import dask.dataframe as dd
import modin.pandas as mpd
import ray
import time

# Initialize Ray
ray.init(ignore_reinit_error=True)

################
# File Reading #
################

def read_config_file(filepath):
    with open(filepath, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(exc)

def replacer(string, char):
    pattern = char + '{2,}'
    string = re.sub(pattern, char, string) 
    return string

def col_header_val(df, table_config):
    '''
    replace whitespaces in the column
    and standardized column names
    '''
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace('[^\w]','_',regex=True)
    df.columns = list(map(lambda x: x.strip('_'), list(df.columns)))
    df.columns = list(map(lambda x: replacer(x,'_'), list(df.columns)))
    expected_col = list(map(lambda x: x.lower(),  table_config['columns']))
    expected_col.sort()
    df.columns =list(map(lambda x: x.lower(), list(df.columns)))
    df = df.reindex(sorted(df.columns), axis=1)
    if len(df.columns) == len(expected_col) and list(expected_col)  == list(df.columns):
        print("column name and column length validation passed")
        return 1
    else:
        print("column name and column length validation failed")
        mismatched_columns_file = list(set(df.columns).difference(expected_col))
        print("Following File columns are not in the YAML file",mismatched_columns_file)
        missing_YAML_file = list(set(expected_col).difference(df.columns))
        print("Following YAML columns are not in the file uploaded",missing_YAML_file)
        logging.info(f'df columns: {df.columns}')
        logging.info(f'expected columns: {expected_col}')
        return 0

def read_file_with_pandas(file_path):
    start_time = time.time()
    df = pd.read_csv(file_path)
    read_time = time.time() - start_time
    return df, read_time

def read_file_with_dask(file_path):
    start_time = time.time()
    df = dd.read_csv(file_path)
    read_time = time.time() - start_time
    return df, read_time

def read_file_with_modin(file_path):
    start_time = time.time()
    df = mpd.read_csv(file_path)
    read_time = time.time() - start_time
    return df, read_time

def read_file_with_ray(file_path):
    start_time = time.time()
    df = ray.data.read_csv(file_path)
    read_time = time.time() - start_time
    return df, read_time

def write_file_gz(df, output_path, sep):
    df.to_csv(output_path, sep=sep, compression='gzip', index=False)

def summarize_file(file_path):
    file_size = os.path.getsize(file_path)
    df = pd.read_csv(file_path)
    num_rows, num_cols = df.shape
    return num_rows, num_cols, file_size
