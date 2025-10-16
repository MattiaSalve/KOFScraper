import argparse
import polars as pl
import dask.dataframe as dd
import pandas as pd
import numpy as np
import csv 
from glob import glob
import os
from pathlib import Path
import pyarrow.parquet as pq

def load_parquet(parsed_dirs: Path):
    corrupted = []
    for f in parsed_dirs.glob("*.parquet"):
        try:
            pq.read_table(f).schema 
        except Exception as e:
            print(f"❌ Corrupted:", f.name, "|", e)
            corrupted.append(f)
    
    
    all_files = [f for f in parsed_dirs.glob("*.parquet")]
    files = [f for f in all_files if f not in corrupted]
    
    ddf = dd.read_parquet(files)    
    ddf = ddf.drop_duplicates()
    
    return ddf

def summary(ddf: dd.DataFrame):
    subpages = ddf[["ID", "error"]][ddf["error"].fillna("None") == "None"]["ID"].value_counts().compute()
    level_zero = ddf[ddf['dl_rank'] == 0].compute()
    errors = level_zero["error"].value_counts()
    errors_percent = 1 - (errors["None"] / errors.sum())
    error_per = (errors / errors.sum())
    top_5_errors = errors.sort_values(ascending=False).head(6)
    errorsdf = pd.DataFrame({
        "Errors" : errors,
        "Percent": error_per})
    errorsdf["Percent"] = round(errorsdf["Percent"]*100, 2)
    top_5_errors_perc = errorsdf.sort_values(ascending=False, by = "Errors").head(6)

    n_companies = level_zero["ID"].size
    timestamps = ddf['timestamp'].compute()

    def format_date(x):
        x = x.split(' ')
        x = [x[2], x[1], x[-1], x[-2]]
        return (' ').join(x)
        
    timestamps = pd.to_datetime(timestamps.dropna().apply(format_date))


