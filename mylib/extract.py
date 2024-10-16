"""
Extract a dataset from a URL like Kaggle or data.gov. JSON or CSV formats tend to work well

food dataset
"""

import requests
import os
import pandas as pd


def extract(
    url="https://github.com/fivethirtyeight/data/raw/refs/heads/master/nba-draft-2015/historical_projections.csv",
    file_path="data/nba-draft-2015.csv",
    directory="data",
):
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url, timeout=10) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    df = pd.read_csv(file_path)
    df_subset = df.head(121)
    df_subset.loc[:, "Projected SPM"] = df_subset["Projected SPM"].astype("float64")
    df_subset.loc[:, "Superstar"] = df_subset["Superstar"].astype("float64")
    df_subset.loc[:, "Starter"] = df_subset["Starter"].astype("float64")
    df_subset.loc[:, "Role Player"] = df_subset["Role Player"].astype("float64")
    df_subset.loc[:, "Bust"] = df_subset["Bust"].astype("float64")
    # Ensure columns that should be numeric are converted to numeric types (floats)
    df_subset.to_csv(file_path, index=False)
    return file_path
