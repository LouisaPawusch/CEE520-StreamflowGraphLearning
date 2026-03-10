import numpy as np
import os
import pandas as pd

DATA_RAW_DIR = os.path.join(os.path.dirname(__file__), "../../data/raw")
DATA_CLEAN_DIR = os.path.join(os.path.dirname(__file__), "../../data/clean")

HUC = 2040101

df = pd.read_csv(os.path.join(DATA_RAW_DIR, "streamflow_wy2022.csv"))
df_metadata = pd.read_csv(os.path.join(DATA_RAW_DIR, "streamflow_wy2022_metadata.csv"))

df_metadata_cleaned = df_metadata[df_metadata["huc8"] == HUC]
valid_sites = set(df_metadata_cleaned["site_id"])
df_cleaned = df[["date"] + [col for col in df.columns if col != "date" and int(col) in valid_sites]]

df_cleaned.to_csv(os.path.join(DATA_CLEAN_DIR, f"streamflow_wy2022_{HUC}.csv"), index=False)
df_metadata_cleaned.to_csv(os.path.join(DATA_CLEAN_DIR, f"streamflow_wy2022_metadata_{HUC}.csv"), index=False)

expected = 365
for col in df_cleaned.columns[1:]:                                                                         
    n = df_cleaned[col].notna().sum()
    if n < expected:
        print(f"{col}: {n}/{expected} ({expected - n} missing)")