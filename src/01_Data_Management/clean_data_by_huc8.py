from dotenv import load_dotenv
import numpy as np
import os
import pandas as pd
import hf_hydrodata as hf
import subsettools as st

load_dotenv()

hf.register_api_pin(os.getenv("HF_EMAIL"), os.getenv("HF_PIN"))

HUC_ID = "02040101"
DATA_RAW_DIR = os.path.join(os.path.dirname(__file__), "../../data/raw/")
DATA_CLEAN_DIR = os.path.join(os.path.dirname(__file__), "../../data/clean")

date_start = "2021-10-01"
date_end = "2022-09-30"

####################################
###### Clean streamflow data #######    
####################################

df = pd.read_csv(os.path.join(DATA_RAW_DIR, "streamflow_wy2022.csv"))
df_metadata = pd.read_csv(os.path.join(DATA_RAW_DIR, "streamflow_wy2022_metadata.csv"))

df_metadata_cleaned = df_metadata[df_metadata["huc8"] == int(HUC_ID)]
valid_sites = set(df_metadata_cleaned["site_id"])
df_cleaned = df[["date"] + [col for col in df.columns if col != "date" and int(col) in valid_sites]]

df_cleaned.to_csv(os.path.join(DATA_CLEAN_DIR, f"streamflow_wy2022_{HUC_ID}.csv"), index=False)
df_metadata_cleaned.to_csv(os.path.join(DATA_CLEAN_DIR, f"streamflow_wy2022_metadata_{HUC_ID}.csv"), index=False)

expected = 365
for col in df_cleaned.columns[1:]:                                                                         
    n = df_cleaned[col].notna().sum()
    if n < expected:
        print(f"{col}: {n}/{expected} ({expected - n} missing)")

####################################
##### Clean precipitation data #####    
####################################

precip = np.load(os.path.join(DATA_RAW_DIR, f"precipitation_wy2022_{HUC_ID}.npy"))  # shape: (days, j, i)   
print("precip shape = ", precip.shape)                                      
df_metadata_cleaned = pd.read_csv(os.path.join(DATA_CLEAN_DIR, f"streamflow_wy2022_metadata_{HUC_ID}.csv"))                                      
                                                                                                                                            
ij_huc_bounds, _ = st.define_huc_domain(hucs=[HUC_ID], grid="conus2")
imin, jmin = ij_huc_bounds[0], ij_huc_bounds[1]

dates = pd.date_range(start=date_start, periods=precip.shape[0], freq="D")

precip_dict = {"date": dates}
for _, row in df_metadata_cleaned.iterrows():
    site_id = row["site_id"]
    if pd.isna(row["conus2_i"]) or pd.isna(row["conus2_j"]):
        i_lat = row["latitude"]
        j_long = row["longitude"]
        ij_bounds, _ = st.define_latlon_domain(latlon_bounds = [[i_lat, j_long],[i_lat+0.1, j_long+0.1]], grid="conus2")
        local_i = ij_bounds[0] - imin
        local_j = ij_bounds[1] - jmin
        precip_dict[site_id] = precip[:, local_j, local_i]
    else:
        local_i = int(row["conus2_i"]) - imin
        local_j = int(row["conus2_j"]) - jmin
        precip_dict[site_id] = precip[:, local_j, local_i]
    print(f"site_id: {site_id}, local_i: {local_i}, local_j: {local_j}, row[conus2_i]: {row['conus2_i']}, row[conus2_j]: {row['conus2_j']}, imin: {imin}, jmin: {jmin}")
    

df_precip = pd.DataFrame(precip_dict)
df_precip.to_csv(os.path.join(DATA_CLEAN_DIR, f"precipitation_wy2022_{HUC_ID}.csv"), index=False)
print(f"Saved precipitation CSV with shape {df_precip.shape}")