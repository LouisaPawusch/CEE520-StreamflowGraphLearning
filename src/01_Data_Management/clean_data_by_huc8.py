from dotenv import load_dotenv
import numpy as np
import os
import pandas as pd
import hf_hydrodata as hf
import subsettools as st

load_dotenv()

hf.register_api_pin(os.getenv("HF_EMAIL"), os.getenv("HF_PIN"))

HUC_IDs = ["02040101", "02040102"]
DATA_RAW_DIR = os.path.join(os.path.dirname(__file__), "../../data/raw/")
DATA_CLEAN_DIR = os.path.join(os.path.dirname(__file__), "../../data/clean")

date_start = "2021-10-01"
date_end = "2022-09-30"

####################################
###### Clean streamflow data #######    
####################################

df = pd.read_csv(os.path.join(DATA_RAW_DIR, "streamflow_wy2022.csv"))
df_metadata = pd.read_csv(os.path.join(DATA_RAW_DIR, "streamflow_wy2022_metadata.csv"), dtype={"site_id": str, "huc8": str})
# Combined metadata for all HUCs
df_metadata_all = df_metadata[df_metadata["huc8"].isin(HUC_IDs)]
df_metadata_all.to_csv(os.path.join(DATA_CLEAN_DIR, "streamflow_wy2022_metadata_all.csv"), index=False)

valid_sites = set(df_metadata_all["site_id"])
df_all = df[["date"] + [col for col in df.columns if col != "date" and col in valid_sites]]
df_all.to_csv(os.path.join(DATA_CLEAN_DIR, f"streamflow_wy2022.csv"), index=False)

expected = 365
for col in df_all.columns[1:]:
    n = df_all[col].notna().sum()
    if n < expected:
        print(f"{col}: {n}/{expected} ({expected - n} missing)")

####################################
##### Clean precipitation data #####    
####################################

precip_dict = {}
df_metadata_cleaned = pd.read_csv(os.path.join(DATA_CLEAN_DIR, f"streamflow_wy2022_metadata_all.csv"), dtype={"site_id": str, "huc8": str})                                    

for HUC_ID in HUC_IDs:

    precip = np.load(os.path.join(DATA_RAW_DIR, f"precipitation_wy2022_{HUC_ID}.npy"))  # shape: (days, j, i)   
    print("precip shape = ", precip.shape)     

    if "date" not in precip_dict:
        precip_dict["date"] = pd.date_range(start=date_start, periods=precip.shape[0], freq="D")

                                                                                                                                                
    ij_huc_bounds, _ = st.define_huc_domain(hucs=[HUC_ID], grid="conus2")
    imin, jmin = ij_huc_bounds[0], ij_huc_bounds[1]

    for _, row in df_metadata_cleaned[df_metadata_cleaned["huc8"] == HUC_ID].iterrows():
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
df_precip.to_csv(os.path.join(DATA_CLEAN_DIR, f"precipitation_wy2022.csv"), index=False)
print(f"Saved precipitation CSV with shape {df_precip.shape}")