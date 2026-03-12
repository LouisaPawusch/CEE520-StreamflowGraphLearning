from dotenv import load_dotenv
import os
import requests
from zipfile import ZipFile
from io import BytesIO
import hf_hydrodata as hf
import subsettools as st
import numpy as np
import pandas as pd

load_dotenv()

DATA_DIR = os.path.join(os.path.dirname(__file__), "../../data/raw/")
SHAPE_DIR = os.path.join(DATA_DIR, "shapes")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(SHAPE_DIR, exist_ok=True)

hf.register_api_pin(os.getenv("HF_EMAIL"), os.getenv("HF_PIN"))


def download_huc2_polygon(huc="02"):
    # Following example from hf_hydrodata/point/example_shapefile.ipynb (https://github.com/hydroframe/subsettools-binder/blob/main/hf_hydrodata/point/example_shapefile.ipynb)
    if len(huc) == 2:
        url = f"https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/WBD/HU2/Shape/WBD_{huc}_HU2_Shape.zip"
    elif len(huc) == 4:
        url = f"https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/WBD/HU4/Shape/WBD_{huc}_HU4_Shape.zip"
    else:
        raise ValueError("HUC must be either 2 or 4 digits long.")
    response = requests.get(url)
    response.raise_for_status()

    zip_file = ZipFile(BytesIO(response.content))

    members = [
        "Shape/WBDHU2.shp",
        "Shape/WBDHU2.shx",
        "Shape/WBDHU2.dbf",
        "Shape/WBDHU2.prj",
    ]
    zip_file.extractall(path=SHAPE_DIR, members=members)

    polygon_path = os.path.join(SHAPE_DIR, "Shape", "WBDHU2.shp")
    prj_path = os.path.join(SHAPE_DIR, "Shape", "WBDHU2.prj")

    with open(prj_path, "r") as f:
        polygon_crs = f.readline().strip()

    return polygon_path, polygon_crs

# Example: get data only for HUC8 = 02030105 inside HUC02 = 02

####################################
####### Grab streamflow data #######    
####################################


polygon_path, polygon_crs = download_huc2_polygon(huc="02")

dataset = "usgs_nwis"
variable = "streamflow"
resolution = "daily"
aggregation = "mean"
date_start = "2021-10-01"
date_end = "2022-09-30"
print(hf.get_citations(dataset=dataset))


df = hf.get_point_data(
    dataset=dataset,
    variable=variable,
    temporal_resolution=resolution,
    aggregation=aggregation,
    date_start=date_start,
    date_end=date_end,
    polygon=polygon_path,
    polygon_crs=polygon_crs
)
df_metadata = hf.get_point_metadata(
    dataset=dataset,
    variable=variable,
    temporal_resolution=resolution,
    aggregation=aggregation,
    date_start=date_start,
    date_end=date_end,
    polygon=polygon_path,
    polygon_crs=polygon_crs
)

print(f"Downloaded {len(df)} rows of {variable} data and {len(df.columns)} columns.")
df.to_csv(os.path.join(DATA_DIR, f"{variable}_wy2022.csv"), index=False)
df_metadata.to_csv(os.path.join(DATA_DIR, f"{variable}_wy2022_metadata.csv"), index=False)


####################################
##### Grab precipitation data ######    
####################################

dataset = "CW3E"
variable = "precipitation"
resolution = "daily"
aggregation = "sum"
date_start = "2021-10-01"
date_end = "2022-09-30"
huc_ids = ["02040101", "02040102"]
print(hf.get_citations(dataset=dataset))

for huc_id in huc_ids:
    ij_huc_bounds, sub_mask = st.define_huc_domain(hucs=[huc_id], grid="conus2")
    print(f"ij_huc_bounds: {ij_huc_bounds}")

    precip = hf.gridded.get_gridded_data(
        dataset=dataset,
        variable=variable,
        temporal_resolution=resolution,
        grid = "conus2",
        aggregation=aggregation,
        date_start=date_start,
        date_end=date_end,
        huc_id = huc_id
    )

    print(f"Downloaded {precip.shape[0]} rows of {variable} data and {precip.shape[1]} columns.")
    np.save(os.path.join(DATA_DIR, f"{variable}_wy2022_{huc_id}"), precip)
