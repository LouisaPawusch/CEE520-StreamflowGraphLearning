from dotenv import load_dotenv
import os
import requests
from zipfile import ZipFile
from io import BytesIO
import hf_hydrodata as hf

load_dotenv()

STATIC_FILE_LIST = ["streamflow"]
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
polygon_path, polygon_crs = download_huc2_polygon(huc="02")

dataset = "usgs_nwis"
resolution = "daily"
aggregation = "mean"
date_start = "2021-10-01"
date_end = "2022-09-30"

"""
for var in STATIC_FILE_LIST:
    df = hf.get_point_data(
        dataset=dataset,
        variable=var,
        temporal_resolution=resolution,
        aggregation=aggregation,
        date_start=date_start,
        date_end=date_end,
        polygon=polygon_path,
        polygon_crs=polygon_crs
    )
    df_metadata = hf.get_point_metadata(
        dataset=dataset,
        variable=var,
        temporal_resolution=resolution,
        aggregation=aggregation,
        date_start=date_start,
        date_end=date_end,
        polygon=polygon_path,
        polygon_crs=polygon_crs
    )

    print(f"Downloaded {len(df)} rows of {var} data and {len(df.columns)} columns.")
    df.to_csv(os.path.join(DATA_DIR, f"{var}_wy2022.csv"), index=False)
    df_metadata.to_csv(os.path.join(DATA_DIR, f"{var}_wy2022_metadata.csv"), index=False)
"""

print(hf.get_citations(dataset=dataset))