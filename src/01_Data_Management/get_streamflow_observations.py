import os
import yaml
import hf_hydrodata as hf

STATIC_FILE_LIST = ["streamflow"]
DATA_DIR = os.path.join(os.path.dirname(__file__), "../../data/raw/")

for var in STATIC_FILE_LIST:
    hf.get_point_data(
        filepath=DATA_DIR,
        dataset="usgs_nwis",
        variable=var,
        temporal_resolution="daily",
        aggregation="mean",
        file_type="netcdf",
        date_start="2022-01-01",
        date_end="2022-01-05"
    )