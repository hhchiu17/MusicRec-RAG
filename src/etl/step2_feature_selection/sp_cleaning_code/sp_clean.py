import sys
import io
import pandas as pd
import os

from dagster import asset
from dagster import AssetExecutionContext



from .mango_to_python import read_mongo_data
from .python_to_postgre import load_postgre_data
from .data_process import extract_artist_name, group_data, clean_data

@asset(
        group_name="step2_feature_selection"
)
def sp_clean(context: AssetExecutionContext):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    path= os.path.join(script_dir, "config.ini") 


    # extract
    fetch_date_sp, raw_data = read_mongo_data(path)

    # transform
    raw_data = clean_data(raw_data)
    raw_data["artist"] = raw_data["artist"].apply(extract_artist_name)
    trans_data = group_data(raw_data)

    # loading
    # trans_data.to_csv("data_cleaning_20241015_pyo.csv", encoding="utf-8")
    load_postgre_data(trans_data, path)

    context.log.info(f"spotify data cleaned successfully, fetch date: {fetch_date_sp}")

    return fetch_date_sp


# if __name__ == "__main__":
#     sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
#     sp_clean()
