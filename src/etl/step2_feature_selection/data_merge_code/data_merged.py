import sys
import io
import pandas as pd
import os

from .postgre_to_python import connect_to_database, read_data 
from .python_to_postgre import load_postgre_data
from .merge import merge_data_same_song, process_yt_data, process_sp_data


from dagster import asset 
from dagster import AssetExecutionContext
from dagster import AssetIn





@asset(
        group_name="step2_feature_selection",
        ins={
            "sp_clean_fetch_date":AssetIn("sp_clean"),
            "yt_clean_fetch_date":AssetIn("yt_clean")
             }
)
def data_merged(context: AssetExecutionContext, sp_clean_fetch_date, yt_clean_fetch_date):

    script_dir = os.path.dirname(os.path.abspath(__file__))
    postgre_path= os.path.join(script_dir, "config.ini") 

    fetch_date_yt = yt_clean_fetch_date
    fetch_date_sp = sp_clean_fetch_date
    

    # extract
    engine = connect_to_database(postgre_path)
    data_yt = read_data("yt_demo", engine, fetch_date_yt)
    data_sp = read_data("sp_demo", engine, fetch_date_sp)

    data_yt =  process_yt_data(data_yt)
    data_sp =  process_sp_data(data_sp)


    # transform
    merged_data = merge_data_same_song(data_sp,data_yt)

    # loading
    load_postgre_data(merged_data, postgre_path)
    context.log.info(f"data merged successfully")




# if __name__ == "__main__":
#     sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
#     data_merged()