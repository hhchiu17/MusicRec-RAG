import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from dagster import asset, AssetIn
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path='step1_yt.env')

today = datetime.today().strftime('%Y%m%d') #yyyymmdd
# today = '20241002'
script_dir = os.path.dirname(os.path.abspath(__file__))
relative_data_path_dir = f"../data/{today}"
absolute_data_path_dir = os.path.join(script_dir, relative_data_path_dir) # data/today

MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = os.getenv('MONGO_PORT')
MONGO_USERNAME = os.getenv('MONGO_USERNAME')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_DB = os.getenv('MONGO_DB')

@asset(
    group_name="step1_yt_raw_data",
    deps=["merge_csv"],
)

# def insert_to_mongo(merge_csv):
def insert_to_mongo():
    """
    insert yt csv to mongo
    """
    
    # mongo connection - local host
    # client = MongoClient(f'mongodb://{MONGO_HOST}:{MONGO_PORT}/')

    # mongo connection - connection to service
    client = MongoClient(f'mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/')
    db = client[MONGO_DB]


    # yt_category_songlist.csv -> mongo
    current_collection = db[f"yt_category_songlist_{today}"]  
    csv_file = f"{absolute_data_path_dir}/yt_category_songlist.csv"  
    
    # batch insert to mongoDB
    chunksize = 1000
    for chunk in pd.read_csv(csv_file, chunksize=chunksize):
        data = chunk.to_dict(orient="records")
        current_collection.insert_many(data)

    print(f"Mongo insert done: {absolute_data_path_dir}/yt_category_songlist.csv")

    client.close()


# insert_to_mongo()