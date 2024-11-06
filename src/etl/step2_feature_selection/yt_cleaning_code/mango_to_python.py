from pymongo import MongoClient, errors
import configparser
import pandas as pd
import re


def connect_to_mongodb(path):
    try:
        config = configparser.ConfigParser()
        config.read(path)

        db_host = config["mongodb"]["host"]
        db_port = config["mongodb"]["port"]
        db_username = config["mongodb"]["username"]
        db_password = config["mongodb"]["password"]  


        # 連接到本地的 MongoDB 伺服器
        client = MongoClient(f"{db_host}:{db_port}/", 
            username=db_username,
            password=db_password,
            serverSelectionTimeoutMS=5000)
        client.server_info()  # 嘗試連接，檢查是否能獲取伺服器信息
        print("Connected to MongoDB successfully")
        return client
    except errors.ServerSelectionTimeoutError as err:
        print(f"Failed to connect to MongoDB: {err}")
        exit(1)


def get_latest_yt_collection(client, db_name, prefix="yt_category_songlist_"):
    db = client[db_name]
    collections = [col for col in db.list_collection_names() if col.startswith(prefix)]
    latest_collection = max(collections, key=lambda x: int(re.findall(r'\d+', x)[0]))
    return latest_collection






def read_mongodb_data(client, db_name, collection_name, query=None):
    db = client[db_name]
    collection = db[collection_name]
    if query is None:
        query = {}
    cursor = collection.find(query)
    return cursor


def read_mongo_data(mongopath):
    mongo_client = connect_to_mongodb(mongopath)
    config = configparser.ConfigParser()
    config.read(mongopath)
    db_name = config["mongodb"]["db"]

    latest_collection = get_latest_yt_collection(mongo_client, db_name)
    print(f'The latest collection: {latest_collection}')

    date_str = latest_collection.split('_')[-1]
    formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    

    mongodb_data = read_mongodb_data(mongo_client, db_name, latest_collection)
    data = pd.DataFrame(list(mongodb_data))

    return formatted_date, data


# if __name__ == "__main__":
#         mongopath = "C:\\Users\\student\\rag_data_clean\\yt_cleaning_code\\yt_cleaning_p3\\config_outer.ini"

#         # extract
#         fetch_date, raw_data = read_mongo_data(mongopath)
#         print(fetch_date)