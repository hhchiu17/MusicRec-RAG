import pandas as pd
import re, json, csv, pymongo
from pymongo import MongoClient
from pymongo.errors import AutoReconnect
from datetime import datetime
from dotenv import load_dotenv
import os
from dagster import asset
import pymongo.collection
import pymongo.database
from .prop import generate_logger
from .spotify_song_features import spotify_song_features
from retry import retry

#變數 =======================================================
logger = generate_logger(__file__)

load_dotenv(dotenv_path='step1_spotify_raw_data/sp.env')

year = datetime.now().year
month = str(datetime.now().month) if datetime.now().month >= 10 else f'0{datetime.now().month}'
day = str(datetime.now().day) if datetime.now().day >= 10 else f'0{datetime.now().day}'
#general functions ==========================================
def merge_csv():
    """
    歌單csv和特徵csv合併
    """
    song_file = f'data/spotify_category_songlist_{year}_{month}_{day}.csv'
    feature_file = f'data/spotify_song_features_{year}_{month}_{day}.csv'

    if os.path.exists(song_file) and os.path.exists(feature_file):

        song = pd.read_csv(song_file, sep=',')
        feature = pd.read_csv(feature_file, sep=',')
        feature = feature.drop_duplicates()

        song['id'] = song['trackURL'].str.split('/').str[-1]

        merged = pd.merge(song, feature, how='left', on='id', validate='many_to_one')

        merged['fetchDate'] = f'{year}-{month}-{day} 00:00:00'

        filename = f'data/spotify_merged_{year}_{month}_{day}.csv'
        merged.to_csv(f'{filename}', index=False)
        logger.info(f"csv file {filename} created")

        os.remove(song_file)
        os.remove(feature_file)

        return filename
    else:
        logger.error("songlist or feature csv files not found")
        # raise FileNotFoundError("songlist or feature csv files not found")
    
@retry(AutoReconnect, tries=3, delay=3)
def connect_to_mongo():
    """
    連接mongoDB
    """
    client = MongoClient(f'mongodb://{os.getenv("USERNAME")}:{os.getenv("PASSWORD")}@{os.getenv("HOST")}:{os.getenv("PORT")}/', serverSelectionTimeoutMS=5000)
    client.admin.command('ping')

    return client

def mongoimport(csv_path: str, db_name: pymongo.database.Database, coll_name: pymongo.collection.Collection):
    """ Imports a csv file at path csv_name to a mongo colection
    return: the amount of rows inserted
    """
    db = db_name
    collection = coll_name

    count = 0
    try:
        with open(csv_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)
            #覆蓋資料: 若collection有資料則刪除所有資料再寫入
            if collection.count_documents({}) > 0:
                collection.delete_many({})
                count = len(rows)
            if rows:
                collection.insert_many(rows)
                count = len(rows)
        logger.info(f"csv file {csv_path} imported to mongodb")
    except pymongo.errors.ServerSelectionTimeoutError as e:
        logger.error(f"Error importing csv file {csv_path}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error importing csv file {csv_path}: {e}")
    return count

def process_artist():
    """
    處理artist欄位
    """

    none_pattern = re.compile(r"None\b")

    for doc in collection.find({}):
        artist_str = doc['artist']
        track_str = doc['track']

        artist_str = artist_str.replace('None', 'null')
        artist_str = artist_str.replace("''", 'null')

        # 替換鍵的單引號為雙引號
        corrected_artist = re.sub(r"([{,]\s*)'([^']+)'(\s*:)", r'\1"\2"\3', artist_str)

        # 替換值的單引號為雙引號
        corrected_artist = re.sub(r":\s*'([^']*)'", r': "\1"', corrected_artist)
        # 處理 value 中間的雙引號，但保留外層雙引號
        corrected_artist = re.sub(r'"\s*([^"]+?)\s*"', r'"\1"', corrected_artist)
    
        # 檢查完全空的情況：""
        if corrected_artist.strip() == "":
            corrected_artist = none_pattern.sub('null', corrected_artist)

        elif track_str.strip() == "":
            track_str = none_pattern.sub('null', track_str)

        try:
            # 更新 artist 欄位並保存
            artist_json = json.loads(corrected_artist)
            collection.update_one({'_id': doc['_id']}, {'$set': {'artist': artist_json, 'track': track_str}})
        except Exception:
            collection.delete_one({'_id': doc['_id']})

    logger.info("artist field processed")

# Main function
@asset(
    group_name="step1_spotify_raw_data",
    deps=[spotify_song_features]
)
def spotify_to_mongodb():
    """
    songlist與feature合併後存入mongodb->artist欄位轉json
    """

    # load_dotenv()
    global client, db, collection

    try:
        client = connect_to_mongo()
        db = client['rag']
        collection = db[f'spotify_data_{year}{month}{day}']
        logger.info("Successfully connected to MongoDB")
    except pymongo.errors.ServerSelectionTimeoutError as e:
        logger.error(f"Error connecting to MongoDB: {e}")
        # exit(1)

    filename = merge_csv()

    mongoimport(filename, db, collection)

    if os.path.exists(filename):
        os.remove(filename)
        logger.info(f"csv file {filename} deleted")

    process_artist()

    client.close()
    return 'done'