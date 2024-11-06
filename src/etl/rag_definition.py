# Dagster
from dagster import Definitions, define_asset_job, AssetSelection, ScheduleDefinition
from dagster import sensor, run_status_sensor, DagsterRunStatus, RunRequest, SkipReason

# import assets
from step1_yt_raw_data.ytmusicapi_tojson import ytmusicapi_tojson
from step1_yt_raw_data.json_tocsv import json_tocsv
from step1_yt_raw_data.merge_csv import merge_csv
# from step1_yt_raw_data.merge_csv import mergecsv_to_inserttomongo_sensor
from step1_yt_raw_data.insert_to_mongo import insert_to_mongo

from step1_spotify_raw_data.spotify_category_songlist import spotify_category_songlist
from step1_spotify_raw_data.wait_between_assets import wait_between_assets
from step1_spotify_raw_data.spotify_song_features import spotify_song_features
from step1_spotify_raw_data.spotify_to_mongodb import spotify_to_mongodb

from step2_feature_selection.sp_cleaning_code.sp_clean import sp_clean
from step2_feature_selection.yt_cleaning_code.yt_clean import yt_clean
from step2_feature_selection.data_merge_code.data_merged import data_merged
from step2_feature_selection.feast.feast_store import materialize_incrementally

from step3_embedding.app import main

from pymongo import MongoClient
import os
from datetime import datetime
from dotenv import load_dotenv
# import redis



load_dotenv(dotenv_path='rag_def.env')

MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = os.getenv('MONGO_PORT')
MONGO_USERNAME = os.getenv('MONGO_USERNAME')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_DB = os.getenv('MONGO_DB')

# REDIS_HOST = os.getenv('REDIS_HOST')
# REDIS_PORT = int(os.getenv('REDIS_PORT'))
# REDIS_DB = int(os.getenv('REDIS_DB'))

today = datetime.today().strftime('%Y%m%d') #yyyymmdd

# define jobs ===============================================================
step1_yt_raw_data_job = define_asset_job(
    name="step1_yt_raw_data_job",
    selection=AssetSelection.groups("step1_yt_raw_data")
)

step1_spotify_raw_data_job = define_asset_job(
    name="step1_spotify_raw_data_job",
    selection=AssetSelection.groups("step1_spotify_raw_data")
)

step2_feature_selection_job = define_asset_job(
    name="step2_feature_selection_job",
    selection=AssetSelection.groups("step2_feature_selection")
)

step3_embedding_job = define_asset_job(
    name="step3_embedding_job",
    selection=AssetSelection.groups("step3_embedding")
)


# define sensors ===============================================================
@sensor(
        job=step2_feature_selection_job,
        minimum_interval_seconds=300 # sensor every 5 min
        )
def trigger_step2(context):
   
    client = MongoClient(f'mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/')
    db = client[MONGO_DB] 

    yt_collection = f'yt_category_songlist_{today}' 
    sp_collection = f'spotify_data_{today}' 
    
    collections = db.list_collection_names()

    if yt_collection in collections and sp_collection in collections:
        context.log.info(f"Both collections '{yt_collection}' and '{sp_collection}' exist. Triggering job...")
     
        yield RunRequest(run_key=today) # run key相同時，不會重複執行。 同一天內不會被重複執行，隔一天時，以yt, sp collection條件不符也不會被執行
    else:
        context.log.info(f"One or both collections do not exist, step2_feature_selection_job not triggered.")

# trigger_step3 by sensoring redis
# @sensor(job=step3_embedding_job, 
#         minimum_interval_seconds=300 # sensor every 5 min
#         )
# def trigger_step3(context):
#     r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

#     if r.keys():  # if key exists, there is data. -> trigger job
#         context.log.info("Redis has data. Triggering job...")
#         yield RunRequest(run_key=None)
#     else:
#         context.log.info("Redis is empty, step3_embedding_job not triggered.")

# trigger step3 depending on step2_feature_selection_job completion
@run_status_sensor(
        run_status=DagsterRunStatus.SUCCESS,
        monitored_jobs=[step2_feature_selection_job],
        request_job=step3_embedding_job )
def trigger_step3(context):
    context.log.info(f"triggering step3_embedding_job...")
    yield RunRequest(run_key=None)


# Definitions =======================================================================
defs = Definitions(

    assets=[
        ytmusicapi_tojson, json_tocsv, merge_csv, insert_to_mongo, # step1_yt_raw_data
        spotify_category_songlist, wait_between_assets, spotify_song_features, spotify_to_mongodb, # step1_spotify_raw_data
        sp_clean, yt_clean, data_merged, materialize_incrementally, # step2_feature_selection
        main # step3_embedding
        ], 

    sensors=[
        trigger_step2, 
        trigger_step3
    ],
    
    jobs=[
        step1_yt_raw_data_job, 
        step1_spotify_raw_data_job, 
        step2_feature_selection_job, 
        step3_embedding_job
        ],
    

    schedules=[ 
        ScheduleDefinition(
            name="step1_yt_raw_data_schedule",  
            job_name="step1_yt_raw_data_job",  
            cron_schedule="0 10 * * 1",  # At 10:00 on Monday.
            # cron_schedule="49 16 22 10 *",  
            execution_timezone="Asia/Taipei"
        ),
        ScheduleDefinition(
            name="step1_spotify_raw_data_schedule",  
            job_name="step1_spotify_raw_data_job",  
            cron_schedule="0 10 * * 1",  # At 10:00 on Monday.
            # cron_schedule="51 16 22 10 *", 
            execution_timezone="Asia/Taipei"
        )
    ]

    )


