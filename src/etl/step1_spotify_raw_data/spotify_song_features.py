import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.oauth2 import SpotifyOAuth
import json
from time import sleep
import os
import pandas as pd
import numpy as np
import csv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from .prop import generate_logger, split_playlist_csv, initialize_sp_client, RetryLimitExceededError
from .wait_between_assets import wait_between_assets
from dotenv import load_dotenv
from dagster import asset

#變數 =========================================================
retries = 0

year = datetime.now().year
month = datetime.now().month
day = datetime.now().day

track = []
audio_features = []
items = None
batch_size = 50

#generate logger
logger = generate_logger(__file__)

load_dotenv(dotenv_path='step1_spotify_raw_data/sp.env')

#初始化客戶端
sp1 = initialize_sp_client(os.getenv("CLIENT_ID_1"), os.getenv("CLIENT_SECRET_1"))
sp2 = initialize_sp_client(os.getenv("CLIENT_ID_2"), os.getenv("CLIENT_SECRET_2"))
sp3 = initialize_sp_client(os.getenv("CLIENT_ID_3"), os.getenv("CLIENT_SECRET_3"))
sp4 = initialize_sp_client(os.getenv("CLIENT_ID_4"), os.getenv("CLIENT_SECRET_4"))
sp5 = initialize_sp_client(os.getenv("CLIENT_ID_5"), os.getenv("CLIENT_SECRET_5"))
sp6 = initialize_sp_client(os.getenv("CLIENT_ID_6"), os.getenv("CLIENT_SECRET_6"))

sp_client = [sp1, sp2, sp3, sp4, sp5, sp6]


#general functions ============================================
# Processing song features for each client and playlist segment
def process_song_features_for_client(client, track_ids):
    result_list = []
    
    # Process tracks in batches of 50 (Spotify's API limit)
    batch_size = 50
    for i in range(0, len(track_ids), batch_size):
        track_batch = track_ids[i:i+batch_size]
        items = song_features(client, track_batch)  # Call song_features() with the current batch of tracks
        if items:
            result_list.extend(items)
    
    return result_list

def song_features(client: spotipy.Spotify, track_batch):
    global retries
    try:
        items = client.audio_features(tracks=track_batch)
        logger.debug(items)
        append_items(items, audio_features)
        return items
    except spotipy.exceptions.SpotifyException as exc:
        while retries < 10:
            retries += 1
            if exc.http_status == 429:
                logger.error(f"{client}'s Rate limit exceeded")
                sleep(5)
            elif exc.http_status in (404, 500):
                logger.error("Track not found or server error")
                sleep(5)
            else:
                logger.error("Error")
            return song_features(client, track_batch)
    
        # return None
        # raise  RetryLimitExceededError("skipping api request")
    except json.decoder.JSONDecodeError:
        return None
        
def append_items(items, audio_features):
    if items:
        # print(items)
        for j in range(len(items)):
            audio_features.append(items[j])
    else:
        return None
    sleep(20)

# Parallel request to song_features() for each client and corresponding sub-CSV file
def parallel_song_features_request(clients, csv_paths):
    merged_list = []
    with ThreadPoolExecutor() as executor:
        futures = []

        # Zip clients with corresponding CSV files
        for client, csv_path in zip(clients, csv_paths):
            df = pd.read_csv(csv_path)
            track_ids = [url.split('/')[-1] for url in df['trackURL'].tolist()]  # Extract track IDs from track URLs
            futures.append(executor.submit(process_song_features_for_client, client, track_ids))
        
        # Collect results and merge into a single list of dictionaries
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    merged_list.extend(result)
            except Exception as e:
                logger.error(f"Error in parallel_song_features_request: {e}")
    return merged_list 

def write_csv(merged_list):
    for i in range(6): #remove sub_playlist_csv after merging
        if os.path.exists(f"sub_playlist_{i+1}.csv"):
            os.remove(f"sub_playlist_{i+1}.csv")
    
    if merged_list:
        features_df = pd.json_normalize(merged_list)
        features_df.to_csv(f'data/spotify_song_features_{year}_{month}_{day}.csv', index=False)
        logger.info("write_csv done")
    else:
        #feature相關欄位都為空值
        features_df = pd.DataFrame(data=None, columns=['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'type', 'id', 'uri', 'track_href', 'analysis_url', 'duration_ms', 'time_signature'])
        features_df.to_csv(f'data/spotify_song_features_{year}_{month}_{day}.csv', index=False)
        logger.error("merged_list is empty, writing null dataframe")
        # raise Exception("No data to write.")

# Main function
@asset(
        group_name="step1_spotify_raw_data",
        deps=[wait_between_assets]
)
def spotify_song_features():
    """
    調用從spotify_category_songlist取得的csv檔案->批量請求feature->寫入feature csv檔案
    """
    
    #初始化客戶端
    # load_dotenv()

    logger.info("Spotify clients initialized")

    df = pd.read_csv(f'data/spotify_category_songlist_{year}_{month}_{day}.csv', sep=',')
    sub_csv_paths = split_playlist_csv(df, len(sp_client))
    logger.info("sub_csv_paths done")

    # Perform parallel requests for song features and merge the results
    merged_song_features = parallel_song_features_request(sp_client, sub_csv_paths)
    if retries < 10:
        logger.info("parallel_request done")
    else:
        logger.error("retries exceeded, skipping api request")

    write_csv(merged_song_features)
    return 'done'
        