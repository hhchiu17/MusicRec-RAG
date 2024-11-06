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
from dotenv import load_dotenv
from .prop import generate_logger, split_playlist_csv, initialize_sp_client, RetryLimitExceededError
from dagster import asset, AssetExecutionContext
from random import choice

#變數 ===========================================================================
retries = 0
logger = generate_logger(__file__)


load_dotenv(dotenv_path='step1_spotify_raw_data/sp.env')
# 初始化客戶端
sp1 = initialize_sp_client(os.getenv("CLIENT_ID_1"), os.getenv("CLIENT_SECRET_1"))
sp2 = initialize_sp_client(os.getenv("CLIENT_ID_2"), os.getenv("CLIENT_SECRET_2"))
sp3 = initialize_sp_client(os.getenv("CLIENT_ID_3"), os.getenv("CLIENT_SECRET_3"))
sp4 = initialize_sp_client(os.getenv("CLIENT_ID_4"), os.getenv("CLIENT_SECRET_4"))
sp5 = initialize_sp_client(os.getenv("CLIENT_ID_5"), os.getenv("CLIENT_SECRET_5"))
sp6 = initialize_sp_client(os.getenv("CLIENT_ID_6"), os.getenv("CLIENT_SECRET_6"))

sp_client = [sp1, sp2, sp3, sp4, sp5, sp6]
    
category_songlist = []
items = None
year = datetime.now().year
month = datetime.now().month
day = datetime.now().day

#general functions ==============================================================
#category
def category():
    category_lst = []
    categories = choice(sp_client).categories(country="US", locale="en", limit=50, offset=0)
    # global category_lst
    for i in range(len(categories['categories']['items'])):
        category_lst.append(categories['categories']['items'][i]['id'])
    logger.info("category done")
    return category_lst

#playlist
def playlist():
    # global year, month, day

    if os.path.exists(f"category_playlist_{year}_{month}_{day}.json"):
        os.remove(f"category_playlist_{year}_{month}_{day}.json")

    for j in range(len(category_lst)):
        category_playlist = choice(sp_client).category_playlists(category_id=category_lst[j], country="US", limit=50, offset=0)
        with open(f"category_playlist_{year}_{month}_{day}.json", "a") as f:
            json.dump(category_playlist, f)
            f.write("\n")
        sleep(20)

    playlist_lst = []
    with open(f"category_playlist_{year}_{month}_{day}.json", "r") as o:
        for line in o.readlines():
            category_json = json.loads(line)
            playlist_lst.append(category_json)
            
    with open(f"playlist_{year}_{month}_{day}.csv", "w") as o:
        o.write("category,playlistId\n")
        for i in range(len(playlist_lst)):
            for j in range(len(playlist_lst[i]['playlists']['items'])):
                o.write(f"{playlist_lst[i]['message']},{playlist_lst[i]['playlists']['items'][j]['external_urls']['spotify'].split('/')[-1]}\n")

    logger.info("playlist done")
    return playlist_lst

#songlist
def songlist(client, df, i, items, category_songlist, playlistId):
    global retries
    try:
        items = client.playlist(playlist_id=playlistId,
                            market="US",
                            fields="name,tracks(items(added_at,track(name,id,artists(name))))")
        logger.debug(f"Retrieved playlist {playlistId} for client {client}")
        append_items(df, i, items, category_songlist)
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
                sleep(10)
            return items
        
        logger.error("retries exceeded")
        # raise RetryLimitExceededError("skipping api request")

# Function to process songlist() for a given client and its playlists
def process_songlist_for_client(client, df, playlist_ids):
    result_list = []
    
    for i, playlist_id in enumerate(playlist_ids):
        items = songlist(client, df, i, None, category_songlist, playlist_id)  # Call songlist() with client and playlist ID
        if items:
            result_list.extend(items)
    
    return result_list

def append_items(df, i, items, category_songlist):
    # 檢查 'items' 是否存在
    if items and 'tracks' in items and 'items' in items['tracks']:
        track_items = items['tracks']['items']  # 提取 tracks 的 items 列表
        for track_info in track_items:
            track = track_info.get('track')  # 使用 .get() 安全提取 'track'
            # 檢查 track 是否為 None
            if track:
                # 構建 info 字典
                info = {
                    'category': df.at[i, 'category'],
                    'playlistId': df.at[i, 'playlistId'],
                    'playlistName': items['name'],
                    'artist': track_info['track']['artists'],
                    'track': track_info['track']['name'],
                    'date_added': track_info['added_at'],
                    'trackURL': f"https://open.spotify.com/track/{track_info['track']['id']}",
                    'id': track_info['track']['id']
                }
                
                # 將收集到的資訊加入 category_songlist
                category_songlist.append(info)

        # 在處理完後添加延遲
        sleep(8.5)
    else:
        return None  # 如果 items 不存在，直接返回 None

# Parallel request to songlist() for each client and corresponding sub-CSV file
def parallel_songlist_request(clients, csv_paths):
    merged_list = []
    with ThreadPoolExecutor() as executor:
        futures = []

        # Zip clients with corresponding CSV files
        for client, csv_path in zip(clients, csv_paths):
            df = pd.read_csv(csv_path)
            playlist_ids = df['playlistId'].tolist()
            futures.append(executor.submit(process_songlist_for_client, client, df, playlist_ids))
        
        # Collect results and merge into a single list of dictionaries
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    merged_list.extend(result)
            except Exception as e:
                logger.error(f"Error in parallel_songlist_request: {e}")
    return merged_list

# Merging the 6 lists into one and saving to a single CSV
def write_merged_csv(merged_list):
    for i in range(6): #remove sub_playlist_csv after merging
        if os.path.exists(f"sub_playlist_{i+1}.csv"):
            os.remove(f"sub_playlist_{i+1}.csv")
            
    if merged_list:
        keys = merged_list[0].keys()
        with open(f'data/spotify_category_songlist_{year}_{month}_{day}.csv', 'w', newline='', encoding='utf8') as f:
            dict_writer = csv.DictWriter(f, keys)
            dict_writer.writeheader()
            dict_writer.writerows(merged_list)

        os.remove(f"category_playlist_{year}_{month}_{day}.json")
        os.remove(f"playlist_{year}_{month}_{day}.csv")
        logger.info("Merged CSV written.")

    else:
        logger.error("No data to write.")
        # raise Exception("No data to write.")

# Main function
@asset(
        group_name="step1_spotify_raw_data",
)
def spotify_category_songlist():
    """
    取得category->category playlist->songlist->csv
    """
    #非星期六則跳過
    # if datetime.today().weekday() != 5:
    #     logger.warning("星期六再來抓")
    #     exit(1)

    #資料放在./data/
    # generate_logger()裡面已經寫好創建data資料夾的程式碼
    if not os.path.isdir('data'):
        os.mkdir('data')

    # load_dotenv()
    logger.info("spotipy client initialized")

    global category_lst

    category_lst = category()
    sleep(3)
    playlist()
    sleep(3)
    
    df = pd.read_csv(f"playlist_{year}_{month}_{day}.csv", sep=",")
    sub_csv_paths = split_playlist_csv(df, len(sp_client))
    logger.info("split_playlist_csv done")

    parallel_songlist_request(sp_client, sub_csv_paths)
    if retries < 10:
        logger.info("parallel_songlist_request done")
    else:
        logger.error("retries exceeded, skipping api request")

    write_merged_csv(category_songlist)
    # logger.info("write_merged_csv done")
    return 'done'
