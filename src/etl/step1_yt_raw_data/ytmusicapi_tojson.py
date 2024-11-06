# from pprint import pprint
import logging
from concurrent.futures import ThreadPoolExecutor
import time
import json
import os
from datetime import datetime
from ytmusicapi import YTMusic
from dagster import asset
import csv
import gc

ytmusic = YTMusic()
today = datetime.today().strftime('%Y%m%d')
# today = '20241030_test_minimize'

script_dir = os.path.dirname(os.path.abspath(__file__))
relative_data_path_dir = f"../data/{today}"
absolute_data_path_dir = os.path.join(script_dir, relative_data_path_dir) # data/today

# general functions ====================================================
# make folders
def ensure_folder_exists(folder):
    if not os.path.exists(folder):
        os.makedirs(folder)

# save to json
def save_to_json(data, filepath):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

# load from json
def load_from_json(filepath):
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None


# logging ==============================================================
ensure_folder_exists(absolute_data_path_dir)

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s', 
                    handlers=[
                        logging.FileHandler(f'{absolute_data_path_dir}/ytmusicapi_tojson.log'),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)


# api related functions ================================================
# 1. get_mood_categories -> json
def get_and_save_mood_categories():
    logger.info('step 1. get_mood_categories: ')
    
    ensure_folder_exists(absolute_data_path_dir)

    # api: get_mood_categories
    mood_categories = ytmusic.get_mood_categories()
    
    save_to_json(mood_categories, f'{absolute_data_path_dir}/mood_categories.json')
    logger.info('step 1 done')
    return mood_categories

# 2. get_mood_playlists -> json
def fetch_mood_playlists():
    logger.info('step 2. get_mood_playlists: ')

    # load file to get "params"
    mood_categories_data = load_from_json(f'{absolute_data_path_dir}/mood_categories.json')
    
    all_mood_playlists = {}

    # api: get_mood_playlists(params)
    for category, category_data in mood_categories_data.items():
        for item in category_data:
            params = item.get('params')
            if params:
                mood_playlists_data = ytmusic.get_mood_playlists(params)
                all_mood_playlists[params] = mood_playlists_data
            else:
                print("params loaded fail.")
                logger.info('params loaded fail.')

    
    save_to_json(all_mood_playlists, f'{absolute_data_path_dir}/mood_playlist.json')
    logger.info('step 2 done.')
    return all_mood_playlists

# 3. get_playlist -> json
def fetch_playlist_songs():
    logger.info('step 3. get_playlist: ')
    # load file to get "playlistId"
    mood_playlist_data = load_from_json(f'{absolute_data_path_dir}/mood_playlist.json')
    logger.info("mood_playlist.json loaded.")

    # get all playlist id (unique)
    playlist_id_all = []
    for playlist in mood_playlist_data.values():
        for song in playlist:
            playlist_id_all.append(song.get('playlistId'))
    unique_playlist_id = list(set(playlist_id_all))

    playlist_data = {}
    # api: get_playlist(playlistId)
    def fetch_playlist_songs_child(playlist_id):
        try:
            playlist_data[playlist_id] = ytmusic.get_playlist(playlist_id)
        except Exception as e:
            logger.error(f"Error fetching playlist {playlist_id}: {e}")
            playlist_data[playlist_id] = "get_playlist_error"


    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = []
        for playlist_id in unique_playlist_id:
            futures.append(executor.submit(fetch_playlist_songs_child, playlist_id))
        
        for future in futures:
            future.result()

    
    save_to_json(playlist_data, f'{absolute_data_path_dir}/playlist_songs.json')
    logger.info('step 3 done.')
    return playlist_data

# 4. ytmusic.get_song -> json 
def fetch_songs_details():
    logger.info('step 4. get_song: ')
    # load file to get "videoId"
    playlist_songs = load_from_json(f"{absolute_data_path_dir}/playlist_songs.json")
    logger.info("playlist_songs.json loaded.")
    # get videoId, unique
    video_id_all = []
    for params, songinfos in playlist_songs.items():
        if songinfos != 'get_playlist_error':
            for song in songinfos.get("tracks", []):
                video_id = song.get("videoId")
                if video_id: # some videoId are null
                    video_id_all.append(video_id)
    video_id_unique = list(set(video_id_all))


    all_songs_data = {}
    # api: get_song(videoId)
    def fetch_details(video_id):
        # if video_id not in all_songs_data:
        try:
            song_data = ytmusic.get_song(video_id)
            all_songs_data[video_id] = song_data
        except Exception as e:
            logger.error(f"Error fetching song for video_id {video_id}: {e}")
            all_songs_data[video_id] = "get_song_error"

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = []
        # count = 0
        for video_id in video_id_unique:
            try: 
                futures.append(executor.submit(fetch_details, video_id))
            except Exception as e:
                logger.error(f'fetch song details error: {video_id}, Exception msg: {e}')
            # count += 1
            # if count == 100:
            #     break

        for future in futures:
            future.result()


    save_to_json(all_songs_data, f'{absolute_data_path_dir}/songs_details.json')
    logger.info('step 4 done.')
    gc.collect()
    return all_songs_data

# get lyrics: step 5,6
# 5. get_watch_playlist -> json
def fetch_watch_playlist():
    logger.info('step 5. get_watch_playlist: ')
    # load file to get "videoId"
    playlist_songs = load_from_json(f"{absolute_data_path_dir}/playlist_songs.json")
    # get videoId, unique
    video_id_all = []
    for params, songinfos in playlist_songs.items():
        if songinfos != 'get_playlist_error':
            for song in songinfos.get("tracks", []):
                video_id = song.get("videoId")
                if video_id: # some videoId are null
                    video_id_all.append(video_id)
    video_id_unique = list(set(video_id_all))


    watch_playlist_data = {}
    # api: get_watch_playlist(videoId)
    def get_watch_playlist(video_id):
        try:
            watch_playlist = ytmusic.get_watch_playlist(video_id)
            watch_playlist_data[video_id] = watch_playlist["lyrics"]
        except Exception as e:
            logger.error(f"Error fetching song for video_id {video_id}: {e}")
            watch_playlist_data[video_id] = "get_watch_playlist_error"
    
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = []
        # count = 0
        for video_id in video_id_unique:
            try:
                futures.append(executor.submit(get_watch_playlist, video_id))
            except Exception as e:
                logger.error(f'get_watch_playlist error: {video_id}, Exception msg: {e}')
            # count += 1
            # if count == 100:
            #     break
        for future in futures:
            future.result()        

    save_to_json(watch_playlist_data, f'{absolute_data_path_dir}/watch_playlist_for_lyrics.json')
    logger.info('step 5 done.')
    gc.collect()
    return watch_playlist_data


# 6. get_lyrics -> csv
def fetch_lyrics():
    logger.info('step 6. get_lyrics: ')

    # load file to get "lyricsId"
    watch_playlist_data = load_from_json(f'{absolute_data_path_dir}/watch_playlist_for_lyrics.json')
    logger.info("load watch_playlist_for_lyrics.json done")
    lyrics_id_all = [value for value in watch_playlist_data.values() if value is not None]
    lyrics_id_unique = list(set(lyrics_id_all))

    # create the csv file to save into
    csv_file_path = f'{absolute_data_path_dir}/lyrics.csv'
    try:
        with open(csv_file_path, mode="x", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["lyricsId", "lyrics", "lyricsSource"]) # write in csv headers
    except FileExistsError:
        pass


    batch_size = 20
    lyrics_batch = []

    # api: get_lyrics(lyricsId)
    def get_lyrics(lyrics_id):
        try:
            lyrics_info = ytmusic.get_lyrics(lyrics_id)
            lyrics_data = [
                lyrics_id,
                lyrics_info.get("lyrics", ""), # if get("lyrics") failed -> ""
                lyrics_info.get("source", "") # if get("source") failed -> ""
            ]
            lyrics_batch.append(lyrics_data)

            # when batch is full, append to csv, and clear lyrics_batch
            if len(lyrics_batch) >= batch_size:
                with open(csv_file_path, mode="a", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerows(lyrics_batch)
                logger.info("batch writing into csv")
                lyrics_batch.clear()  # 清空批次資料

        except Exception as e:
            logger.error(f"Error fetching lyrics for lyrics_id {lyrics_id}: {e}")
            # lyrics_data[lyrics_id] = "get_lyrics_error"
        
    

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = []
        # count = 0
        for lyrics_id in lyrics_id_unique:
            try:
                futures.append(executor.submit(get_lyrics, lyrics_id))
            except Exception as e:
                logger.error(f'lyricId: {lyrics_id} - Exception msg: {e}')
            # count += 1
            # if count == 100:
            #     break
        for future in futures:
            future.result()

    # check if there is batch left (when len(lyrics_batch) < batch_size):
    if lyrics_batch:
        with open(csv_file_path, mode="a", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(lyrics_batch)
        lyrics_batch.clear()


    logger.info('step 6 done.')
    gc.collect()
    return None





# main function ========================================================
# run all the api request functions, and time them
@asset(
    group_name="step1_yt_raw_data",
)
def ytmusicapi_tojson():
    """
    Run all the ytmusic api request functions and save the data to json files.
    Api functions contain: 
        1. get_and_save_mood_categories
        2. fetch_mood_playlists
        3. fetch_playlist_songs
        4. fetch_songs_details
        5. fetch_watch_playlist
        6. fetch_lyrics
    """
    main_start_time = time.time()  # 開始時間
    print("main: time start...")
    logging.info("main: time start...")

    #  run api functions ================
    1.
    start_time = time.time()
    get_and_save_mood_categories()
    end_time = time.time()
    time_msg = f"time spent for 'get_and_save_mood_categories': {(end_time - start_time):.2f} sec"
    print(time_msg)
    logging.info(time_msg)

    # 2.
    start_time = time.time()
    fetch_mood_playlists()
    end_time = time.time()
    time_msg = f"time spent for 'fetch_mood_playlists': {(end_time - start_time):.2f} sec"
    print(time_msg)
    logging.info(time_msg)
    
    # 3.
    start_time = time.time()
    fetch_playlist_songs()
    end_time = time.time()
    time_msg = f"time spent for 'fetch_playlist_songs': {(end_time - start_time):.2f} sec"
    print(time_msg)
    logging.info(time_msg)
    
    # 4.
    start_time = time.time()
    fetch_songs_details()
    end_time = time.time()
    time_msg = f"time spent for 'fetch_songs_details': {(end_time - start_time):.2f} sec"
    print(time_msg)
    logging.info(time_msg)

    # 5.
    start_time = time.time()
    fetch_watch_playlist()
    end_time = time.time()
    time_msg = f"time spent for 'fetch_watch_playlist': {(end_time - start_time):.2f} sec"
    print(time_msg)
    logging.info(time_msg)

    # 6.
    start_time = time.time()
    fetch_lyrics()
    end_time = time.time()
    time_msg = f"time spent for 'fetch_lyrics': {(end_time - start_time):.2f} sec"
    print(time_msg)
    logging.info(time_msg)
    # #  ==============================


    main_end_time = time.time()  # 結束時間
    main_total_time = main_end_time - main_start_time 
    time_msg = f"All done. Time spent: {main_total_time:.2f} sec"
    print(time_msg)
    logging.info(time_msg)

    return today



# ytmusicapi_tojson()






