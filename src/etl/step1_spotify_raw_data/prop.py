import logging
import pandas as pd
import numpy as np
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
from datetime import datetime

class RetryLimitExceededError(Exception):
    pass

def initialize_sp_client(id: str, secret: str):
    sp_client = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(client_id=id,
                                                           client_secret=secret),
        requests_session=False,
        requests_timeout=None,
        proxies=None,
        retries=10,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
        language=None)
    return sp_client

def generate_logger(filename: str):
    today = datetime.now().strftime('%Y%m%d')
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    relative_data_path_dir = f"../data/{today}"
    absolute_data_path_dir = os.path.join(script_dir, relative_data_path_dir) # data/today

    if os.path.exists(absolute_data_path_dir) == False:
        os.makedirs(absolute_data_path_dir)

    filename = filename.split('\\')[-1]

    # 確保每次都建立新的 logger
    logger = logging.getLogger(filename)
    logger.setLevel(logging.INFO)

    # 如果 logger 沒有 handlers，才添加新的 handler
    if not logger.hasHandlers():
        log_file = os.path.join(absolute_data_path_dir, f'{filename}.log')
        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        stream_handler = logging.StreamHandler()

        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

        # 添加 handlers
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger

# Splitting the playlist.csv file into 6 sub-files
def split_playlist_csv(df: pd.DataFrame, num_clients: int):
    sub_csv_paths = []
    sub_dfs = np.array_split(df, num_clients)  # Split the dataframe into 6 parts

    for i, sub_df in enumerate(sub_dfs):
        sub_csv_path = f'sub_playlist_{i+1}.csv'
        sub_df.to_csv(sub_csv_path, index=False)
        sub_csv_paths.append(sub_csv_path)
    
    return sub_csv_paths