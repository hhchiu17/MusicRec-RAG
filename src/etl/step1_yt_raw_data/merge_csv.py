import pandas as pd
from datetime import datetime
from dagster import asset, AssetIn, RunRequest, AssetSelection, sensor
import os


today = datetime.today().strftime('%Y%m%d') #yyyymmdd
# today = '20241010'

fetch_date = datetime.today().strftime('%Y-%m-%d')
# fetch_date='2024-10-10'

script_dir = os.path.dirname(os.path.abspath(__file__))
relative_data_path_dir = f"../data/{today}"
absolute_data_path_dir = os.path.join(script_dir, relative_data_path_dir)


@asset(
    group_name="step1_yt_raw_data",
    deps=["json_tocsv"],
)
def merge_csv():
    """
    merge all csv and generate yt_category_songlist.csv
    """


    df_category = pd.read_csv(f"{absolute_data_path_dir}/mood_categories.csv", encoding="utf-8")
    df_playlist = pd.read_csv(f"{absolute_data_path_dir}/mood_playlist.csv", encoding="utf-8")
    df_songs = pd.read_csv(f"{absolute_data_path_dir}/playlist_songs.csv", encoding="utf-8")
    df_details = pd.read_csv(f"{absolute_data_path_dir}/songs_details.csv", encoding="utf-8")

    # version1: keep the lyrics parts
    df_watchplaylist = pd.read_csv(f"{absolute_data_path_dir}/watch_playlist_for_lyrics.csv", encoding="utf-8")
    df_lyrics = pd.read_csv(f"{absolute_data_path_dir}/lyrics.csv", encoding = "utf-8")


    # merge: mood_categories, mood_playlist, playlist_songs, songs_details, watch_playlist_for_lyrics, lyrics
    merge_temp = (pd.merge(df_category, df_playlist, on='categoryParams', how='inner')
               .drop(columns=['playlistDescription'])
               
               .merge(df_songs, on='playlistId', how='inner')
               .drop(columns=['playlistRelated', 'songAlbum', 'songLikeStatus', 'songIsExplicit', 'songVideoType', 'songDuration'])
               
               .merge(df_details, on='songVideoId', how='left').drop(columns=['playlistTitle_y'])
               .rename(columns={'playlistTitle_x': 'playlistTitle'})
               )
    
    # add songURL column
    def generate_url(videoId):
        if pd.notna(videoId): 
            return f'https://music.youtube.com/watch?v={videoId}'
        else:
            return ''
    merge_temp['songURL'] = merge_temp['songVideoId'].apply(generate_url)

    # add fetchDate column
    merge_temp['fetchDate'] = fetch_date
 
    # version1: keep the lyrics parts
    # merge: merge_temp, watch_playlist_for_lyrics, lyrics
    merge_final = (pd.merge(merge_temp, df_watchplaylist, on='songVideoId', how='left')
                   .merge(df_lyrics, on='lyricsId', how='left')
                   )


    merge_final.to_csv(f"{absolute_data_path_dir}/yt_category_songlist.csv", encoding="utf-8", index=False)

    # version2: give up on lyrics parts:
    # merge_temp['lyricsId'] = ''
    # merge_temp['lyrics'] = ''
    # merge_temp['lyricsSource'] = ''
    # merge_temp.to_csv(f"{absolute_data_path_dir}/yt_category_songlist.csv", encoding="utf-8", index=False)
    


# merge_csv()