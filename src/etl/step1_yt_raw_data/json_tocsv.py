from pprint import pprint
import pandas as pd
import json
from datetime import datetime
import os
from dagster import asset, AssetIn

today = datetime.today().strftime('%Y%m%d')
# today = '20241030_test_minimize'

script_dir = os.path.dirname(os.path.abspath(__file__))
relative_data_path_dir = f"../data/{today}"
absolute_data_path_dir = os.path.join(script_dir, relative_data_path_dir) # data/today


def load_from_json(filepath):
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None

# mood_categories.json -> csv
def mood_categories_json_tocsv():
    data = load_from_json(f"{absolute_data_path_dir}/mood_categories.json")
    
    result_data = []
    
    for category, results in data.items():
        for item in results:
            result_data.append({
                "category": category,
                "categoryTitle": item['title'],
                "categoryParams": item['params']
            })
    
    df = pd.DataFrame(result_data)
    df.to_csv(f'{absolute_data_path_dir}/mood_categories.csv', index=False, encoding='utf-8')

# mood_playlist.json -> csv
def mood_playlist_json_tocsv():
    data = load_from_json(f"{absolute_data_path_dir}/mood_playlist.json")
    
    result_data = []

    for params, results in data.items():
        for item in results:
            result_data.append({
                "categoryParams": params,
                "playlistTitle": item['title'],
                "playlistId": item['playlistId'],
                "playlistDescription": item['description']
            })
    df = pd.DataFrame(result_data)
    df.to_csv(f"{absolute_data_path_dir}/mood_playlist.csv", index=False, encoding='utf-8')


# playlist_songs.json -> csv
def playlist_songs_json_tocsv():
    data = load_from_json(f"{absolute_data_path_dir}/playlist_songs.json")
    
    result_data = []

    for playlistId, results in data.items():
        # for item in results:
        if results != 'get_playlist_error':
            for track in results['tracks']:
                result_data.append({
                    "playlistId": playlistId,
                    "playlistDescription": results['description'], # some nulls
                    "playlistTitle": results['title'],
                    "playlistYear": results['year'],
                    "playlistDuration": results['duration'],
                    "playlistTrackCount": results['trackCount'],
                    "playlistRelated": results['related'], #check
                    "songVideoId": track['videoId'], 
                    "songTitleLong": track['title'], 
                    "songArtists": track['artists'], 
                    "songAlbum": track['album'], #check
                    "songLikeStatus": track['likeStatus'], #check
                    # "songInLibrary": track['inLibrary'], #all null
                    "songIsAvailable": track['isAvailable'], #check
                    "songIsExplicit": track['isExplicit'], #check
                    "songVideoType": track['videoType'], #check
                    # "songViews": track['views'], #all null
                    "songDuration": track['duration'],
                    "songDurationSec": track['duration_seconds']
                })
    df = pd.DataFrame(result_data)
    df.to_csv(f"{absolute_data_path_dir}/playlist_songs.csv", index=False, encoding='utf-8')



# songs_details.json -> csv
def songs_details_json_tocsv():
    data = load_from_json(f"{absolute_data_path_dir}/songs_details.json")
    
    result_data = []

    for videoId, results in data.items():
        # 有時會有results結果為get_song_error而非字典的情形
        if isinstance(results,dict): 
            result_data.append({
                "songVideoId": videoId,
                "playabilityStatus": results['playabilityStatus']['status'],
                "songTitleShort": results['videoDetails']['title'],
                "songDurationSec2": results['videoDetails']['lengthSeconds'],
                "channelId": results['videoDetails']['channelId'],
                "viewCount": results['videoDetails']['viewCount'], 
                "author": results['videoDetails']['author'],
                # "musicVideoType": (results['videoDetails']['musicVideoType'] or "musicVideoType_none"),
                "publishDate": results['microformat']['microformatDataRenderer']['publishDate'],
                "songCategory": results['microformat']['microformatDataRenderer']['category'],
                "uploadDate": results['microformat']['microformatDataRenderer']['uploadDate']
            })
        else:
            print(f"vidoeId: {videoId} - results: {results}")
            
    df = pd.DataFrame(result_data)
    df.to_csv(f"{absolute_data_path_dir}/songs_details.csv", index=False, encoding='utf-8')

# watch_playlist_for_lyrics -> csv
def watch_playlist_for_lyrics_tocsv():
    data = load_from_json(f'{absolute_data_path_dir}/watch_playlist_for_lyrics.json')

    # result_data = []
    # for videoId, infos in data.items():
    #     if isinstance(infos, dict):
    #         result_data.append({
    #             "songVideoId": videoId,
    #             "lyricsId": infos['lyrics']
    #         })
    #     else:
    #         print(f"videoId: {videoId} - infos: {infos}")
    # df = pd.DataFrame(result_data)

    df = pd.DataFrame(data.items(), columns=['songVideoId', 'lyricsId'])
    df = df.dropna(subset=['lyricsId'])
    
    df.to_csv(f"{absolute_data_path_dir}/watch_playlist_for_lyrics.csv", index=False, encoding='utf-8')

# lyrics -> csv
def lyrics_tocsv():
    data = load_from_json(f'{absolute_data_path_dir}/lyrics.json')
    result_data = []
    for lyricsId, infos in data.items():
        # if lyricsId != 'null':
        if infos != 'get_lyrics_error':
            result_data.append({
                "lyricsId": lyricsId,
                "lyrics": infos['lyrics'],
                "lyricsSource": infos['source']
            })
    df = pd.DataFrame(result_data)
    df.to_csv(f"{absolute_data_path_dir}/lyrics.csv", index=False, encoding='utf-8')




# run all functions:

@asset(
    group_name="step1_yt_raw_data",
    deps=["ytmusicapi_tojson"],
)
def json_tocsv():
    """
    convert json files to csv files respectively.
    """
    mood_categories_json_tocsv()
    mood_playlist_json_tocsv()
    playlist_songs_json_tocsv()
    songs_details_json_tocsv()
    watch_playlist_for_lyrics_tocsv()
    # lyrics_tocsv()



# json_tocsv()