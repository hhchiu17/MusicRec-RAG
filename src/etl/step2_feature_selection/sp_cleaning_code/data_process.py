import pandas as pd

def clean_data(data):
    # 將空字串轉為 NaN
    data.replace("", pd.NA, inplace=True)

    # 將空值 NaN 填充為適當的預設值
    numeric_columns = ['duration_ms', 'time_signature', 'danceability', 'energy', 
                       'key', 'loudness', 'mode', 'speechiness', 'acousticness', 
                       'instrumentalness', 'liveness', 'valence', 'tempo']

    # 將非數值的資料轉為 NaN，但不填充 NaN
    data[numeric_columns] = data[numeric_columns].apply(
        pd.to_numeric, errors='coerce'
    )

    # 填充其他類型欄位的 NaN（如字串）為空字串或適當的預設值
    data.fillna({"artist": "", "category": "", "playlistName": "", "track": ""}, inplace=True)

    return data







def extract_artist_name(artist_list):
    try:
        # 如果是字串，轉換為列表
        artist_data = eval(artist_list) if isinstance(artist_list, str) else artist_list
        if (
            isinstance(artist_data, list)
            and len(artist_data) > 0
            and isinstance(artist_data[0], dict)
        ):
            return artist_data[0].get(
                "name", None
            )  # 從字典中提取 'name'，如果是 None 則保留 None
        else:
            return None  # 保留 None 形式
    except (SyntaxError, ValueError):
        return None  # 處理無效格式






def group_data(data):

    try:
        df_grouped = data.groupby(["id"], as_index=False).agg(
            {
                "track": "first",
                "artist": lambda x: list(set(x)),
                "category": lambda x: list(set(x)),
                "playlistName": lambda x: list(set(x)),
                "trackURL": "first",
                "date_added": "first",
                "duration_ms": "first",
                "time_signature": "first", 
                "danceability": "first", 
                "energy": "first", 
                "key": "first", 
                "loudness": "first", 
                "mode": "first", 
                "speechiness": "first", 
                "acousticness": "first", 
                "instrumentalness": "first", 
                "liveness": "first", 
                "valence": "first", 
                "tempo": "first", 
                "fetchDate": "first"
            }
        )
        return df_grouped
    except KeyError as e:
        print(
            f"Error: One or more required columns are not found in the DataFrame for grouping: {e}"
        )
    except Exception as e:
        print(f"An unexpected error occurred while grouping the data: {e}")
