import pandas as pd
from datetime import datetime
import re


def process_yt_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    處理 YouTube 資料：
    1. 添加 'Platform' 欄位，值為 'youtube_music'。
    2. 將欄位名稱加上 'yt_' 作為前綴。
    3. 對 yt_categoryTitle 欄位進行清理與替換。
    4. 添加 'fetchDate' 欄位，記錄當前的時間戳。
    
    Args:
        data (pd.DataFrame): 原始 YouTube 資料。

    Returns:
        pd.DataFrame: 處理過的 YouTube 資料。
    """
    # 添加 Platform 欄位
    data["Platform"] = "youtube_music"

    # 將欄位名稱加上 'yt_' 作為前綴
    data.columns = ["yt_" + col for col in data.columns]

    # 定義要替換的映射
    replacement_map = {
        "Taiwan Music": "Mandopop",
        "Hong Kong Music": "Mandopop",
        "Feel Good": "Mood",
        "Sad": "Mood",
        "Romance": "Love",
        "Country & Americana": "Country",
        "Dance & Electronic": "Dance/Electronic",
        "J-Pop": "J-Tracks",
    }

    def clean_and_replace(text):
        """
        清理文字並根據映射進行替換。
        """
        if pd.isna(text):
            return []  # 保持 NaN 為空列表

        # 移除多餘符號，轉換為列表
        text = re.sub(r"[{}\"]", "", text)  # 移除大括號與多餘的引號
        categories = [cat.strip().strip('"') for cat in text.split(",")]  # 分割並去除空白

        # 進行替換
        replaced = [replacement_map.get(cat, cat) for cat in categories]

        return list(set(replaced))  # 去除重複項目
    

    def clean_artist(artist):
        if pd.isna(artist):
            return []  # 如果是 NaN，返回空列表

        # 去除大括號和引號，並分割成乾淨的字串
        cleaned = artist.strip("{}").replace('"', "").split(",")
        return [a.strip().strip('"') for a in cleaned]  # 去除每個元素的多餘空格

    def string_to_list(value):
        if pd.isna(value):
            return []  # 如果是 NaN，返回空列表

        # 移除大括號並以逗號分隔為列表
        items = value.strip("{}").split(",")

        # 移除每個元素的多餘空格並返回清理後的列表
        return [item.strip().strip('"') for item in items]

    # 對 yt_categoryTitle 欄位進行替換
    data["yt_categoryTitle"] = data["yt_categoryTitle"].apply(clean_and_replace)

    # 對 sp_artist 欄位應用清理函數
    data["yt_author"] = data["yt_author"].apply(clean_artist)

    data["yt_playlistTitle"] = data["yt_playlistTitle"].apply(string_to_list)


    return data


def process_sp_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    處理 Spotify 資料：
    1. 添加 'Platform' 欄位，值為 'spotify'。
    2. 將欄位名稱加上 'sp_' 作為前綴。
    3. 對 sp_category 欄位進行替換與去重。
    4. 添加 'fetchDate' 欄位，記錄當前的時間戳。

    Args:
        data (pd.DataFrame): 原始 Spotify 資料。

    Returns:
        pd.DataFrame: 處理過的 Spotify 資料。
    """
    # 添加 Platform 欄位
    data["Platform"] = "spotify"

    # 將欄位名稱加上 'sp_' 作為前綴
    data.columns = ["sp_" + col for col in data.columns]

    def replace_and_deduplicate(category):
        """
        替換 'Soul' 和 'R&B' 為 'R&B & Soul'，並去重。
        """
        if pd.isna(category):
            return []  # 如果是 NaN，返回空的大括號

        # 移除大括號並將字串分割為類別列表
        categories = category.strip("{}").split(",")
        
        # 進行替換
        categories = [
            "R&B & Soul" if cat.strip() in ["Soul", "R&B"] else cat.strip().strip('"')
            for cat in categories
        ]

        # 去重並返回排序後的字串格式
        return categories
    

    def clean_artist(artist):
        if pd.isna(artist):
            return []  # 如果是 NaN，返回空列表

        # 去除大括號和引號，並分割成乾淨的字串
        cleaned = artist.strip("{}").replace('"', "").split(",")
        return [a.strip().strip('"') for a in cleaned]  # 去除每個元素的多餘空格

    def string_to_list(value):
        if pd.isna(value):
            return []  # 如果是 NaN，返回空列表

        # 移除大括號並以逗號分隔為列表
        items = value.strip("{}").split(",")

        # 移除每個元素的多餘空格並返回清理後的列表
        return [item.strip().strip('"') for item in items]

    # 針對 sp_category 欄位進行替換與去重
    data["sp_category"] = data["sp_category"].apply(replace_and_deduplicate)

    # 對 sp_artist 欄位應用清理函數
    data["sp_artist"] = data["sp_artist"].apply(clean_artist)

    # 對指定欄位sp_playlistName應用轉換函數
    data["sp_playlistName"] = data["sp_playlistName"].apply(string_to_list)

    return data


# 合併邏輯函數
def merge_data_same_song(sp_data, yt_data):
    merged = pd.merge(
        sp_data, yt_data, left_on="sp_id", right_on="yt_songVideoId", how="outer"
    )

    # 展開並去重的函數
    def flatten_and_unique(x):
        items = [
            i
            for sublist in x.dropna()
            for i in (sublist if isinstance(sublist, list) else [sublist])
        ]
        return list(set(items)) if items else None
    
    def flatten_and_join_unique(elements):
        """將 list 中的元素去重後合併為一個以 _ 分隔的字串。"""
        unique_elements = sorted(set(filter(pd.notnull, elements)))
        return "_".join(map(str, unique_elements))
    

    def ensure_hashable(x):
        if isinstance(x, list):
            return tuple(ensure_hashable(i) for i in x)
        return x

    def clean_artist(artist):
        if isinstance(artist, tuple) and len(artist) == 1:
            return artist[0].strip()  # 提取 tuple 中唯一的元素並去除多餘空白
        return artist  # 若非 tuple，則直接返回

    # 合併各欄位
    merged["id"] = (
        merged[["sp_id", "yt_songVideoId"]]
        .bfill(axis=1)
        .apply(lambda row: list(filter(pd.notnull, row)), axis=1)
        .apply(flatten_and_join_unique)
    )
    merged["song"] = merged[["sp_track", "yt_songTitleShort"]].bfill(axis=1).iloc[:, 0].infer_objects(copy=False)

    merged["artist"] = merged[["sp_artist", "yt_author"]].bfill(axis=1).iloc[:, 0].infer_objects(copy=False)

    merged["category"] = merged[["sp_category", "yt_categoryTitle"]].apply(
        flatten_and_unique, axis=1
    )
    merged["playlist"] = merged[["sp_playlistName", "yt_playlistTitle"]].apply(
        flatten_and_unique, axis=1
    )

    merged["url"] = (
        merged[["sp_trackURL", "yt_songURL"]]
        .bfill(axis=1)
        .apply(flatten_and_unique, axis=1)
    )
    merged["publishDate"] = (
        merged[["sp_date_added", "yt_publishDate"]].bfill(axis=1).iloc[:, 0].infer_objects(copy=False)
    )
    merged["platform"] = merged[["sp_Platform", "yt_Platform"]].apply(
        flatten_and_unique, axis=1
    )

    # songDuration 轉換為秒並取最大值
    merged["songDuration"] = merged[["sp_duration_ms", "yt_songDurationSec"]].apply(
        lambda x: max(
            x["sp_duration_ms"] / 1000 if pd.notna(x["sp_duration_ms"]) else 0,
            x["yt_songDurationSec"] if pd.notna(x["yt_songDurationSec"]) else 0,
        ),
        axis=1,
    )

    # 將需要 hashable 型態的欄位進行轉換
    merged["song"] = merged["song"].apply(ensure_hashable)
    merged["artist"] = merged["artist"].apply(ensure_hashable)

    # 新增 fetchDate
    merged["fetchDate"] = datetime.now().strftime("%Y-%m-%d 00:00:00")

    # 刪除原始欄位
    columns_to_drop = [
        "sp_id",
        "yt_songVideoId",
        "sp_track",
        "yt_songTitleShort",
        "sp_artist",
        "yt_author",
        "sp_category",
        "yt_categoryTitle",
        "sp_playlistName",
        "yt_playlistTitle",
        "sp_trackURL",
        "yt_songURL",
        "sp_date_added",
        "yt_publishDate",
        "sp_duration_ms",
        "yt_songDurationSec",
        "sp_Platform",
        "yt_Platform",
        "sp_fetchDate",
        "yt_fetchDate",
    ]
    merged.drop(columns=columns_to_drop, inplace=True, errors="ignore")

    # 去除 sp_ 和 yt_ 前綴的欄位名稱
    merged.columns = [
        col.split("_", 1)[-1] if "_" in col else col for col in merged.columns
    ]

    # 將所有欄位依據 song 和 artist 合併
    agg_functions = {
        col: "first" for col in merged.columns if col not in ["song", "artist"]
    }
    # 對特定欄位進行自定義合併
    agg_functions.update(
        {
            "id": flatten_and_join_unique,
            "category": flatten_and_unique,
            "playlist": flatten_and_unique,
            "platform": flatten_and_unique,
            "url": flatten_and_unique,
            "songDuration": "max",
        }
    )

    # 針對 song 和 artist 相同的資料進行合併
    merged = merged.groupby(["song", "artist"], as_index=False).agg(agg_functions)

    # 重新排列欄位，將合併的 11 個欄位移到最前面
    main_columns = [
        "id",
        "song",
        "artist",
        "category",
        "playlist",
        "playlistDescription",
        "url",
        "language",
        "publishDate",
        "platform",
        "songDuration",
        "fetchDate",
    ]

    remaining_columns = [col for col in merged.columns if col not in main_columns]
    merged = merged[main_columns + remaining_columns]
    merged["artist"] = merged["artist"].apply(clean_artist)

    return merged



