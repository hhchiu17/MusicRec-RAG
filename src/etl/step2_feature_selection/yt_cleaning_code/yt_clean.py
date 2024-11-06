import sys
import io
import pandas as pd
from .data_clean_emoji import convert_dates, clean_dataframe
from .data_process import (
    select_required_columns,
    group_data,
    filter_categories,
    filter_playlist_titles,
)
from .data_clean_language import apply_language_detection
from .python_to_postgre import load_postgre_data
from .mango_to_python import read_mongo_data
import os

from dagster import asset 
from dagster import AssetExecutionContext

from ..sp_cleaning_code.sp_clean import sp_clean


def process_youtube_data(
    data,
    date_columns,
    required_columns,
    categories_to_remove,
    playlist_keywords_to_remove,
):
    """
    Load, process, and select specific columns from YouTube song data.

    Parameters:
    file_path (str): Path to the CSV file containing YouTube song data.
    date_columns (list of str): List of date columns to be processed.
    required_columns (list of str): List of columns to retain in the DataFrame.
    categories_to_remove (set): Set of category titles to be removed.
    playlist_keywords_to_remove (list of str): List of keywords to be removed from 'playlistTitle'.
    output_file_path (str): Path to save the processed CSV file.

    Returns:
    pd.DataFrame: Processed DataFrame.
    """
    try:
        # # Load data
        # mongopath = "C:\\Users\\student\\rag_data_clean\\yt_cleaning_p3\\config.ini"
        # mongo_client = connect_to_mongodb(mongopath)
        # mongodb_data = read_mongodb_data(mongo_client, mongopath)
        # data = pd.DataFrame(list(mongodb_data))
        # if data is None:
        #     return None

        # Convert dates for specified columns
        data = convert_dates(data, date_columns)
        if data is None:
            return None

        # Select required columns
        data = select_required_columns(data, required_columns)
        if data is None:
            return None

        # Group the data
        data = group_data(data, required_columns)
        if data is None:
            return None

        # Filter out unwanted categories
        data = filter_categories(data, categories_to_remove)
        if data is None:
            return None

        # Filter out unwanted playlist titles
        data = filter_playlist_titles(data, playlist_keywords_to_remove)
        if data is None:
            return None

        # Clean the DataFrame by removing emojis
        data = clean_dataframe(data)

        # Detect language of lyrics
        data = apply_language_detection(data, lyrics_column="lyrics")
        data = data[data["language"] != "Other"]

        # Handle rows with language "Unknown"
        unknown_data = data[data["language"] == "Unknown"].copy()
        unknown_data["language"] = unknown_data["songTitleShort"].apply(
            lambda x: apply_language_detection(
                pd.DataFrame([x], columns=["lyrics"]), lyrics_column="lyrics"
            )["language"].iloc[0]
        )
        data.loc[data["language"] == "Unknown", "language"] = unknown_data["language"]
        data = data[data["language"] != "Other"]

        return data
    except Exception as e:
        print(f"An unexpected error occurred while processing YouTube data: {e}")

@asset(
        group_name="step2_feature_selection",
        deps=[sp_clean]
)
def yt_clean(context: AssetExecutionContext):
    """
    Main function to orchestrate loading, processing, and selecting columns from YouTube song data.
    """
    try:
        # Define columns for processing
        date_columns = ["publishDate", "uploadDate"]
        required_columns = [
            "songTitleShort",
            "author",
            "songVideoId",
            "songTitleLong",
            "songDurationSec",
            "viewCount",
            "publishDate",
            "uploadDate",
            "categoryTitle",
            "playlistTitle",
            "playlistDescription",
            "songURL",
            "lyrics",
            "lyricsSource",
            "fetchDate",
        ]
        categories_to_remove = {"Bollywood & Indian", "Indonesian", "Thai"}
        playlist_keywords_to_remove = [
            "Iraqi",
            "Indonesi",
            "Bollywood",
            "Arabic",
            "Khaleeji",
            "Araby",
            "Egyptian",
            "Indian",
            "Ukrainian",
            "Turkish",
            "Russia",
            "Afro",
            "Arabesk",
            "Bhojpuri",
        ]

        script_dir = os.path.dirname(os.path.abspath(__file__))
        path= os.path.join(script_dir, "config.ini") 



        # extract
        fetch_date_yt, raw_data = read_mongo_data(path)

        # transform
        trans_data = process_youtube_data(
            raw_data,
            date_columns,
            required_columns,
            categories_to_remove,
            playlist_keywords_to_remove,
        )

        # loading
        # trans_data.to_csv("yt_data_cleaning_20241015.csv", encoding="utf-8")
        load_postgre_data(trans_data, path)

        context.log.info(f"yt music data cleaned successfully, fetch date: {fetch_date_yt}")


        return fetch_date_yt

    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except Exception as e:
        print(f"An unexpected error occurred in the main function: {e}")


# if __name__ == "__main__":
#     sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
#     yt_clean()
