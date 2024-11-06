import pandas as pd
import re


def select_required_columns(data, required_columns):
    """
    Select and return only the specified columns from the DataFrame.

    Parameters:
    data (pd.DataFrame): Original DataFrame.
    required_columns (list of str): List of columns to retain in the DataFrame.

    Returns:
    pd.DataFrame: DataFrame containing only the required columns.
    """
    try:
        return data[required_columns]
    except KeyError as e:
        print(
            f"Error: One or more required columns are not found in the DataFrame: {e}"
        )
    except Exception as e:
        print(f"An unexpected error occurred while selecting required columns: {e}")


def group_data(data, required_columns):
    """
    Group the data by 'songTitleShort' and 'author', aggregating other columns as specified.

    Parameters:
    data (pd.DataFrame): DataFrame to be grouped.
    required_columns (list of str): List of columns to retain in the DataFrame.

    Returns:
    pd.DataFrame: Grouped DataFrame.
    """
    try:
        grouped_data = (
            data[required_columns]
            .groupby(["songVideoId"], as_index=False)
            .agg(
                {
                    "songTitleShort":"first",
                    "author": lambda x: list(set(x.dropna())),
                    "songDurationSec": "first",
                    "viewCount": "first",
                    "publishDate": "first",
                    "categoryTitle": lambda x: list(set(x.dropna())),
                    "playlistTitle": lambda x: list(set(x.dropna())),
                    "playlistDescription": lambda x: list(set(x.dropna())),
                    "songURL": "first",
                    "lyrics": "first",
                    "lyricsSource": "first",
                    "fetchDate": "first",
                }
            )
        )
        return grouped_data
    except KeyError as e:
        print(
            f"Error: One or more required columns are not found in the DataFrame for grouping: {e}"
        )
    except Exception as e:
        print(f"An unexpected error occurred while grouping the data: {e}")


def filter_categories(data, categories_to_remove):
    """
    Filter out rows where 'categoryTitle' contains any of the specified categories to remove.

    Parameters:
    data (pd.DataFrame): DataFrame to be filtered.
    categories_to_remove (set): Set of category titles to be removed.

    Returns:
    pd.DataFrame: Filtered DataFrame.
    """
    try:
        filtered_data = data[
            ~data["categoryTitle"].apply(
                lambda x: any(cat in categories_to_remove for cat in x)
            )
        ]
        return filtered_data
    except KeyError as e:
        print(f"Error: Column {e} not found in the DataFrame for filtering: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while filtering the data: {e}")


def filter_playlist_titles(data, playlist_keywords_to_remove):
    """
    Filter out rows where 'playlistTitle' contains any of the specified keywords to remove.

    Parameters:
    data (pd.DataFrame): DataFrame to be filtered.
    playlist_keywords_to_remove (list of str): List of keywords to be removed from 'playlistTitle'.

    Returns:
    pd.DataFrame: Filtered DataFrame.
    """
    try:
        pattern = "|".join(playlist_keywords_to_remove)
        filtered_data = data[
            ~data["playlistTitle"].apply(
                lambda x: any(
                    re.search(pattern, playlist, re.IGNORECASE) for playlist in x
                )
            )
        ]
        return filtered_data
    except KeyError as e:
        print(
            f"Error: Column {e} not found in the DataFrame for filtering playlist titles: {e}"
        )
    except Exception as e:
        print(f"An unexpected error occurred while filtering playlist titles: {e}")
