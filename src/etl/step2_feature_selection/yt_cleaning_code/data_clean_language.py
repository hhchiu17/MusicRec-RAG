import pandas as pd
import re


def detect_language(lyrics, threshold):
    """
    Detect the language of the given lyrics based on character analysis.

    Parameters:
    lyrics (str): Lyrics to be analyzed.
    threshold (float): Threshold to decide if the text is predominantly non-English.

    Returns:
    str: Detected language.
    """

    def contains_korean(text):
        korean_pattern = re.compile(r"[\uac00-\ud7af\u3130-\u318f]")
        return bool(korean_pattern.search(text))

    def contains_japanese(text):
        japanese_pattern = re.compile(r"[\u3040-\u30ff\u31f0-\u31ff\u3000-\u303f]")
        return bool(japanese_pattern.search(text))

    def contains_chinese(text):
        chinese_pattern = re.compile(r"[\u4e00-\u9fff]")
        return bool(chinese_pattern.search(text))

    def count_english(text):
        english_pattern = re.compile(r"[A-Za-z]")
        return len(english_pattern.findall(text))

    def count_non_english(text):
        non_english_pattern = re.compile(r"[^A-Za-z]")
        return len(non_english_pattern.findall(text))

    if lyrics is None or pd.isnull(lyrics):
        return "Unknown"
    lyrics = str(lyrics)
    total_chars = len(lyrics)

    if total_chars == 0:
        return "Unknown"

    if contains_korean(lyrics):
        return "Korean"
    elif contains_japanese(lyrics):
        return "Japanese"
    elif contains_chinese(lyrics):
        return "Chinese"

    non_english_count = count_non_english(lyrics)
    english_count = count_english(lyrics)

    non_english_ratio = non_english_count / (non_english_count + english_count)
    english_ratio = english_count / (non_english_count + english_count)

    if non_english_ratio > threshold:
        return "Other"
    elif english_ratio > threshold:
        return "English"
    else:
        return "Other"


def apply_language_detection(data, lyrics_column, threshold=0.6):
    """
    Apply language detection to the specified lyrics column of a DataFrame.

    Parameters:
    data (pd.DataFrame): DataFrame containing the lyrics column.
    lyrics_column (str): Name of the column containing lyrics to be analyzed.
    threshold (float): Threshold to decide if the text is predominantly non-English.

    Returns:
    pd.DataFrame: DataFrame with an additional 'language' column indicating detected language.
    """
    try:
        data["language"] = data[lyrics_column].apply(
            lambda x: detect_language(x, threshold)
        )
        return data
    except KeyError as e:
        print(
            f"Error: Column {e} not found in the DataFrame for language detection: {e}"
        )
    except Exception as e:
        print(f"An unexpected error occurred while detecting language: {e}")
