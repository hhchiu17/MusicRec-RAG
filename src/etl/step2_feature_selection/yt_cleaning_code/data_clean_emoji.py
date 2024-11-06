import pandas as pd
import re


def convert_dates(data, columns):
    """
    Convert date columns by removing time information and converting them to datetime format.

    Parameters:
    data (pd.DataFrame): DataFrame containing the columns to be processed.
    columns (list of str): List of column names to be processed.

    Returns:
    pd.DataFrame: DataFrame with updated date columns.
    """
    try:
        for column in columns:
            if column in data.columns:
                data[column] = data[column].replace(
                    to_replace=r"T.*", value="", regex=True
                )
                data[column] = pd.to_datetime(data[column], errors="coerce")
        return data
    except KeyError as e:
        print(f"Error: Column {e} not found in the DataFrame.")
    except Exception as e:
        print(f"An unexpected error occurred while converting dates: {e}")


def remove_emoji(text):
    """
    Remove emojis from the given text.

    Parameters:
    text (str): Text from which emojis need to be removed.

    Returns:
    str: Text with emojis removed.
    """
    try:
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"  # Emoticons
            "\U0001F300-\U0001F5FF"  # Symbols & Pictographs
            "\U0001F680-\U0001F6FF"  # Transport & Map Symbols
            "\U0001F1E0-\U0001F1FF"  # Flags
            "\U00002500-\U00002BEF"  # Misc symbols
            "\U00002702-\U000027B0"  # Dingbats
            "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
            "\U0001F300-\U0001F3FA"  # Weather, landscape, and sky symbols
            "\U0001F650-\U0001F67F"  # Extended pictographs
            "\U0001F6C0-\U0001F6FF"  # Transport symbols extension
            "\U0001F004-\U0001F0FF"  # Playing cards and symbols
            "\U0001F191-\U0001F251"  # Enclosed characters
            "\U00002600-\U000026FF"  # Various symbols (fixed range format)
            "\U00002700-\U000027BF"  # Dingbats (fixed range format)
            "\U0001F910-\U0001F9FF"  # Emoticons and gestures
            "]+",
            flags=re.UNICODE,
        )
        return emoji_pattern.sub(r"", text)
    except Exception as e:
        print(f"An unexpected error occurred while removing emojis: {e}")
        return text


def clean_dataframe(df):
    """
    Clean an entire DataFrame by removing emojis from each cell.

    Parameters:
    df (pd.DataFrame): DataFrame to be cleaned.

    Returns:
    pd.DataFrame: Cleaned DataFrame.
    """
    try:
        cleaned_df = df.apply(
            lambda x: (
                x.map(
                    lambda cell: (
                        remove_emoji(str(cell)) if isinstance(cell, str) else cell
                    )
                )
                if x.dtype == "O"
                else x
            )
        )
        return cleaned_df
    except Exception as e:
        print(f"An unexpected error occurred while cleaning the DataFrame: {e}")
        return df
