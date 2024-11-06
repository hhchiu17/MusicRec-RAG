from dagster import asset
from .prop import generate_logger
from time import sleep
from .spotify_category_songlist import spotify_category_songlist
# 變數 ==========================================
logger = generate_logger(__file__)

# main function =================================
@asset(
    group_name="step1_spotify_raw_data",
    deps=[spotify_category_songlist],
)
def wait_between_assets():
    """
    請求完spotify_category_songlist後，等待3小時再請求spotify_song_features
    """
    logger.info("wait_between_assets")
    sleep(10800)
    logger.info("wait_between_assets done")