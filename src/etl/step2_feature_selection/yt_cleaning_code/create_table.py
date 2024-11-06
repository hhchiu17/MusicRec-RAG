import os
import pandas as pd
import configparser
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    String,
    Float,
    MetaData,
    DateTime,
)


def connect_to_database(path):
    config = configparser.ConfigParser()
    config.read(path)

    db_user = config["postgres"]["user"]
    db_password = config["postgres"]["password"]
    db_host = config["postgres"]["host"]
    db_port = config["postgres"]["port"]
    db_name = config["postgres"]["db_name"]

    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(db_url)
    print("Connected to database successfully")
    return engine


def create_table(engine, table_name):
    metadata = MetaData()
    yt_demo_table = Table(
        table_name,
        metadata,
        Column("songVideoId", String),
        Column("songTitleShort", String),
        Column("author", String),
        Column("categoryTitle", String),
        Column("playlistTitle", String),
        Column("playlistDescription", String),
        Column("songURL", String),
        Column("language", String),
        Column("publishDate", DateTime),
        Column("songDurationSec", Integer),
        Column("viewCount", Float),
        Column("lyrics", String),
        Column("lyricsSource", String),
        Column("fetchDate", DateTime),
    )
    metadata.create_all(engine)
    print("Table created successfully")


if __name__ == "__main__":
    path = "config.ini"
    # Step 1: Connect to PostgreSQL database
    engine = connect_to_database(path)

    # Step 2: Create the table
    table_name = "yt_demo_"
    create_table(engine, table_name)
