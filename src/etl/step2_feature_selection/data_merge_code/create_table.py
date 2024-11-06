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
    sp_demo_table = Table(
        table_name,
        metadata,
        Column("id", String),
        Column("song", String),
        Column("artist", String),
        Column("category", String),
        Column("playlist", String),
        Column("playlistDescription", String),
        Column("url", String),
        Column("language", String),
        Column("publishDate", DateTime),
        Column("platform", String),
        Column("songDuration", Float),
        Column("fetchDate", DateTime),
        Column("time_signature", Integer),
        Column("danceability", Float),
        Column("energy", Float),
        Column("key", Integer),
        Column("loudness", Float),
        Column("mode", Integer),
        Column("speechiness", Float),
        Column("acousticness", Float),
        Column("instrumentalness", Float),
        Column("liveness", Float),
        Column("valence", Float),
        Column("tempo", Float),
        Column("viewCount", Float),
        Column("lyrics", String),
        Column("lyricsSource", String)
    )
    metadata.create_all(engine)
    print("Table created successfully")


if __name__ == "__main__":
    path = "config.ini"
    # Step 1: Connect to PostgreSQL database
    engine = connect_to_database(path)

    # Step 2: Create the table
    table_name = "merge_demo"
    create_table(engine, table_name)
