import pandas as pd
import configparser
from sqlalchemy import (
    create_engine,
    inspect,
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


def insert_data(engine, table_name, df):
    df.to_sql(table_name, engine, if_exists="append", index=False)
    print("Data has been successfully inserted into PostgreSQL!")


def load_postgre_data(data, path):
    # Step 1: Connect to PostgreSQL database
    engine = connect_to_database(path)

    # Step 3: Insert data if table exists
    table_name = "merge_demo"
    insert_data(engine, table_name, data)
