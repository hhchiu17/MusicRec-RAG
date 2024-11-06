import pandas as pd
import configparser
from sqlalchemy import (
    create_engine
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


def read_data(table_name, engine, fetchDate):
    try:
        query = f'''SELECT * FROM {table_name} WHERE "fetchDate"::Date = '{fetchDate}';'''
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print(f"發生錯誤: {e}")


# if __name__ == "__main__":
#     postgre_path = "C:\\Users\\student\\rag_data_clean\\data_merge_code\\data_merge_p2\\config.ini"

#     # extract
#     engine = connect_to_database(postgre_path)
#     # data_yt = read_data("yt_demo", engine)
#     fetchDate = '2024-10-09'
#     data_sp = read_data("sp_demo", engine, fetchDate)
#     print(data_sp)