import pandas as pd
from sqlalchemy import create_engine
import pandas as pd
import requests
import re
import time

from .env_manager import EnvManager

class OnlineFeatureManager:
    def __init__(self, 
                 rag_features : list = [
            'rag_feature:song',
            'rag_feature:artist',
            'rag_feature:category',
            'rag_feature:playlist',
            'rag_feature:playlistDescription',
            'rag_feature:language',
            'rag_feature:publishDate',
            'rag_feature:platform',
            'rag_feature:songDuration',
            'rag_feature:url',
            'rag_feature:lyrics'
        ]):
        self.rag_features = rag_features
        self.env_manager = EnvManager()
        pg_id = self.env_manager.get("POSTGRES_ID")
        pg_pw = self.env_manager.get("POSTGRES_PW")
        pg_host = self.env_manager.get("POSTGRES_HOST")
        pg_port = self.env_manager.get("POSTGRES_PORT")
        pg_db = self.env_manager.get("POSTGRES_RAG_DB")

        self.postgres_conn_str = f"postgresql://{pg_id}:{pg_pw}@{pg_host}:{pg_port}/{pg_db}"
        self.feature_server_url = f"http://rag-feast-feast-feature-server:80/get-online-features"
        self.engine = create_engine(self.postgres_conn_str)
 

    def get_entity_ids(self, table_name="public.merge_demo") -> list:
        '''From PostgreSQL'''
        query = f"SELECT DISTINCT id FROM {table_name};"
        entity_ids_df = pd.read_sql(query, con=self.engine)

        self.engine.dispose()
        
        return list(entity_ids_df['id'])

    def fetch_online_features(self, entity_ids):
        data = {
            "features": self.rag_features,
            "entities": {
                "id": entity_ids
            }
        }

        retry_attempts = 10
        for attempt in range(retry_attempts):
            try:
                response = requests.post(self.feature_server_url, json=data)
                response.raise_for_status() 

                if response.status_code == 200:
                    json_data = response.json()
                    feature_names = json_data['metadata']['feature_names']
                    values = [item['values'] for item in json_data['results']]
                    timestamps = [json_data['results'][1]['event_timestamps'][0]]  # 取得 timestamps

                    df_values = pd.DataFrame(values).transpose()
                    df_timestamp = pd.DataFrame(timestamps * (df_values.last_valid_index() + 1), columns=['event_timestamp'])

                    df_values.columns = feature_names 
                    df_final = pd.concat([df_values, df_timestamp], axis=1)  

                    return df_final

                else:
                    raise Exception(f"Request failed with status code: {response.status_code}")

            except ConnectionError as e:
                    print(f"Connection error: {e}, retrying {attempt + 1}/{retry_attempts}")
                    time.sleep(3)  # 

            raise ConnectionError(f"Failed to connect to {self.feature_server_url} after {retry_attempts} attempts")
        
    def rag_features_final_cleaning(self,
                                    df_to_clean : pd.DataFrame,
                                    sec_to_min_schema : list = ['songDuration'],
                                    re_index_col : list = ['id', 'platform', 'artist', 'language', 'playlistDescription', 'category', 'song', 'url', 'songDuration', 'playlist', 'publishDate', 'lyrics', 'event_timestamp'],
                                    metadata_schema : list = ['platform', 'artist', 'language', 'category', 'song', 'url', 'songDurationMinute', 'playlist', 'publishDate', 'lyrics'],
                                    embedding_schema : list = ['platform', 'artist', 'language', 'category', 'song', 'songDurationMinute', 'playlist', 'publishDate'],
                                    id_schema : list = ['id']
                                    ):
        # embedding amount
        df = df_to_clean.sample(n=100, replace=True)
        #


        for coln in sec_to_min_schema:
            df.loc[:, coln] = df[coln] / 60
            df.loc[:, coln] = df[coln].apply(lambda x: None if x == 0 else x)


        df = df.reindex(columns = re_index_col)

        for coln in sec_to_min_schema:
            df.rename(columns={coln: f'{coln}Minute'}, inplace=True)

        cp_set = re.compile(r'^\{(.*)\}$')
        ts_set = re.compile(r'^(\d\d\d\d-\d\d-\d\d)(T)(.*)$')
        for col in list(df.columns):
            df[col] = df[col].astype(str).apply(lambda x : re.match(cp_set, x).group(1) if re.match(cp_set, x) else x)
            df[col] = df[col].astype(str).apply(lambda x : x.replace('\"', ''))
            df[col] = df[col].astype(str).apply(lambda x : re.match(ts_set, x).group(1) if re.match(ts_set, x) else x)

        rag_on_line_feature = df.to_dict(orient='index')

        dict_cleaned = {}
        for _, feature_dict in rag_on_line_feature.items():

            dict_cleaned_value = {}
            documents = ""
            feature_metadata = {}
            feature_for_embedding = ""
            id_p = ""

            for schema, feature in feature_dict.items():

                documents += (feature + ",")
                
                if schema in metadata_schema:
                    feature_metadata[schema] = feature 

                if schema in embedding_schema:                                      
                    feature_for_embedding += feature

                if schema in id_schema:
                    id_p = feature

            dict_cleaned_value['documents'] = documents.rstrip(',')  # 去掉最後的逗號
            dict_cleaned_value['metadata'] = feature_metadata
            dict_cleaned_value['for_embedding'] = feature_for_embedding

            # 將清理後的值存入字典，這裡要確保是放在外層循環中
            dict_cleaned[f'music-{id_p}'] = dict_cleaned_value

        return dict_cleaned
