import pandas as pd
import datetime


from .env_manager import EnvManager
from .api_manager import OpenAPIManager
from .chromadb_client_manager import ChromaDBClientManager
from .feast_online_feature_getter import OnlineFeatureManager

from dagster import asset 
from dagster import AssetExecutionContext, MetadataValue, MaterializeResult
from dagster import AssetIn, RetryPolicy, Backoff, Jitter

class Application:
    def __init__(self):
        self.env_manager = EnvManager()
        self.api_manager = OpenAPIManager(self.env_manager)
        self.db_client_manager = ChromaDBClientManager()
        self.online_feature_manager = None
        self.online_feature = None

    def rag_online_features(self):
        self.online_feature_manager = OnlineFeatureManager()
        online_feature_manager = self.online_feature_manager

        entity_ids : list = online_feature_manager.get_entity_ids()      
        self.online_feature = online_feature_manager.fetch_online_features(entity_ids = entity_ids)

    def rag_bf_embedding(self):
        df_rag_for_cleaned = self.online_feature
        self.online_feature = self.online_feature_manager.rag_features_final_cleaning(df_to_clean = df_rag_for_cleaned)

    def embedding_and_add_to_chroma(self, 
            collection_name : str = None, 
            rag_on_line_feature : pd.DataFrame = None):
        
        collection_name = collection_name if collection_name is not None else datetime.date.today().strftime('%Y%m%d')
        collection = self.db_client_manager.select_collection_for_add_data(collection_name, embedding_model=self.api_manager.embedding_model)
        rag_on_line_feature = self.online_feature if rag_on_line_feature is None else rag_on_line_feature
        self.db_client_manager.embedding_add_to_collection(collection, 
                                                          rag_on_line_feature_cleaned=rag_on_line_feature)

@asset(retry_policy=RetryPolicy(
        max_retries=3,
        delay=10,  # 200ms
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS),
        group_name="step3_embedding")
def main():
    app = Application()
    app.rag_online_features()
    app.rag_bf_embedding()
    app.embedding_and_add_to_chroma()