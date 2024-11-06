import chromadb
from datetime import datetime

from .env_manager import EnvManager
from .api_manager import OpenAPIManager


class ChromaDBClientManager:
    def __init__(self):
        self.env_manager = EnvManager()
        self.client = self._initialize_client(env_manager=self.env_manager)
        self.embedding_model = None
        self.collection = None

    def _initialize_client(self, env_manager):
        client = chromadb.HttpClient(
            host = "chroma",
            port = 8000,
            tenant = "pikamongo",
            database = "rag"
        )

        return client

    def select_collection_for_add_data(self, name: str, embedding_model: str):
        formatted_date = datetime.today().strftime('%Y-%m-%d')

        self.collection = self.client.get_or_create_collection(name=name, 
                                                    metadata={"embedding_model" : embedding_model,
                                                              "created_date" : formatted_date})
        return self.collection

    def embedding_add_to_collection(self, 
                                    collection, 
                                    rag_on_line_feature_cleaned: dict = None):

        for id, dict_feature_value in rag_on_line_feature_cleaned.items():
            feature_for_embedding = dict_feature_value.get("for_embedding", "")
            feature_metadata = dict_feature_value.get("metadata", {})
            documents = dict_feature_value.get("documents", {})
            embeddings = None
            id_p = id

            if self.embedding_model is None:
                self.embedding_model = OpenAPIManager(env_manager=EnvManager())  # 使用 OpenAI 的 API
                embeddings = self.embedding_model.get_embeddings(input_data=feature_for_embedding)
                
            else:
                embeddings = self.embedding_model.get_embeddings(input_data=feature_for_embedding)

        
            if embeddings is not None:
                collection.add(
                    documents=[documents],
                    embeddings=[embeddings],
                    metadatas=[feature_metadata],
                    ids=[f"{id_p}"]
                )
            else:
                print("Embeddings are None !!!")

    def select_latest_collection_for_query(self):
        collections = self.client.list_collections()
        lastest_date = str(max([int(collection.name) for collection in collections]))
        self.collection = self.client.get_collection(name=lastest_date)

        return self.collection

    def embedding_query(self, 
                        query_embeddings : str, 
                        where_query : dict = None,
                        where_doc_query : dict = None, 
                        n_results : int=10):

        selected_collection = self.collection

        results = selected_collection.query(
                    query_embeddings = query_embeddings,
                    n_results = n_results,
                    where = where_query,
                    where_document = where_doc_query,
                    include=["documents"]
                    )
        
        return results
