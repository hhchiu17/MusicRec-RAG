import requests
import json
from .env_manager import EnvManager


class OpenAPIManager:
    def __init__(self, env_manager: EnvManager = EnvManager()):
        self.env_manager = env_manager
        self.openai_api_key = "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"
        self.openai_embedding_site = "https://api.openai.com/v1/embeddings"
        self.embedding_model = "text-embedding-3-small"
        
        self.base_url = "https://api.openai.com/v1"
        self.primary_model = "gpt-4o"
        self.second_model = "gpt-4"
        self.third_model = "gpt-3.5-turbo"

    def get_embeddings(self, input_data, dimensions=1536, model=None):
        if model is None:
            model = self.embedding_model
        payload = {"input": input_data, "model": model, "dimensions": dimensions}
        headers = {"Authorization": f'Bearer {self.openai_api_key}', "Content-Type": "application/json"}
        response = requests.post(self.openai_embedding_site, headers = headers, data = json.dumps(payload))
        obj = json.loads(response.text)

        if response.status_code == 200:
            return obj["data"][0]["embedding"]
        else:
            return obj["error"]