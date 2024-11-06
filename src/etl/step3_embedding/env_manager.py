import os
from dotenv import load_dotenv

class EnvManager:
    """環境變數管理類別，負責載入和存取環境變數"""
    def __init__(self, env_path=os.path.join(".", "env",".env")):
        self.env_path = env_path
        self.load_env()

    def load_env(self):
        load_dotenv(self.env_path, override=True)


    def get(self, key):
        return os.getenv(key)
    
if __name__ == "__main__":
    a = EnvManager()
    print(a.get("HOST_IP"))