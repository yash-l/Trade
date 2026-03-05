import json
import os
import threading
from datetime import datetime

class TokenStore:
    _lock = threading.Lock()

    def __init__(self, file_path: str = None):  
        self.FILE_PATH = file_path or os.path.join(os.getcwd(), "token_store.json")  

    def saveToken(self, access_token: str, client_code: str):  
        with self._lock:  
            data = {  
                "access_token": access_token,  
                "client_code": client_code,  
                "date": datetime.now().strftime("%Y-%m-%d")  
            }  
            with open(self.FILE_PATH, "w") as f:  
                json.dump(data, f)  

    def loadToken(self):  
        with self._lock:  
            if not os.path.exists(self.FILE_PATH):  
                return None  

            with open(self.FILE_PATH, "r") as f:  
                return json.load(f)
