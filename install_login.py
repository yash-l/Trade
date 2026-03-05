import os

# Define the directory structure and file contents based EXACTLY on the provided code
project_structure = {
    "login": {
        "__init__.py": "",
        "models.py": """from dataclasses import dataclass
from typing import Optional

@dataclass
class CredentialOutput:
    app_key: str
    encryption_key: str
    user_id: str
    vendor_key: str
    response_url: str

@dataclass
class AccessTokenInput:
    request_token: str
    credential: CredentialOutput

@dataclass
class AccessTokenOutput:
    access_token: Optional[str]
    client_code: Optional[str]
    body_status: int
    head_status: int
    message: str
    allow_nse_cash: Optional[str]
    allow_nse_deriv: Optional[str]
    allow_mcx_comm: Optional[str]

@dataclass
class TokenValidationOutput:
    is_valid: bool
    reason: Optional[str] = None
""",
        "credential_manager.py": """import os
from models import CredentialOutput

class CredentialManager:
    def getCredentials(self) -> CredentialOutput:
        return CredentialOutput(
            app_key=os.getenv("APP_KEY"),
            encryption_key=os.getenv("ENCRYPTION_KEY"),
            user_id=os.getenv("USER_ID"),
            vendor_key=os.getenv("APP_KEY"),
            response_url=os.getenv("RESPONSE_URL"),
        )
""",
        "credential_validator.py": """from models import CredentialOutput

class CredentialValidator:
    def validate(self, credential: CredentialOutput):
        if not credential.app_key:
            raise Exception("APP_KEY missing")

        if not credential.encryption_key:  
            raise Exception("ENCRYPTION_KEY missing")  

        if not credential.user_id:  
            raise Exception("USER_ID missing")  

        if not credential.response_url:  
            raise Exception("RESPONSE_URL missing")
""",
        "oauth_builder.py": """from urllib.parse import urlencode
from models import CredentialOutput

class OAuthUrlBuilder:
    BASE_URL = "https://dev-openapi.5paisa.com/WebVendorLogin/VLogin/Index"

    def buildUrl(self, credential: CredentialOutput, state: str = "LoginSession001") -> str:  
        query = urlencode({  
            "VendorKey": credential.vendor_key,  
            "ResponseURL": credential.response_url,  
            "State": state  
        })  
        return f"{self.BASE_URL}?{query}"
""",
        "logger_service.py": """import logging

class LoggerService:
    def __init__(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(message)s"
        )
        self.logger = logging.getLogger("5paisa-login")

    def logLoginSuccess(self, client_code: str, segments: dict):  
        self.logger.info(  
            f"Login Success | ClientCode={client_code} | Segments={segments}"  
        )  

    def logRetry(self, attempt: int):  
        self.logger.warning(f"Retry attempt {attempt}")  

    def logError(self, message: str):  
        self.logger.error(message)  

    def logWarning(self, message: str):  
        self.logger.warning(message)
""",
        "access_token_service.py": """import requests
import time
from requests.exceptions import Timeout, ConnectionError, RequestException
from models import AccessTokenInput, AccessTokenOutput

class AccessTokenService:
    URL = "https://Openapi.5paisa.com/VendorsAPI/Service1.svc/GetAccessToken"

    def __init__(self, logger):  
        self.logger = logger  

    def generateAccessToken(self, input: AccessTokenInput) -> AccessTokenOutput:  
        payload = self._buildPayload(input)  

        for attempt in range(1, 4):  
            try:  
                response = requests.post(self.URL, json=payload, timeout=10)  
                response.raise_for_status()  
                return self._mapResponse(response.json())  

            except (Timeout, ConnectionError, RequestException) as e:  
                self.logger.logRetry(attempt)  
                time.sleep(0.5 * attempt)  

                if attempt == 3:  
                    raise Exception(f"Network failure after retries: {str(e)}")  

        raise Exception("Unexpected login failure")  

    def _buildPayload(self, input: AccessTokenInput) -> dict:  
        return {  
            "head": {"Key": input.credential.app_key},  
            "body": {  
                "RequestToken": input.request_token,  
                "EncryKey": input.credential.encryption_key,  
                "UserId": input.credential.user_id,  
            },  
        }  

    def _mapResponse(self, data: dict) -> AccessTokenOutput:  
        body = data.get("body", {})  
        head = data.get("head", {})  

        return AccessTokenOutput(  
            access_token=body.get("AccessToken"),  
            client_code=body.get("ClientCode"),  
            body_status=body.get("Status"),  
            head_status=head.get("Status"),  
            message=body.get("Message"),  
            allow_nse_cash=body.get("AllowNseCash"),  
            allow_nse_deriv=body.get("AllowNseDeriv"),  
            allow_mcx_comm=body.get("AllowMCXComm"),  
        )
""",
        "token_store.py": """import json
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
""",
        "token_validator.py": """from datetime import datetime
from models import TokenValidationOutput

class TokenValidator:
    def validateToken(self, stored_token: dict) -> TokenValidationOutput:
        if not stored_token:
            return TokenValidationOutput(False, "No token found")

        if not stored_token.get("access_token"):  
            return TokenValidationOutput(False, "Empty token")  

        if len(stored_token.get("access_token")) < 20:  
            return TokenValidationOutput(False, "Invalid token length")  

        today = datetime.now().strftime("%Y-%m-%d")  
        if stored_token.get("date") != today:  
            return TokenValidationOutput(False, "Token expired")  

        return TokenValidationOutput(True)
""",
        "header_builder.py": """class HeaderBuilder:
    def buildHeaders(self, access_token: str, client_code: str) -> dict:
        return {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "x-clientcode": client_code
        }
""",
        "login_orchestrator.py": """from models import AccessTokenInput
from credential_manager import CredentialManager
from credential_validator import CredentialValidator
from access_token_service import AccessTokenService
from token_store import TokenStore
from token_validator import TokenValidator
from logger_service import LoggerService

class LoginOrchestrator:
    def __init__(self, token_store_path: str = None):
        self.logger = LoggerService()
        self.credential_manager = CredentialManager()
        self.credential_validator = CredentialValidator()
        self.token_service = AccessTokenService(self.logger)
        self.token_store = TokenStore(token_store_path)
        self.validator = TokenValidator()

    def login(self, request_token: str):  
        try:  
            credential = self.credential_manager.getCredentials()  
            self.credential_validator.validate(credential)  

            token_input = AccessTokenInput(request_token, credential)  
            output = self.token_service.generateAccessToken(token_input)  

            self._validateResponse(output)  
            self._validateSegments(output)  

            self.token_store.saveToken(output.access_token, output.client_code)  

            segments = {  
                "NSE_CASH": output.allow_nse_cash,  
                "NSE_DERIV": output.allow_nse_deriv,  
                "MCX_COMM": output.allow_mcx_comm  
            }  

            self.logger.logLoginSuccess(output.client_code, segments)  
            return output  

        except Exception as e:  
            self.logger.logError(str(e))  
            raise  

    def getValidToken(self):  
        stored = self.token_store.loadToken()  
        validation = self.validator.validateToken(stored)  
        if not validation.is_valid:  
            raise Exception(validation.reason)  
        return stored  

    def _validateResponse(self, output):  
        if output.head_status != 0:  
            raise Exception("Head status failed")  

        if output.body_status == 2:  
            raise Exception("Token Expired")  

        if output.body_status != 0:  
            raise Exception(f"Login failed: {output.message}")  

        if output.message != "Success":  
            raise Exception(f"Unexpected response message: {output.message}")  

        if not output.access_token:  
            raise Exception("Empty access token")  

    def _validateSegments(self, output):  
        if output.allow_nse_cash != "Y":  
            raise Exception("NSE Cash not enabled")  

        if output.allow_nse_deriv != "Y":  
            self.logger.logWarning("NSE Derivatives not enabled")  

        if output.allow_mcx_comm != "Y":  
            self.logger.logWarning("MCX not enabled")
"""
    }
}

def create_project(base_path="."):
    for folder, files in project_structure.items():
        folder_path = os.path.join(base_path, folder)
        os.makedirs(folder_path, exist_ok=True)
        print(f"Created directory: {folder_path}/")
        
        for file_name, content in files.items():
            file_path = os.path.join(folder_path, file_name)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"  Created file: {file_path}")

if __name__ == "__main__":
    print("Starting installation...")
    create_project()
    print("Installation complete. Your login module is ready.")
