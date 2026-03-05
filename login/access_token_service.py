import requests
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
