import os
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
