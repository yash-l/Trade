from models import CredentialOutput

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
