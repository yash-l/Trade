from models import AccessTokenInput
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
