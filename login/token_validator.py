from datetime import datetime
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
