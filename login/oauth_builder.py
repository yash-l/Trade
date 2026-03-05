from urllib.parse import urlencode
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
