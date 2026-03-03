import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    ENVIRONMENT = os.environ.get("FLASK_ENV", "production")
    
    REQUIRED_VARS = [
        "API_KEY"
        5PAISA_VENDOR_KEY=kiFauE4G3aPebavZqzaCDXL5ZqLWpcYC,
        "ENCRYPTION_KEY"
        5PAISA_ENCRYPTION_KEY=Z7w27HD7OGCobFo48ACHhoQEjqtXP6ra,
        "USER_ID"
        5PAISA_USER_ID=Xl3r3TUSHdF,
        "CALLBACK_URL",
        "REDIS_URL",
        "TOKEN_ENCRYPTION_KEY",
        "DATABASE_URL"
    ]
    
    API_KEY = os.environ.get("5PAISA_VENDOR_KEY")
    ENCRYPTION_KEY = os.environ.get("5PAISA_ENCRYPTION_KEY")
    USER_ID = os.environ.get("5PAISA_USER_ID") 
    CALLBACK_URL = os.environ.get("5PAISA_RESPONSE_URL")
    
    REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    DATABASE_URL = os.environ.get("DATABASE_URL")
    
    PAISA_OAUTH_URL = os.environ.get("5PAISA_OAUTH_URL", "https://dev-openapi.5paisa.com")
    PAISA_API_URL = os.environ.get("5PAISA_API_URL", "https://Openapi.5paisa.com")
    
    TOKEN_ENCRYPTION_KEY = os.environ.get("HYDRA_TOKEN_ENCRYPTION_KEY")
    PREVIOUS_ENCRYPTION_KEY = os.environ.get("HYDRA_PREVIOUS_KEY")

    @classmethod
    def validate(cls):
        missing = [var for var in cls.REQUIRED_VARS if getattr(cls, var) is None]
        if missing:
            raise ValueError(f"CRITICAL BOOT FAILURE: Missing environment variables: {missing}")

Config.validate()
