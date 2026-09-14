from dataclasses import dataclass
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
