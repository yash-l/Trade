class HeaderBuilder:
    def buildHeaders(self, access_token: str, client_code: str) -> dict:
        return {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "x-clientcode": client_code
        }
