import logging

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
