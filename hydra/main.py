import uvicorn
from .config import Settings

def main():
    c=Settings(); uvicorn.run("hydra.api:app",host=c.host,port=c.port,reload=False,log_level="info")
if __name__=="__main__":main()
