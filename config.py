import os
from dotenv import load_dotenv

load_dotenv(override=True)


class Config:
    DB_NAME = os.getenv("DB_NAME", "")
    DB_URI = os.getenv("DB_URI", "")
    API_HASH = os.getenv("API_HASH", default="")
    API_ID = int(os.getenv("API_ID", default=0))
    BOT_TOKEN = os.getenv("BOT_TOKEN", default="")
    USER_ID = int(os.getenv("USER_ID", default=0))
    EXPIRY_DATE = os.getenv("EXPIRY_DATE", default="")
    
