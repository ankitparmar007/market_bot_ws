import os
from dotenv import load_dotenv

load_dotenv(override=True)


class Config:
    DB_NAME = os.getenv("DB_NAME", "")
    DB_URI = os.getenv("DB_URI", "")
    CLICK_HOUSE_HOST = os.getenv("CLICK_HOUSE_HOST", "")
    CLICK_HOUSE_USERNAME = os.getenv("CLICK_HOUSE_USERNAME", "")
    CLICK_HOUSE_PASSWORD = os.getenv("CLICK_HOUSE_PASSWORD", "")
    CLICK_HOUSE_DB = os.getenv("CLICK_HOUSE_DB", "")
    API_HASH = os.getenv("API_HASH", default="")
    API_ID = int(os.getenv("API_ID", default=0))
    BOT_TOKEN = os.getenv("BOT_TOKEN", default="")
    USER_ID = int(os.getenv("USER_ID", default=0))
    VERSION = os.getenv("VERSION", default="")

