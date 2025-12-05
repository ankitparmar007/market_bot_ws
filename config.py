import os
from dotenv import load_dotenv

load_dotenv(override=True)


class Config:
    DB_NAME = os.getenv("DB_NAME", "")
    DB_URI = os.getenv("DB_URI", "")
    API_KEY_UPSTOX = os.getenv("API_KEY_UPSTOX")
    API_SECRET_UPSTOX = os.getenv("API_SECRET_UPSTOX")
    AUTH_TOKEN_UPSTOX = os.getenv("AUTH_TOKEN_UPSTOX")
    