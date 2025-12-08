import os
from dotenv import load_dotenv

load_dotenv(override=True)


class Config:
    DB_NAME = os.getenv("DB_NAME", "")
    DB_URI = os.getenv("DB_URI", "")
