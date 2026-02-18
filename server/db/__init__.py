import certifi
from config import Config

from server.db.client import MongoDB

# Create global MongoDB instance
mongodb_client = MongoDB(
    uri=Config.DB_URI, db_name=Config.DB_NAME, tlsCAFile=certifi.where()
)

DB_URI = "mongodb://marketbot:marketbot2026@194.195.119.34:27017/?authSource=MarketBot"
DB_NAME = "MarketBot"

mongodb_ticks_client = MongoDB(uri=DB_URI, db_name=DB_NAME)
