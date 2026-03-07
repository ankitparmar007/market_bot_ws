import certifi
from config import Config
import platform
from server.db.client import MongoDB

# Create global MongoDB instance
mongodb_client = MongoDB(
    uri=Config.DB_URI, db_name=Config.DB_NAME, tlsCAFile=certifi.where()
)

DB_URI = ""
DB_NAME = "MarketBot"

if platform.system() == "Windows":
    DB_URI = "mongodb://marketbot:marketbot2026@194.195.119.34:27017/?authSource=MarketBot"

elif platform.system() == "Linux":
    DB_URI = "mongodb://marketbot:marketbot2026@localhost:27017/?authSource=MarketBot"

mongodb_ticks_client = MongoDB(uri=DB_URI, db_name=DB_NAME)
