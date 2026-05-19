import certifi
from config import Config
from server.db.clickhouse_db import ClickHouseDB
from server.db.mongo_db import MongoDB

# Create global MongoDB instance
mongodb_client = MongoDB(
    uri=Config.DB_URI, db_name=Config.DB_NAME, tlsCAFile=certifi.where()
)

# DB_URI = ""
# DB_NAME = "MarketBot"

# if platform.system() == "Windows":
#     DB_URI = (
#         "mongodb://marketbot:marketbot2026@194.195.119.34:27017/?authSource=MarketBot"
#     )

# elif platform.system() == "Linux":
#     DB_URI = "mongodb://marketbot:marketbot2026@localhost:27017/?authSource=MarketBot"

# mongodb_ticks_client = MongoDB(uri=DB_URI, db_name=DB_NAME)

clickhouse_client = ClickHouseDB(
    host=Config.CLICK_HOUSE_HOST,
    port=8123,
    username=Config.CLICK_HOUSE_USERNAME,
    password=Config.CLICK_HOUSE_PASSWORD,
    database=Config.CLICK_HOUSE_DB,
)
