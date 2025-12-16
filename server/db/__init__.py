from config import Config

from server.db.client import MongoDB

# Create global MongoDB instance
mongodb_client = MongoDB(uri=Config.DB_URI, db_name=Config.DB_NAME)
