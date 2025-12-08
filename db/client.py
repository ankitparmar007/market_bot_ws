from time import sleep
from typing import Optional
import certifi
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import ServerSelectionTimeoutError, PyMongoError


from typing import Optional
from pymongo.errors import ServerSelectionTimeoutError, PyMongoError, AutoReconnect

from db.exceptions import AppException


class MongoDB:
    def __init__(
        self,
        uri: str,
        db_name: str,
        retries: int = 5,
        backoff: float = 1.0,
    ) -> None:
        self._uri = uri
        self._db_name = db_name
        self._client: Optional[MongoClient] = None
        self._db = None
        self._retries = retries
        self._backoff = backoff

        self._connect()  # connect on init

    @property
    def db(self) -> Database:
        if self._db is None:
            raise AppException("❌ Database not initialized")
        return self._db

    def _connect(self) -> None:
        """Connect to MongoDB with retries and ping"""
        attempt = 0
        while attempt < self._retries:
            try:
                self._client = MongoClient(
                    self._uri,
                    tlsCAFile=certifi.where(),
                    serverSelectionTimeoutMS=2000,  # fast fail
                )
                self._db = self._client[self._db_name]

                # Warm-up ping
                self._db.command("ping")
                print(f"✅ MongoDB connected to `{self._db_name}`")
                return

            except (ServerSelectionTimeoutError, AutoReconnect, PyMongoError) as e:
                attempt += 1
                wait_time = self._backoff * (2 ** (attempt - 1))
                print(
                    f"⚠️ Connection attempt {attempt}/{self._retries} failed: {e}. "
                    f"Retrying in {wait_time:.1f}s..."
                )
                self._client = None
                self._db = None
                sleep(wait_time)

        raise AppException("❌ Could not connect to MongoDB after retries")

    # def ensure_connection(self) -> dict:
    #     """Check if connection is alive, reconnect if needed"""
    #     if self._db is None:
    #         self._connect()
    #         return SuccessResponse(message="✅ MongoDB connected").to_dict()
    #     else:
    #         return SuccessResponse(message="✅ MongoDB connection is alive").to_dict()

    def close(self) -> None:
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            print("🔌 MongoDB connection closed")
            self._client = None
            self._db = None

