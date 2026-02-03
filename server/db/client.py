import asyncio
import certifi
from typing import Optional

from pymongo import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import ServerSelectionTimeoutError, PyMongoError, AutoReconnect

from server.api.exceptions import DatabaseException
from server.api.models import SuccessResponse


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
        self._retries = retries
        self._backoff = backoff

        self._client: Optional[AsyncMongoClient] = None
        self._db: Optional[AsyncDatabase] = None

    @property
    def db(self) -> AsyncDatabase:
        if self._db is None:
            print("[MongoDB.db] ❌ Database connection is not established")
            raise DatabaseException()
        return self._db

    async def _connect(self) -> None:
        attempt = 0
        while attempt < self._retries:
            try:
                self._client = AsyncMongoClient(
                    self._uri,
                    tlsCAFile=certifi.where(),
                    serverSelectionTimeoutMS=2000,
                )
                self._db = self._client[self._db_name]

                # async ping
                await self._db.command("ping")

                print(f"✅ MongoDB connected → `{self._db_name}`")
                return

            except (ServerSelectionTimeoutError, AutoReconnect, PyMongoError) as e:
                attempt += 1
                wait_time = self._backoff * (2 ** (attempt - 1))
                print(
                    f"⚠️ Mongo attempt {attempt}/{self._retries} failed: {e}. "
                    f"Retrying in {wait_time:.1f}s..."
                )
                self._client = None
                self._db = None
                await asyncio.sleep(wait_time)

        print(
            "[MongoDB._connect] ❌ Could not connect to database after multiple attempts"
        )
        raise DatabaseException()

    async def ensure_connection(self) -> dict:
        if self._db is None:
            await self._connect()
            return SuccessResponse(message="✅ MongoDB connected").model_dump()
        return SuccessResponse(message="✅ MongoDB connection is alive").model_dump()

    async def close(self) -> None:
        if self._client:
            await self._client.close()
            print("[MongoDB.close] 🔌 MongoDB connection closed")
            self._client = None
            self._db = None
