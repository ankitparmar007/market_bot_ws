from typing import Any, List, Mapping
from pymongo import InsertOne, UpdateOne
from pymongo.errors import ServerSelectionTimeoutError, PyMongoError
from pymongo.results import (
    BulkWriteResult,
    UpdateResult,
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
)

from server.api.exceptions import DatabaseException
from server.db import mongodb_client
from pymongo.asynchronous.collection import AsyncCollection


class _Collections:
    def __init__(self, collection_name: str) -> None:
        self.collection_name = collection_name

    @property
    def collection(self) -> AsyncCollection:
        return mongodb_client.db[self.collection_name]

    async def find(
        self,
        *args: Any,
        sort: list[tuple[str, int]] | None = None,
        skip: int | None = None,
        limit: int | None = None,
        **kwargs: Any,
    ) -> List[Mapping[str, Any]]:
        try:
            cursor = self.collection.find(*args, **kwargs)
            if sort:
                cursor = cursor.sort(sort)
            if skip:
                cursor = cursor.skip(skip)
            if limit:
                cursor = cursor.limit(limit)
            return await cursor.to_list(length=None)

        except (ServerSelectionTimeoutError, PyMongoError) as e:
            print("[Collections.find] = ", str(e))
            raise DatabaseException()

    async def find_one(self, *args: Any, **kwargs: Any) -> Mapping[str, Any] | None:
        try:
            return await self.collection.find_one(*args, **kwargs)
        except (ServerSelectionTimeoutError, PyMongoError) as e:
            print("[Collections.find_one] = ", str(e))
            raise DatabaseException()

    async def update_one(self, *args: Any, **kwargs: Any) -> UpdateResult:
        try:
            return await self.collection.update_one(*args, **kwargs)
        except (ServerSelectionTimeoutError, PyMongoError) as e:
            print("[Collections.update_one] = ", str(e))
            raise DatabaseException()

    async def delete_many(self, *args: Any, **kwargs: Any) -> DeleteResult:
        try:
            return await self.collection.delete_many(*args, **kwargs)
        except (ServerSelectionTimeoutError, PyMongoError) as e:
            print("[Collections.delete_many] = ", str(e))
            raise DatabaseException()

    async def bulk_update(self, operations: list[UpdateOne]) -> BulkWriteResult:
        try:
            return await self.collection.bulk_write(operations)
        except (ServerSelectionTimeoutError, PyMongoError) as e:
            print("[Collections.bulk_update] = ", str(e))
            raise DatabaseException()

    async def bulk_insert(self, operations: list[InsertOne]) -> BulkWriteResult:
        try:
            return await self.collection.bulk_write(operations, ordered=False)
        except (ServerSelectionTimeoutError, PyMongoError) as e:
            print("[Collections.bulk_insert] = ", str(e))
            raise DatabaseException()

    async def insert_many(self, documents: list[dict]) -> InsertManyResult:
        try:
            return await self.collection.insert_many(documents,ordered=False)
        except (ServerSelectionTimeoutError, PyMongoError) as e:
            print("[Collections.insert_many] = ", str(e))
            raise DatabaseException()

    async def insert_one(self, document: dict) -> InsertOneResult:
        try:
            return await self.collection.insert_one(document)
        except (ServerSelectionTimeoutError, PyMongoError) as e:
            print("[Collections.insert_one] = ", str(e))
            raise DatabaseException()

    async def delete_one(self, *args: Any, **kwargs: Any) -> DeleteResult:
        try:
            return await self.collection.delete_one(*args, **kwargs)
        except (ServerSelectionTimeoutError, PyMongoError) as e:
            print("[Collections.delete_one] = ", str(e))
            raise DatabaseException()

    async def count_documents(self, *args: Any, **kwargs: Any) -> int:
        try:
            return await self.collection.count_documents(*args, **kwargs)
        except (ServerSelectionTimeoutError, PyMongoError) as e:
            print("[Collections.count_documents] = ", str(e))
            raise DatabaseException()

    async def distinct(self, *args: Any, **kwargs: Any) -> list[Any]:
        try:
            return await self.collection.distinct(*args, **kwargs)
        except (ServerSelectionTimeoutError, PyMongoError) as e:
            print("[Collections.distinct] = ", str(e))
            raise DatabaseException()

    async def aggregate(
        self,
        pipeline: list[dict],
        **kwargs: Any,
    ) -> List[Mapping[str, Any]]:
        try:
            cursor = self.collection.aggregate(pipeline, **kwargs)
            return [doc async for doc in await cursor]
        except (ServerSelectionTimeoutError, PyMongoError) as e:
            print("[Collections.aggregate] = ", str(e))
            raise DatabaseException()


class Collections:
    future_contracts = _Collections(collection_name="future_contracts")
    indices = _Collections(collection_name="indices")
    intraday_all_history = _Collections(collection_name="intraday_all_history")
    option_chain = _Collections(collection_name="option_chain")
    option_contracts = _Collections(collection_name="option_contracts")
    stocks = _Collections(collection_name="stocks")
    stocks_history = _Collections(collection_name="stocks_history")
    token = _Collections(collection_name="token")
    upstox = _Collections(collection_name="upstox")
    volume_history = _Collections(collection_name="volume_history")
    option_oi_signal = _Collections(collection_name="option_oi_signal")

