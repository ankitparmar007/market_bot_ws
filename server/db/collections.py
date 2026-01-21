from typing import Any, List, Mapping
from pymongo import InsertOne, UpdateOne

from server.api.exceptions import AppException
from pymongo.errors import ServerSelectionTimeoutError, PyMongoError
from server.db import mongodb_client
from pymongo.results import (
    BulkWriteResult,
    UpdateResult,
    DeleteResult,
    InsertManyResult,
)


class _Collections:
    def __init__(self, collection_name: str) -> None:
        self.collection_name = collection_name

    def find(
        self,
        *args: Any,
        sort: list[tuple[str, int]] | None = None,
        **kwargs: Any,
    ) -> List[Mapping[str, Any]]:
        try:

            cursor = mongodb_client.db[self.collection_name].find(*args, **kwargs)
            if sort:
                cursor = cursor.sort(sort)
            return cursor.to_list(length=None)

        except (ServerSelectionTimeoutError, PyMongoError) as e:
            raise AppException(status_code=503, message=str(e))

    def find_one(self, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
        try:

            res = mongodb_client.db[self.collection_name].find_one(*args, **kwargs)
            if not res:
                raise AppException("Document not found")
            return res

        except (ServerSelectionTimeoutError, PyMongoError) as e:
            raise AppException(status_code=503, message=str(e))

    def find_one_or_none(self, *args: Any, **kwargs: Any) -> Mapping[str, Any] | None:
        try:

            res = mongodb_client.db[self.collection_name].find_one(*args, **kwargs)
            if not res:
                return None
            return res

        except (ServerSelectionTimeoutError, PyMongoError) as e:
            raise AppException(status_code=503, message=str(e))

    def update_one(self, *args: Any, **kwargs: Any) -> UpdateResult:
        try:

            return mongodb_client.db[self.collection_name].update_one(*args, **kwargs)

        except (ServerSelectionTimeoutError, PyMongoError) as e:
            raise AppException(status_code=503, message=str(e))

    def delete_many(self, *args: Any, **kwargs: Any) -> DeleteResult:
        try:

            return mongodb_client.db[self.collection_name].delete_many(*args, **kwargs)

        except (ServerSelectionTimeoutError, PyMongoError) as e:
            raise AppException(status_code=503, message=str(e))

    def bulk_update(self, operations: list[UpdateOne]) -> BulkWriteResult:
        try:

            return mongodb_client.db[self.collection_name].bulk_write(operations)

        except (ServerSelectionTimeoutError, PyMongoError) as e:
            raise AppException(status_code=503, message=str(e))

    def bulk_insert(self, operations: list[InsertOne]) -> BulkWriteResult:
        try:

            return mongodb_client.db[self.collection_name].bulk_write(
                operations, ordered=False
            )

        except (ServerSelectionTimeoutError, PyMongoError) as e:
            raise AppException(status_code=503, message=str(e))

    def insert_many(
        self, documents: list[dict], ordered: bool = False
    ) -> InsertManyResult:
        try:

            result = mongodb_client.db[self.collection_name].insert_many(
                documents, ordered=ordered
            )
            return result

        except (ServerSelectionTimeoutError, PyMongoError) as e:
            raise AppException(status_code=503, message=str(e))

    def aggregate(
        self,
        pipeline: list[dict],
        # allow_disk_use: bool = False,
        **kwargs: Any,
    ) -> List[Mapping[str, Any]]:
        try:

            cursor = mongodb_client.db[self.collection_name].aggregate(
                pipeline, **kwargs
            )
            return list(cursor)

        except (ServerSelectionTimeoutError, PyMongoError) as e:
            raise AppException(status_code=503, message=str(e))


class Collections:
    future_contracts = _Collections(collection_name="future_contracts")
    indices = _Collections(collection_name="indices")
    intraday_history = _Collections(collection_name="intraday_history")
    option_chain = _Collections(collection_name="option_chain")
    option_contracts = _Collections(collection_name="option_contracts")
    stocks = _Collections(collection_name="stocks")
    stocks_history = _Collections(collection_name="stocks_history")
    token = _Collections(collection_name="token")
    upstox = _Collections(collection_name="upstox")
    volume_history = _Collections(collection_name="volume_history")
    option_oi_signal = _Collections(collection_name="option_oi_signal")
