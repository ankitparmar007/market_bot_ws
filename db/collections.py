from typing import Any, List, Mapping

from pymongo.errors import ServerSelectionTimeoutError, PyMongoError
from pymongo.results import UpdateResult
from db import mongodb_client
from db.exceptions import AppException


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

    def insert_one(self, *args: Any, **kwargs: Any) -> Any:
        try:

            return mongodb_client.db[self.collection_name].insert_one(*args, **kwargs)

        except (ServerSelectionTimeoutError, PyMongoError) as e:
            raise AppException(status_code=503, message=str(e))


class Collections:
    volume_history = _Collections(collection_name="volume_history")
    stocks = _Collections(collection_name="stocks")
    token = _Collections(collection_name="token")
    upstox = _Collections(collection_name="upstox")
