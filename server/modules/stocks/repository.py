from typing import List

from server.db.collections import Collections

from server.api.exceptions import AppException
from server.modules.stocks.models import StockModel


class StockRepository:

    @staticmethod
    def all_stocks() -> List[StockModel]:
        res = Collections.stocks.find({}, {"_id": 0})
        if res:
            return [StockModel(**stock) for stock in res]
        else:
            raise AppException("[all_stocks] No stocks found")
