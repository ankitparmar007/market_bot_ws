from typing import List

from server.db.collections import Collections

from server.api.exceptions import BadRequestException
from server.modules.stocks.models import StockModel


class StockRepository:

    @staticmethod
    async def all_stocks() -> List[StockModel]:
        res = await Collections.stocks.find({}, {"_id": 0})
        if res:
            return [StockModel(**stock) for stock in  res]
        else:
            raise BadRequestException("[all_stocks] No stocks found")
