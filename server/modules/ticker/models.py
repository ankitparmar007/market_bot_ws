from datetime import datetime
from enum import Enum
from pydantic import BaseModel


class OhlcModel:
    __slots__ = ("ts", "open", "high", "low", "close", "volume", "oi")

    def __init__(
        self,
        ts: datetime,
        open: float,
        high: float,
        low: float,
        close: float,
        volume: int = 0,
        oi: int = 0,
    ):
        self.ts = ts
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.oi = oi


class Direction(Enum):
    neutral = "neutral"
    buy = "buy"
    sell = "sell"


class VolumeDeltaModel(BaseModel):
    prev_direction: Direction = Direction.neutral
    prev_ltt: datetime | None = None
    prev_vtt: int | None = None
    prev_ltp: float | None = None
    prev_trade_key: tuple | None = None
    minute_buy: float = 0
    minute_sell: int = 0
    minute_volume: int = 0
    symbol: str
