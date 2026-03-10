from dataclasses import dataclass
from datetime import datetime
from enum import Enum


@dataclass(slots=True)
class OhlcModel:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int = 0
    # oi: int = 0


class Direction(Enum):
    neutral = "neutral"
    buy = "buy"
    sell = "sell"


@dataclass(slots=True, kw_only=True)
class VolumeDetailModel:
    symbol: str
    direction: Direction = Direction.neutral
    ltt: datetime
    vtt: int
    ltp: float
    minute_buy_vol: int = 0
    minute_sell_vol: int = 0
    minute_total_vol: int = 0


@dataclass(slots=True, kw_only=True)
class VolumeDeltaModel:
    timestamp: datetime
    symbol: str
    buy: int
    sell: int
    total: int
    delta: int
