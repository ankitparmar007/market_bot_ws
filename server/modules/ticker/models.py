from dataclasses import dataclass
from datetime import datetime
from enum import Enum


@dataclass(slots=True)
class OhlcModel:
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int = 0
    oi: int = 0


class Direction(Enum):
    neutral = "neutral"
    buy = "buy"
    sell = "sell"


@dataclass(slots=True, kw_only=True)
class VolumeDetailModel:
    symbol: str
    prev_direction: Direction = Direction.neutral
    prev_ltt: datetime | None = None
    prev_vtt: int | None = None
    prev_ltp: float | None = None
    minute_buy: int = 0
    minute_sell: int = 0
    minute_volume: int = 0


@dataclass(slots=True, kw_only=True)
class VolumeDeltaModel:
    timestamp: datetime
    symbol: str
    buy: int
    sell: int
    total: int
    delta: int
