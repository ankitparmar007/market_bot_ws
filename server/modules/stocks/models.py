from pydantic import BaseModel


class TOHLCVOIModel(BaseModel):
    timestamp: str
    open: float | float = 0.0
    high: float | float = 0.0
    low: float | float = 0.0
    close: float | float = 0.0
    volume: int | int = 0
    oi: int | int = 0


class StockModel(BaseModel):
    symbol: str
    sector: list[int]
    instrument_key: str
    average_volume: float
    avg_negative_change: float
    avg_positive_change: float
    ohlc_1d_ago: TOHLCVOIModel
    ohlc: TOHLCVOIModel
