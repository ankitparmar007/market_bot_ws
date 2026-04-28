from pydantic import BaseModel

from server.modules.stocks.models import TOHLCVOIModel


class IndicesModel(BaseModel):
    symbol: str
    instrument_key: str
    breakout_1d_high: str | None
    breakout_1d_low: str | None
    breakout_high_change: float | None
    breakout_low_change: float | None
    cmp: float | None
    gap: float | None
    percentage_change: float | None
    market: str | None
    ohlc_1d_ago: TOHLCVOIModel
    ohlc: TOHLCVOIModel
    has_option: bool
