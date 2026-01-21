from pydantic import BaseModel, Field
from datetime import datetime

from server.utils.ist import IndianDateTime


class MarketData(BaseModel):
    ltp: float | float = 0.0
    volume: int | int = 0
    oi: float | float = 0.0
    close_price: float | float = 0.0
    bid_price: float | float = 0.0
    bid_qty: int | int = 0
    ask_price: float | float = 0.0
    ask_qty: int | int = 0
    prev_oi: float | float = 0.0


class OptionOiSignal(BaseModel):
    timestamp: str = Field(default_factory=IndianDateTime.now_preety_isoformat)
    call_pct: float | float = 0.0
    put_pct: float | float = 0.0
    symbol: str
    signal: str


class OptionData(BaseModel):
    instrument_key: str
    market_data: MarketData


class OptionChain(BaseModel):
    created_at: str = Field(default_factory=datetime.now().isoformat)
    expiry: str
    pcr: float | float = 0.0
    strike_price: float
    underlying_key: str
    underlying_spot_price: float
    call_options: OptionData
    put_options: OptionData


class ContractModel(BaseModel):
    name: str
    expiry: str
    exchange: str
    underlying_symbol: str
    underlying_key: str
    instrument_key: str
    trading_symbol: str
    instrument_type: str
    lot_size: int
    tick_size: int
