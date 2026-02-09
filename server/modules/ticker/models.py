from datetime import datetime
from pydantic import BaseModel, field_validator

from server.utils.ist import IndianDateTime


class OhlcModel(BaseModel):
    open: float
    high: float
    low: float
    close: float
    ts: datetime
    oi: int = 0
    vol: int = 0

    @field_validator("ts", mode="before")
    @classmethod
    def parse_ts(cls, value):
        """
        ts can be:
        - milliseconds since epoch (str)
        """
        if isinstance(value, str):
            return IndianDateTime.fromtimestamp(value).replace(second=0, microsecond=0)

        raise ValueError("Invalid timestamp format")
