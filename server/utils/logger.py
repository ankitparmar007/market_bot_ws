import logging
from datetime import datetime
from zoneinfo import ZoneInfo  # Python 3.9+

IST = ZoneInfo("Asia/Kolkata")


class ISTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=IST)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()


formatter = ISTFormatter(
    fmt="%(asctime)s — %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

handler = logging.StreamHandler()
handler.setFormatter(formatter)

log = logging.getLogger("upstox_feed")
log.setLevel(logging.INFO)
log.addHandler(handler)
log.propagate = False
