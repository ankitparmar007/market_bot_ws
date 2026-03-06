import logging
from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")


class ISTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=IST)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()


def get_logger(name: str = "upstox_feed") -> logging.Logger:
    logger = logging.getLogger(name)

    # Prevent duplicate handlers on reload
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = ISTFormatter(
        fmt="%(asctime)s.%(msecs)03d — %(levelname)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.propagate = False

    return logger


log = get_logger()
