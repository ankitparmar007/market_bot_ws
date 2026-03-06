from datetime import timedelta, timezone, datetime


class ISDateTime:

    __IST_OFFSET = timedelta(hours=5, minutes=30, seconds=0, microseconds=0)
    __IST_TIMEZONE = timezone(__IST_OFFSET)

    @staticmethod
    def now() -> datetime:
        return datetime.now(timezone.utc).astimezone(ISDateTime.__IST_TIMEZONE)

    @staticmethod
    def market_start_utc() -> datetime:
        return datetime.now(timezone.utc).replace(
            hour=3, minute=45, second=0, microsecond=0
        )

    @staticmethod
    def market_end_utc() -> datetime:
        return datetime.now(timezone.utc).replace(
            hour=10, minute=0, second=0, microsecond=0
        )

    @staticmethod
    def now_isoformat() -> str:
        return ISDateTime.now().isoformat()

    @staticmethod
    def now_preety_isoformat() -> str:
        return ISDateTime.now().strftime("%d-%m-%Y %H:%M:%S")

    @staticmethod
    # 2026-02-19 09:15:00+05:30 = timezone-aware datetime
    def fromtimestamp(timestamp: str) -> datetime:
        ts = int(timestamp)

        utc_dt = datetime.fromtimestamp(ts // 1000, timezone.utc)
        utc_dt = utc_dt.replace(microsecond=(ts % 1000) * 1000)

        return utc_dt.astimezone(ISDateTime.__IST_TIMEZONE)

    @staticmethod
    def utc_to_ist_naive(utc_dt: datetime) -> datetime:
        ist_dt = utc_dt.astimezone(ISDateTime.__IST_TIMEZONE)
        return ist_dt.replace(tzinfo=None)
    
    @staticmethod
    def utc_to_ist(utc_dt: datetime) -> datetime:
        ist_dt = utc_dt.astimezone(ISDateTime.__IST_TIMEZONE)
        return ist_dt

    @staticmethod
    # 2026-02-19 09:15:00 = timezone-naive datetime
    def fromtimestampnaive(timestamp: str) -> datetime:
        ts = int(timestamp)

        utc_dt = datetime.fromtimestamp(ts // 1000, timezone.utc)
        utc_dt = utc_dt.replace(microsecond=(ts % 1000) * 1000)
        ist_dt = utc_dt.astimezone(ISDateTime.__IST_TIMEZONE)

        return ist_dt.replace(tzinfo=None)
