from datetime import timedelta, timezone, datetime


class IndianDateTime:

    __IST_OFFSET = timedelta(hours=5, minutes=30, seconds=0, microseconds=0)
    __IST_TIMEZONE = timezone(__IST_OFFSET)

    @staticmethod
    def now() -> datetime:
        return datetime.now(timezone.utc).astimezone(IndianDateTime.__IST_TIMEZONE)

    @staticmethod
    def now_isoformat() -> str:
        return IndianDateTime.now().isoformat()

    @staticmethod
    def now_preety_isoformat() -> str:
        return IndianDateTime.now().strftime("%d-%m-%Y %H:%M:%S")

    @staticmethod
    # 2026-02-19 09:15:00+05:30 = timezone-aware datetime
    def fromtimestamp(timestamp: str):
        ts = int(timestamp)

        utc_dt = datetime.fromtimestamp(ts // 1000, timezone.utc)
        utc_dt = utc_dt.replace(microsecond=(ts % 1000) * 1000)

        return utc_dt.astimezone(IndianDateTime.__IST_TIMEZONE)

    # @staticmethod
    # def fromtimestampnaive(timestamp:str):
    #     return datetime.utcfromtimestamp(float(timestamp) / 1000) + IndianDateTime.__IST_OFFSET

    @staticmethod
    # 2026-02-19 09:15:00 = timezone-naive datetime
    def fromtimestampnaive(timestamp: str):
        ts = int(timestamp)

        utc_dt = datetime.fromtimestamp(ts // 1000, timezone.utc)
        utc_dt = utc_dt.replace(microsecond=(ts % 1000) * 1000)

        ist_dt = utc_dt.astimezone(IndianDateTime.__IST_TIMEZONE)

        return ist_dt.replace(tzinfo=None)
