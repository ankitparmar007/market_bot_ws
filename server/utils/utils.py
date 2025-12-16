from datetime import timedelta, timezone, datetime


IST = timezone(timedelta(hours=5, minutes=30, seconds=0, microseconds=0))


class IndianDateTime:
    @staticmethod
    def now():
        return datetime.now(IST)

    @staticmethod
    def fromtimestamp(timestamp:str):
        return datetime.fromtimestamp(float(timestamp) / 1000, IST)