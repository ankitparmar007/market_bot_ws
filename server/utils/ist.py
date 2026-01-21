from datetime import timedelta, timezone, datetime


class IndianDateTime:

    __IST = timezone(timedelta(hours=5, minutes=30, seconds=0, microseconds=0))

    @staticmethod
    def now() -> datetime:
        return datetime.now(IndianDateTime.__IST)

    @staticmethod
    def now_isoformat() -> str:
        return IndianDateTime.now().isoformat()
    
    @staticmethod
    def now_preety_isoformat() -> str:
        return IndianDateTime.now().strftime("%d-%m-%Y %H:%M:%S")
    
    
    @staticmethod
    def fromtimestamp(timestamp:str):
        return datetime.fromtimestamp(float(timestamp) / 1000, IndianDateTime.__IST)
