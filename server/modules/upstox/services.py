from datetime import datetime
from typing import List

from server.api import api_client
from server.api.exceptions import BadRequestException
from server.modules.options.models import OptionChain
from server.modules.stocks.models import TOHLCVOIModel
from server.modules.token.enums import Developer
from server.modules.token.repository import TokenRepository


class UpstoxServices:

    @staticmethod
    async def quotes(instrument_key) -> dict:
        url = "https://api.upstox.com/v2/market-quote/quotes"
        headers = {
            "Authorization": f"Bearer {await TokenRepository.get_token(Developer.ANKIT)}"
        }

        try:
            response = await api_client.get_json(
                url=url,
                headers=headers,
                params={"instrument_key": instrument_key},
            )

            return response["data"]
        except Exception as e:
            print(f"Error fetching quotes: {e}")
            raise BadRequestException(f"Error fetching quotes: {e}")

    @staticmethod
    async def option_chain(
        instrument_key: str, expiry_date: str, developer: Developer
    ) -> List[OptionChain]:
        url = "https://api.upstox.com/v2/option/chain"
        headers = {"Authorization": f"Bearer {await TokenRepository.get_token(developer)}"}

        try:
            response = await api_client.get_json(
                url,
                params={
                    "instrument_key": instrument_key,
                    "expiry_date": expiry_date,
                },
                headers=headers,
            )

            data = [OptionChain.model_validate(item) for item in response["data"]]
            sorted_list = sorted(data, key=lambda x: x.strike_price)
            return sorted_list

        except Exception as e:
            raise BadRequestException(
                f"[UpstoxServices.option_chain] {e} for {instrument_key}"
            )

    @staticmethod
    async def intraday_history_v3(
        instrument_key, minutes=1, validate=True, history_date: datetime | None = None
    ) -> list[TOHLCVOIModel]:

        # end_date = datetime(day=20, month=1, year=2026).strftime("%Y-%m-%d")
        # start_date = datetime(day=20, month=1, year=2026).strftime("%Y-%m-%d")
        # print(f"Fetching intraday data for {instrument_key} from {start_date} to {end_date}")
        # url = f"https://api.upstox.com/v3/historical-candle/{instrument_key}/minutes/{minutes}/{start_date}/{end_date}"

        url = f"https://api.upstox.com/v3/historical-candle/intraday/{instrument_key}/minutes/{minutes}"

        if history_date:
            date = history_date.strftime("%Y-%m-%d")
            url = f"https://api.upstox.com/v3/historical-candle/{instrument_key}/minutes/{minutes}/{date}/{date}"

            # print(f"history_date {instrument_key} from {history_date} to {url}")

        response = await api_client.get_json(url)

        ohlc_list = [
            TOHLCVOIModel.model_validate(
                dict(
                    zip(
                        (
                            "timestamp",
                            "open",
                            "high",
                            "low",
                            "close",
                            "volume",
                            "oi",
                        ),
                        row,
                    )
                )
            )
            for row in response["data"]["candles"]
        ]
        sorted_list = sorted(ohlc_list, key=lambda x: x.timestamp)

        if validate:
            if not sorted_list:
                raise BadRequestException(
                    f"[intraday_history_v3] No valid intraday data found for {instrument_key} minutes={minutes} validate={validate}"
                )

        return sorted_list
