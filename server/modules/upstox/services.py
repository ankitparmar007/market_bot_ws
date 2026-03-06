from typing import List

from server.api import api_client
from server.api.exceptions import BadRequestException
from server.modules.options.models import OptionChain
from server.modules.token.enums import Developer
from server.modules.token.repository import TokenRepository


class UpstoxServices:

    @staticmethod
    async def option_chain(
        instrument_key: str, expiry_date: str, developer: Developer
    ) -> List[OptionChain]:
        url = "https://api.upstox.com/v2/option/chain"
        headers = {
            "Authorization": f"Bearer {await TokenRepository.get_token(developer)}"
        }

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
