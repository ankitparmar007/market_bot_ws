from typing import List

from server.api import api_client
from server.api.exceptions import BadRequestException, NotFoundException
from server.modules.options.models import ContractModel, OptionChain
from server.modules.token.enums import Developer
from server.modules.token.repository import TokenRepository


class UpstoxServices:

    option_chain_url = "https://api.upstox.com/v2/option/chain"

    @classmethod
    async def option_chain(
        cls, instrument_key: str, expiry_date: str, developer: Developer
    ) -> List[OptionChain]:
        headers = {
            "Authorization": f"Bearer {await TokenRepository.get_token(developer)}"
        }

        try:
            response = await api_client.get_json(
                cls.option_chain_url,
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
    async def option_contract(instrument_key: str) -> List[ContractModel]:
        url = "https://api.upstox.com/v2/option/contract"
        headers = {
            "Authorization": f"Bearer {await TokenRepository.get_token(Developer.ANKIT)}"
        }

        try:
            response = await api_client.get_json(
                url,
                params={"instrument_key": instrument_key},
                headers=headers,
            )

            data: List[ContractModel] = [
                ContractModel.model_validate(item) for item in response["data"]
            ]
            if data:
                sorted_list = sorted(data, key=lambda x: x.expiry)
                distinct_expiry_objects = list(
                    {obj.expiry: obj for obj in sorted_list}.values()
                )
                return distinct_expiry_objects
            else:
                msg = f"[option_contract] No valid option contracts found for {instrument_key}"
                raise NotFoundException(msg)

        except Exception as e:
            msg = f"[option_contract] Error fetching option contracts: {e} for {instrument_key}"
            raise NotFoundException(msg)
