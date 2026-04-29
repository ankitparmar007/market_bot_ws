import asyncio
from pymongo import UpdateOne
from server.api.models import ErrorResponse, SuccessResponse
from server.db.collections import Collections
from server.modules.expiry.repository import ExpiryRepository
from server.modules.indices.repository import IndicesRepository
from server.modules.options.models import OptionChain
from server.modules.stocks.repository import StockRepository
from server.modules.token.enums import Developer
from server.modules.upstox.services import UpstoxServices
from server.utils.is_dt import ISDateTime


class OptionServices:

    # Use local lambdas to avoid repeated deep attribute lookups inside sum
    @staticmethod
    def c_prev(s: OptionChain):
        return s.call_options.market_data.prev_oi

    @staticmethod
    def p_prev(s: OptionChain):
        return s.put_options.market_data.prev_oi

    @staticmethod
    def c_now(s: OptionChain):
        return s.call_options.market_data.oi

    @staticmethod
    def p_now(s: OptionChain):
        return s.put_options.market_data.oi

    @staticmethod
    async def calculate_oi_change(
        symbol: str,
        instrument_key: str,
        expiry_date: str,
        developer: Developer,
        open_price: float,
        percentage_change: float,
    ) -> tuple[UpdateOne, UpdateOne]:

        strikes = await UpstoxServices.option_chain(
            instrument_key=instrument_key,
            expiry_date=expiry_date,
            developer=developer,
        )

        p_change = percentage_change / 100
        open = open_price
        upper = open + (open * p_change)
        lower = open - (open * p_change)

        # Generator to avoid creating a list
        filtered = (s for s in strikes if lower < s.strike_price < upper)

        # Materialize filtered once so we don't iterate it 4 times
        filtered_list = list(filtered)

        prev_call_oi = sum(OptionServices.c_prev(s) for s in filtered_list)
        prev_put_oi = sum(OptionServices.p_prev(s) for s in filtered_list)
        # prev_net_oi = prev_put_oi - prev_call_oi
        call_oi = sum(OptionServices.c_now(s) for s in filtered_list)
        put_oi = sum(OptionServices.p_now(s) for s in filtered_list)
        # net_oi = put_oi - call_oi

        call_pct = (
            ((call_oi - prev_call_oi) / prev_call_oi * 100) if prev_call_oi else 0
        )
        put_pct = ((put_oi - prev_put_oi) / prev_put_oi * 100) if prev_put_oi else 0

        # net_pct = ((net_oi - prev_net_oi) / prev_net_oi * 100) if prev_net_oi else 0
        net_pct = put_pct - call_pct

        # pcr = put_oi / call_oi if call_oi else 0

        # all stricks pcr
        all_prev_call_oi = sum(OptionServices.c_prev(s) for s in strikes)
        all_prev_put_oi = sum(OptionServices.p_prev(s) for s in strikes)
        # prev_net_oi = prev_put_oi - prev_call_oi
        all_call_oi = sum(OptionServices.c_now(s) for s in strikes)
        all_put_oi = sum(OptionServices.p_now(s) for s in strikes)

        all_strikes_prev_pcr = (
            all_prev_put_oi / all_prev_call_oi if all_prev_call_oi else 0
        )
        all_strikes_pcr = all_put_oi / all_call_oi if all_call_oi else 0

        return (
            UpdateOne(
                upsert=False,
                filter={"symbol": symbol},
                update={
                    "$set": {
                        "symbol": symbol,
                        "instrument_key": instrument_key,
                        "expiry_date": expiry_date,
                        "strikes": [s.model_dump() for s in strikes],
                        "updated_at": ISDateTime.now_isoformat(),
                    }
                },
            ),
            UpdateOne(
                upsert=False,
                filter={"symbol": symbol},
                update={
                    "$set": {
                        "total_call_oi_change_percent": round(call_pct, 2),
                        "total_put_oi_change_percent": round(put_pct, 2),
                        "net_oi_change_percent": round(net_pct, 2),
                        "all_strikes_prev_pcr": round(all_strikes_prev_pcr, 2),
                        "all_strikes_pcr": round(all_strikes_pcr, 2),
                        "updated_at": ISDateTime.now_isoformat(),
                    }
                },
            ),
        )

    @staticmethod
    async def update_option_chain_and_oi():
        stocks = await StockRepository.all_stocks()
        stock_oi_updates: list[UpdateOne] = []
        indices_oi_updates: list[UpdateOne] = []
        option_chain_updates: list[UpdateOne] = []
        expiry_date = await ExpiryRepository.get_expiry()
        developer = Developer.RACHIT

        for stock in stocks:
            await asyncio.sleep(0.1)
            try:
                option_chain_update, oi_update = await OptionServices.calculate_oi_change(
                symbol=stock.symbol,
                instrument_key=stock.instrument_key,
                expiry_date=expiry_date,
                developer=developer,
                open_price=stock.ohlc.open,
                percentage_change=5,
            )
            except Exception as e:
                print(f"Error updating {stock.symbol}: {e}")
                continue
            option_chain_updates.append(option_chain_update)
            stock_oi_updates.append(oi_update)

        indices = await IndicesRepository.all_indices(only_option=True)

        for index in indices:
            await asyncio.sleep(0.1)
            try:
                option_chain_update, oi_update = await OptionServices.calculate_oi_change(
                    symbol=index.symbol,
                    instrument_key=index.instrument_key,
                    expiry_date=expiry_date,
                    developer=developer,
                    open_price=index.ohlc.open,
                    percentage_change=1,
                )
                option_chain_updates.append(option_chain_update)
                indices_oi_updates.append(oi_update)
            except Exception as e:
                print(f"Error updating {index.symbol}: {e}")
                continue

        if option_chain_updates:

            res1 = await Collections.stocks.bulk_update(stock_oi_updates)
            res2 = await Collections.indices.bulk_update(indices_oi_updates)
            res3 = await Collections.option_chain.bulk_update(option_chain_updates)

            if res1.acknowledged and res2.acknowledged and res3.acknowledged:
                return SuccessResponse(
                    message=f"[update_option_chain_and_oi] Stocks Updated {res1.modified_count}, Indices Updated {res2.modified_count} and Option Chain Updated {res3.modified_count}"
                ).model_dump()
            else:
                return ErrorResponse(
                    message="[update_option_chain_and_oi] No oi were updated."
                ).model_dump()
