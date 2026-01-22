from pymongo import UpdateOne
from config import Config
from server.db.collections import Collections
from server.db.exceptions import AppException
from server.modules.options.models import OptionChain
from server.modules.stocks.repository import StockRepository
from server.modules.token.enums import Developer
from server.modules.upstox.services import UpstoxServices
from server.utils.ist import IndianDateTime


class OptionServices:

    @staticmethod
    async def update_option_chain_and_oi():
        stocks = StockRepository.all_stocks()
        oi_updates: list[UpdateOne] = []
        option_chain_updates: list[UpdateOne] = []
        for stock in stocks:
            # print(f"Updating option chain for {stock.symbol}")
            strikes = await UpstoxServices.option_chain(
                instrument_key=stock.instrument_key,
                expiry_date=Config.EXPIRY_DATE,
                developer=Developer.ANKIT,
            )

            option_chain_updates.append(
                UpdateOne(
                    upsert=True,
                    filter={"symbol": stock.symbol},
                    update={
                        "$set": {
                            "symbol": stock.symbol,
                            "instrument_key": stock.instrument_key,
                            "expiry_date": Config.EXPIRY_DATE,
                            "strikes": [s.model_dump() for s in strikes],
                            "updated_at": IndianDateTime.now_isoformat(),
                        }
                    },
                )
            )

            percentage_change = 5 / 100
            open = stock.ohlc.open
            upper = open + (open * percentage_change)
            lower = open - (open * percentage_change)

            # Generator to avoid creating a list
            filtered = (s for s in strikes if lower < s.strike_price < upper)

            # Use local lambdas to avoid repeated deep attribute lookups inside sum
            def c_prev(s: OptionChain):
                return s.call_options.market_data.prev_oi

            def p_prev(s: OptionChain):
                return s.put_options.market_data.prev_oi

            def c_now(s: OptionChain):
                return s.call_options.market_data.oi

            def p_now(s: OptionChain):
                return s.put_options.market_data.oi

            # Materialize filtered once so we don't iterate it 4 times
            filtered_list = list(filtered)

            total_prev_call_oi = sum(c_prev(s) for s in filtered_list)
            total_prev_put_oi = sum(p_prev(s) for s in filtered_list)
            total_call_oi = sum(c_now(s) for s in filtered_list)
            total_put_oi = sum(p_now(s) for s in filtered_list)

            # Percentages
            call_prev = total_prev_call_oi
            put_prev = total_prev_put_oi

            call_pct = (
                ((total_call_oi - call_prev) / call_prev * 100) if call_prev else 0
            )
            put_pct = ((total_put_oi - put_prev) / put_prev * 100) if put_prev else 0

            net_oi_change_percent = put_pct - call_pct

            oi_updates.append(
                UpdateOne(
                    upsert=False,
                    filter={"symbol": stock.symbol},
                    update={
                        "$set": {
                            "total_call_oi_change_percent": round(call_pct, 2),
                            "total_put_oi_change_percent": round(put_pct, 2),
                            "net_oi_change_percent": round(net_oi_change_percent, 2),
                            "updated_at": IndianDateTime.now_isoformat(),
                        }
                    },
                )
            )

        if oi_updates and option_chain_updates:

            res1 = Collections.stocks.bulk_update(oi_updates)
            res2 = Collections.option_chain.bulk_update(option_chain_updates)

            if not (
                res1.acknowledged
                and res2.acknowledged
                and res1.modified_count > 0
                and res2.modified_count > 0
            ):
                raise AppException("[OptionServices.update_option_chain_and_oi] Failed to update oi or option chain")
