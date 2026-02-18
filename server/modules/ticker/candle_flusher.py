from pymongo import UpdateOne
from server.db.collections import Collections, TicksCollections
from server.modules.ticker.ohlc_ticker import OhlcTicker
from server.modules.ticker.volume_ticker import VolumeTicker
from typing import List
from server.utils.logger import log
from server.utils.time_tracker import timing_decorator


class CandleFlusher:
    @staticmethod
    @timing_decorator
    async def flush_candles_to_db():
        list = await TicksCollections.ticks.aggregate(
            [{"$group": {"_id": "$instrument_key"}}]
        )
        instrument_keys = [doc["_id"] for doc in list]
        # print(instrument_keys)

        ohlc_history_list: List[UpdateOne] = []
        volume_history_list: List[UpdateOne] = []

        for instrument_key in instrument_keys:

            print(f"Processing instrument_key1 : {instrument_key}")

            ticks = await TicksCollections.ticks.find(
                {"instrument_key": instrument_key},
                {
                    "fullFeed.marketFF.marketLevel": 0,
                    "fullFeed.marketFF.optionGreeks": 0,
                    "fullFeed.marketFF.tbq": 0,
                    "fullFeed.marketFF.tsq": 0,
                },
            )

            symbol = ticks[0]["_id"]

            ohlc_history = OhlcTicker.extract_unique_I1_minutes(
                ticks=ticks,
            )

            ohlc_history_list.append(
                UpdateOne(
                    {"instrument_key": instrument_key},
                    {
                        "$set": {
                            "symbol": symbol,
                            "history": ohlc_history,
                            "instrument_key": instrument_key,
                        }
                    },
                    upsert=True,
                )
            )

            print(f"Processing instrument_key2 : {instrument_key}")

            volume_history = VolumeTicker.process_volume_ticks(
                ticks=ticks,
            )
            volume_history_list.append(
                UpdateOne(
                    {"instrument_key": instrument_key},
                    {
                        "$set": {
                            "symbol": symbol,
                            "history": volume_history,
                            "instrument_key": instrument_key,
                        }
                    },
                    upsert=True,
                )
            )
        res1 = await Collections.intraday_history.bulk_update(ohlc_history_list)
        res2 = await Collections.volume_history.bulk_update(volume_history_list)
        log.info(
            f"CandleFlusher: Flushed candles to DB. Ohlc modified: {res1.modified_count}, Ohlc upserted: {res1.upserted_count}, Volume modified: {res2.modified_count}, Volume upserted: {res2.upserted_count}"
        )
