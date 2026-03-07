# from server.db import mongodb_client, mongodb_ticks_client

# from server.modules.stocks.repository import StockRepository
# from server.modules.ticker.volume_ticker import VolumeTicker
# from server.utils.is_dt import ISDateTime
# from server.utils.logger import log
# from server.utils.time_tracker import timing_decorator

# import asyncio
# from server.utils.logger import log
# from server.db.collections import TicksCollections


# @timing_decorator
# async def check():

#     vt = VolumeTicker()

#     stocks = await StockRepository.all_stocks()

#     for stock in stocks:
#         print("Processing ", stock.symbol)
#         ticks = await TicksCollections.ticks.find(
#             {
#                 "instrument_key": stock.instrument_key,
#             }
#         )
        
#         if not ticks:
#             print(f"Ticks not found for {stock.symbol}")

#         ticks.sort(key=lambda x: x["timestamp"])

#         for tick in ticks:
#             await vt.process_tick(
#                 symbol=tick["_id"],
#                 ltp=tick["ltp"],
#                 ltt=ISDateTime.utc_to_ist(tick["timestamp"]),
#                 vtt=tick["vtt"],
#             )

#     await vt.dispose()




# async def main():
#     await mongodb_client.ensure_connection()
#     await mongodb_ticks_client.ensure_connection()

#     try:

#         await check()

#     except asyncio.CancelledError:
#         log.warning("Main task cancelled")

#     finally:
#         log.info("Shutting down services...")

#     await mongodb_client.close()
#     await mongodb_ticks_client.close()

#     log.info("Cleanup completed")


# if __name__ == "__main__":
#     try:
#         asyncio.run(main())
#     except KeyboardInterrupt:
#         log.warning("Exiting...")




from server.db import mongodb_client, mongodb_ticks_client,clickhouse_client

from server.modules.stocks.repository import StockRepository
from server.modules.ticker.volume_ticker_clickhouse import VolumeTicker
from server.utils.is_dt import ISDateTime
from server.utils.logger import log
from server.utils.time_tracker import timing_decorator

import asyncio
from server.utils.logger import log
from server.db.collections import TicksCollections


@timing_decorator
async def check():

    vt = VolumeTicker()

    stocks = await StockRepository.all_stocks()

    for stock in stocks:
        print("Processing ", stock.symbol)
        ticks = await TicksCollections.ticks.find(
            {
                "instrument_key": stock.instrument_key,
            }
        )
        
        if not ticks:
            print(f"Ticks not found for {stock.symbol}")

        ticks.sort(key=lambda x: x["timestamp"])
        

        for tick in ticks:
            print(tick["_id"],ISDateTime.utc_to_ist(tick["timestamp"]))
            
            await vt.process_tick(
                symbol=tick["_id"],
                ltp=tick["ltp"],
                ltt=ISDateTime.utc_to_ist(tick["timestamp"]),
                vtt=tick["vtt"],
            )

    await vt.dispose()




async def main():
    await mongodb_client.ensure_connection()
    await mongodb_ticks_client.ensure_connection()
    await clickhouse_client.ensure_connection()

    try:

        await check()

    except asyncio.CancelledError:
        log.warning("Main task cancelled")

    finally:
        log.info("Shutting down services...")

    await mongodb_client.close()
    await mongodb_ticks_client.close()
    await clickhouse_client.close()

    log.info("Cleanup completed")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("Exiting...")
