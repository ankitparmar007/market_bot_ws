import asyncio
from server.modules.options.update_option_chain import OptionServices
from server.modules.telegram.commands import TGCommands
from server.modules.ticker.volume_ticker import VolumeTicker
from server.modules.ticker.ohlc_ticker import OhlcTicker
from server.utils.logger import log
from server.api import api_client
from server.db import mongodb_client
from asyncio import Task

from server.modules.r_factor.r_factor_updater import RFactor
from server.modules.telegram.telegram import Telegram
from server.modules.ticker.tick_data_updater import Ticker
from server.utils.scheduler import Scheduler
from server.modules.token.repository import TokenRepository


# ==========================================================
# MAIN
# ==========================================================

listen_messages: Task | None = None
ticker_task: Task | None = None
ohlc_ticker_write_task: Task | None = None
volume_ticker_write_task: Task | None = None
update_r_factor_task: Task | None = None
update_oi_task: Task | None = None


retry_count = 3


async def listen_upstox():
    global retry_count, ticker_task, ohlc_ticker_write_task, volume_ticker_write_task
    while True:
        try:
            log.info("Connecting to Upstox feed...")
            await Ticker.run_ws()
        except Exception as e:
            log.error(f"Ticker error: {e}")
            await Telegram.send_message(
                f"Ticker error: {e}, Reconnecting in 5 seconds... retry left: {retry_count}"
            )
            retry_count -= 1
            if retry_count == 0:
                retry_count = 3
                await Telegram.send_message(
                    "Max retries reached. Please check errors and restart."
                )
                ticker_task = None
                if ohlc_ticker_write_task and not ohlc_ticker_write_task.done():
                    ohlc_ticker_write_task.cancel()
                    ohlc_ticker_write_task = None
                if volume_ticker_write_task and not volume_ticker_write_task.done():
                    volume_ticker_write_task.cancel()
                    volume_ticker_write_task = None
                break

        log.warning("Reconnecting in 5 seconds...")
        await asyncio.sleep(5)


async def telgram_message_task_func(text: str):
    global ticker_task, update_r_factor_task, update_oi_task, ohlc_ticker_write_task, volume_ticker_write_task

    if text.lower() == TGCommands.START_TICKER.value:
        if ticker_task and not ticker_task.done():
            await Telegram.send_message("Ticker is running.")
        else:
            await Telegram.send_message("Starting Ticker...")
            ohlc_ticker_write_task = asyncio.create_task(VolumeTicker.db_writer())
            volume_ticker_write_task = asyncio.create_task(OhlcTicker.db_writer())
            ticker_task = asyncio.create_task(listen_upstox())

    elif text.lower() == TGCommands.STOP_TICKER.value:
        if ticker_task and not ticker_task.done():
            await Telegram.send_message("Stopping Ticker...")
            ticker_task.cancel()
            if ohlc_ticker_write_task and not ohlc_ticker_write_task.done():
                ohlc_ticker_write_task.cancel()
                ohlc_ticker_write_task = None
            if volume_ticker_write_task and not volume_ticker_write_task.done():
                volume_ticker_write_task.cancel()
                volume_ticker_write_task = None
            ticker_task = None
            await Telegram.send_message("Ticker stopped.")
        else:
            await Telegram.send_message("Ticker is not running.")

    elif text.lower() == TGCommands.TICKER_STATUS.value:
        if ticker_task and not ticker_task.done():
            await Telegram.send_message("Ticker is running.")
        else:
            await Telegram.send_message("Ticker is not running.")

    elif text.lower() == TGCommands.START_UPDATE_R_FACTOR.value:
        if update_r_factor_task and not update_r_factor_task.done():
            await Telegram.send_message("update_r_factor_task is already running.")
        else:
            await Telegram.send_message("Starting update_r_factor_task...")
            update_r_factor_task = asyncio.create_task(
                Scheduler.run_every_n_minutes(
                    minutes=1,
                    target_second=5,
                    task=RFactor.update_r_factor,
                )
            )

    elif text.lower() == TGCommands.STOP_UPDATE_R_FACTOR.value:
        if update_r_factor_task and not update_r_factor_task.done():
            await Telegram.send_message("Stopping update_r_factor_task...")
            update_r_factor_task.cancel()
            update_r_factor_task = None
            await Telegram.send_message("update_r_factor_task stopped.")
        else:
            await Telegram.send_message("update_r_factor_task is not running.")

    elif text.lower() == TGCommands.START_UPDATE_OI.value:
        if update_oi_task and not update_oi_task.done():
            await Telegram.send_message("update_oi_task is already running.")
        else:
            await Telegram.send_message("Starting update_oi_task...")
            update_oi_task = asyncio.create_task(
                Scheduler.run_every_n_minutes(
                    minutes=4,
                    target_second=30,
                    task=OptionServices.update_option_chain_and_oi,
                )
            )

    elif text.lower() == TGCommands.STOP_UPDATE_OI.value:
        if update_oi_task and not update_oi_task.done():
            await Telegram.send_message("Stopping update_oi_task...")
            update_oi_task.cancel()
            update_oi_task = None
            await Telegram.send_message("update_oi_task stopped.")
        else:
            await Telegram.send_message("update_oi_task is not running.")

    elif text.lower() == TGCommands.REFRESH_TOKEN.value:
        await TokenRepository.refresh_cached_token()
        await Telegram.send_message("Tokens refreshed in cache.")

    elif text.lower() == TGCommands.START.value:
        lines = ["📌 *Available Commands:*"]
        for cmd in TGCommands:
            lines.append(f"{cmd.value}  —  {cmd.name.replace('_', ' ').title()}")
        message = "\n".join(lines)
        await Telegram.send_message(message)


async def main():
    try:
        await api_client.start()
        await Telegram.start()
        await mongodb_client.ensure_connection()
        Telegram.set_message_callback(telgram_message_task_func)
        asyncio.create_task(Telegram.listen_messages())
        await asyncio.Event().wait()

    except asyncio.CancelledError:
        log.warning("Main task cancelled")

    finally:
        log.info("Shutting down services...")

        await api_client.close()
        await Telegram.close()  
        await mongodb_client.close()

        log.info("Cleanup completed")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("Exiting...")


# if __name__ == "__main__":
#     count = 1
#     while True:
#         try:
#             asyncio.run(main())
#         except KeyboardInterrupt:
#             log.warning("Exiting...")
#             break
#         except Exception as e:
#             log.error(f"Main loop error: {e}, restarting... attempt #{count}")
#             if count >= 5:
#                 log.error("Max main loop retries reached, exiting.")
#                 break
#             time.sleep(2**count)  # exponential backoff before restart
#             count += 1
