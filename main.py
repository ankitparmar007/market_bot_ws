import asyncio
from server.modules.options.update_option_chain import OptionServices
from server.modules.telegram.commands import TGCommands

from server.utils.logger import log
from server.api import api_client
from server.db import mongodb_client, clickhouse_client
from asyncio import Task

from server.modules.r_factor.r_factor_updater import RFactor
from server.modules.telegram.telegram import Telegram
from server.modules.ticker.ticker import Ticker
from server.utils.scheduler import Scheduler
from server.modules.token.repository import TokenRepository


# ==========================================================
# MAIN
# ==========================================================

listen_messages: Task | None = None
update_r_factor_task: Task | None = None
update_oi_task: Task | None = None


async def telgram_message_task_func(text: str):
    global update_r_factor_task, update_oi_task
    log.info("TG message received: " + text)
    if text.lower() == TGCommands.START_TICKER.value:
        await Ticker.start()

    elif text.lower() == TGCommands.STOP_TICKER.value:
        await Ticker.stop()

    elif text.lower() == TGCommands.TICKER_STATUS.value:
        await Ticker.status()

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
                    target_second=10,
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
        await Telegram.start()
        Telegram.set_message_callback(telgram_message_task_func)
        asyncio.create_task(Telegram.listen_messages())
        await api_client.start()
        await mongodb_client.ensure_connection()
        await clickhouse_client.ensure_connection()
        # await mongodb_ticks_client.ensure_connection()
        await asyncio.Event().wait()

    except asyncio.CancelledError:
        log.warning("Main task cancelled")

    finally:
        log.info("Shutting down services...")
        await api_client.close()
        await mongodb_client.close()
        await clickhouse_client.close()
        # await mongodb_ticks_client.close()
        await Telegram.close()
        

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
