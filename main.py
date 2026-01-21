import asyncio
import time
from server.modules.options.update_option_chain import OptionServices
from server.modules.telegram.commands import TGCommands
from server.utils.logger import log
from server.api import api_client
from asyncio import Task

from server.modules.r_factor.r_factor_updater import RFactor
from server.modules.telegram.telegram import Telegram
from server.modules.ticker.tick_data_updater import Ticker
from server.utils.scheduler import Scheduler


# ==========================================================
# MAIN
# ==========================================================

listen_messages: Task | None = None
writer_task: Task | None = None
ws_task: Task | None = None
update_r_factor_task: Task | None = None
update_oi_task: Task | None = None


retry_count = 3


async def listen_upstox():
    global retry_count, ws_task
    while True:
        try:
            log.info("Connecting to Upstox feed...")
            await Ticker.run_ws()
        except Exception as e:
            log.error(f"WS error: {e}")
            await Telegram.send_message(
                f"Upstox WS error: {e}, Reconnecting in 5 seconds... retry left: {retry_count}"
            )
            retry_count -= 1
            if retry_count == 0:
                retry_count = 3
                await Telegram.send_message(
                    "Max retries reached. Please check errors and restart."
                )
                ws_task = None
                break

        log.warning("Reconnecting in 5 seconds...")
        await asyncio.sleep(5)


async def telgram_message_task_func(text: str):
    global ws_task, update_r_factor_task, update_oi_task  # must be at top!

    if text.lower() == TGCommands.START_WS.value:
        if ws_task and not ws_task.done():
            await Telegram.send_message("WS is already running.")
        else:
            await Telegram.send_message("Starting WS...")
            ws_task = asyncio.create_task(listen_upstox())

    elif text.lower() == TGCommands.STOP_WS.value:
        if ws_task and not ws_task.done():
            await Telegram.send_message("Stopping WS...")
            ws_task.cancel()
            ws_task = None
            await Telegram.send_message("WS stopped.")
        else:
            await Telegram.send_message("WS is not running.")

    elif text.lower() == TGCommands.STATUS.value:
        if ws_task and not ws_task.done():
            await Telegram.send_message("WS is running.")
        else:
            await Telegram.send_message("WS is not running.")

    elif text.lower() == TGCommands.DOCS_BUFFER.value:
        await Telegram.send_message(f"Current buffer size: {len(Ticker.docs)}")

    elif text.lower() == TGCommands.FLUSH_DOCS.value:
        await Ticker.flush_batch()
        await Telegram.send_message("Flushed docs buffer.")

    elif text.lower() == TGCommands.START_UPDATE_R_FACTOR.value:
        if update_r_factor_task and not update_r_factor_task.done():
            await Telegram.send_message("update_r_factor_task is already running.")
        else:
            await Telegram.send_message("Starting update_r_factor_task...")
            update_r_factor_task = asyncio.create_task(
                Scheduler.run_every_n_minutes(
                    minutes=1,
                    target_second=3,
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
                    minutes=3,
                    target_second=3,
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
        from server.modules.token.repository import TokenRepository

        TokenRepository.refresh_cached_token()
        await Telegram.send_message("Tokens refreshed in cache.")

    elif text.lower() == TGCommands.START.value:
        lines = ["📌 *Available Commands:*"]
        for cmd in TGCommands:
            lines.append(f"{cmd.value}  —  {cmd.name.replace('_', ' ').title()}")
        message = "\n".join(lines)
        await Telegram.send_message(message)


async def main():
    await api_client.start()
    await Telegram.start()
    Telegram.set_message_callback(telgram_message_task_func)
    listen_messages = asyncio.create_task(Telegram.listen_messages())
    writer_task = asyncio.create_task(Ticker.db_writer())

    # Start DB writer and WS listener concurrently

    await asyncio.gather(listen_messages, writer_task)


if __name__ == "__main__":
    count = 1
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.warning("Exiting...")
            break
        except Exception as e:
            log.error(f"Main loop error: {e}, restarting... attempt #{count}")
            if count >= 5:
                log.error("Max main loop retries reached, exiting.")
                break
            time.sleep(2**count)  # exponential backoff before restart
            count += 1
