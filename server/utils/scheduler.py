import asyncio
from datetime import timedelta
from time import time
from server.api.exceptions import AppException
from server.modules.telegram.telegram import Telegram
from server.utils.ist import IndianDateTime
from server.utils.logger import log


class Scheduler:
    @staticmethod
    async def run_every_n_minutes(
        minutes: int,
        target_second: int,
        task,
    ):
        assert minutes > 0
        assert 0 <= target_second < 60

        while True:
            now = IndianDateTime.now()
            minute_bucket = (now.minute // minutes + 1) * minutes
            next_run = now.replace(
                minute=minute_bucket % 60,
                second=target_second,
                microsecond=0,
            )

            if minute_bucket >= 60:
                next_run += timedelta(hours=1)

            sleep_duration = (next_run - now).total_seconds()

            if sleep_duration <= 0:
                sleep_duration += minutes * 60

            log.info(
                f"[Scheduler.{task.__name__}]: Next run at {next_run.strftime('%Y-%m-%d %H:%M:%S')} (in {sleep_duration} seconds)\n"
            )

            await asyncio.sleep(sleep_duration)

            try:
                print(f"[Scheduler.{task.__name__}] Starting task...")
                start = time()
                await task()
                print(f"[Scheduler.{task.__name__}] Cycle completed. Time taken: {time() - start:.2f}s\n")
            except AppException as e:
                log.error(f"[Scheduler] Error in scheduled task: {e}")
                await Telegram.send_message(f"[Scheduler] {e} for task {task.__name__}")
                break
            except Exception as e:
                log.error(f"[Scheduler] Unexpected error in task: {e}")
                await Telegram.send_message(f"[Scheduler] Unexpected error:{e} for task {task.__name__}")
                break
        log.info(f"Scheduler: cleanly exiting the scheduler for task {task.__name__}")
        await Telegram.send_message(
            f"[Scheduler] Exiting the scheduler for task {task.__name__}"
        )
