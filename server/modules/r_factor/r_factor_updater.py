import asyncio
from datetime import datetime, timedelta
from time import time

from server.api.client import APIClient
from server.modules.telegram.telegram import Telegram
from server.utils.utils import IndianDateTime


class RFactor:

    @staticmethod
    async def sleep_until(t: datetime):
        now = IndianDateTime.now()
        delay = (t - now).total_seconds()
        if delay > 0:
            await asyncio.sleep(delay)

    @staticmethod
    async def update():

        START_TIME = IndianDateTime.now().replace(
            hour=9, minute=16, second=3, microsecond=0
        )
        END_TIME = IndianDateTime.now().replace(
            hour=15, minute=30, second=0, microsecond=0
        )

        async with APIClient() as client:
            while True:
                now = IndianDateTime.now()

                # -------- BEFORE MARKET HOURS --------
                if now < START_TIME:
                    print("Market not open. Sleeping until", START_TIME)
                    await Telegram.send_message(
                        f"[update_r_factor_loop] Market not open. Sleeping until{START_TIME}",
                    )
                    await RFactor.sleep_until(START_TIME)

                # -------- AFTER MARKET HOURS --------
                elif now > END_TIME:
                    print("Market closed. Stopping worker.")
                    await Telegram.send_message(
                        f"[update_r_factor_loop] Market closed. Stopping worker.",
                    )
                    break  # or sleep until next day 9:15

                # ---- WAIT UNTIL NEXT :03 SECOND ----
                target_second = 3

                if now.second > target_second:
                    next_time = (now + timedelta(minutes=1)).replace(
                        second=target_second, microsecond=0
                    )
                else:
                    next_time = now.replace(second=target_second, microsecond=0)

                print(
                    f"Sleeping {(next_time - now).total_seconds():.2f}s until {next_time}"
                )
                await RFactor.sleep_until(next_time)

                # -------- START CYCLE --------
                fetch_option_chain = 1 if (now.minute % 4 == 0) else 0
                print(
                    "Starting cycle at",
                    IndianDateTime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Fetching option chain:",
                    fetch_option_chain,
                )

                start = time()
                try:
                    response = await client.get_json(
                        "https://market-bot-api-five.vercel.app/update_data",
                        params={
                            "fetch_option_chain": fetch_option_chain,
                            "password": "valid123",
                        },
                        headers={
                            "Content-Type": "application/json",
                        },
                    )
                    print("Update data response:", response)

                except Exception as e:
                    print("Error in loop:", e)
                    await Telegram.send_message(
                        f"[update_r_factor_loop] Stopped! Error in loop: {e}",
                    )
                    break

                print(f"Cycle completed. Time taken: {time() - start:.2f}s\n")


# async def main():
# async with APIClient() as client:
# await worker_loop(client)


# if __name__ == "__main__":
#     asyncio.run(main())
