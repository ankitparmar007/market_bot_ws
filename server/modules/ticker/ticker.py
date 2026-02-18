import asyncio
from asyncio import Task
from typing import Dict, Any, List
import websockets
import json
import uuid
from server.modules.token.enums import Developer
from server.modules.token.repository import TokenRepository
from server.utils.ist import IndianDateTime
from server.utils.logger import log
from google.protobuf.json_format import MessageToDict
from server.db.collections import Collections, TicksCollections
import server.modules.ticker.marketfeed_pb2 as pb
from server.modules.telegram.telegram import Telegram


class Ticker:

    run_ws_task: Task | None = None
    db_writer_task: Task | None = None
    retry_count = 3

    write_queue: asyncio.Queue = asyncio.Queue(maxsize=50000)
    docs: List[dict] = []
    BATCH_SIZE = 2000
    FLUSH_INTERVAL = 3  # seconds
    URL = "wss://api.upstox.com/v3/feed/market-data-feed"

    # -----------------------------------------
    # Decode Protobuf
    # -----------------------------------------
    @staticmethod
    def decode_protobuf(buffer: bytes) -> Dict[str, Any]:
        obj = pb.FeedResponse()
        obj.ParseFromString(buffer)
        return MessageToDict(obj)

    # -----------------------------------------
    # Flush Batch to Mongo
    # -----------------------------------------
    @classmethod
    async def flush_batch(cls):
        if not cls.docs:
            return
        try:
            await TicksCollections.ticks.insert_many(cls.docs)
        except Exception as e:
            await Telegram.send_message(f"[Ticker.flush_batch.Exception]: {e}")
            print("Batch insert failed:", e)

        cls.docs = []

    # ----------------- ------------------------
    # Database Writer
    # -----------------------------------------
    @classmethod
    async def db_writer(cls):
        print("[TickWriter] Started")

        last_flush = asyncio.get_running_loop().time()

        try:
            while True:
                try:
                    doc = await asyncio.wait_for(cls.write_queue.get(), timeout=1)
                    cls.docs.append(doc)
                    cls.write_queue.task_done()
                except asyncio.TimeoutError:
                    pass

                now = asyncio.get_running_loop().time()

                if len(cls.docs) >= cls.BATCH_SIZE or (
                    cls.docs and now - last_flush >= cls.FLUSH_INTERVAL
                ):
                    await cls.flush_batch()
                    last_flush = now

        except asyncio.CancelledError:
            await cls.flush_batch()
            await Telegram.send_message(f"[Ticker.db_writer.stopped/Cancelled]")
            print("[Ticker.db_writer.stopped]")

    # -----------------------------------------
    # WebSocket Listener
    # -----------------------------------------
    @classmethod
    async def run_ws(cls):

        AUTH_TOKEN = await TokenRepository.get_token(Developer.ANKIT)
        headers = {"Authorization": f"Bearer {AUTH_TOKEN}"}

        stocks = await Collections.stocks.find(
            {}, {"_id": 0, "instrument_key": 1, "symbol": 1}
        )

        stock_instruments_and_symbol: Dict[str, str] = {
            doc["instrument_key"]: doc["symbol"] for doc in stocks
        }

        STOCK_INSTRUMENTS = list(stock_instruments_and_symbol.keys())

        instrumentKeys = STOCK_INSTRUMENTS

        async with websockets.connect(
            cls.URL,
            additional_headers=headers,
            ping_interval=20,
            ping_timeout=10,
        ) as ws:

            payload = {
                "guid": str(uuid.uuid4()),
                "method": "sub",
                "data": {
                    "mode": "full",
                    "instrumentKeys": instrumentKeys,
                },
            }

            await Telegram.send_message("Upstox WS connected and subscribing...")

            await ws.send(json.dumps(payload).encode())

            await Telegram.send_message(
                "Upstox WS subscription sent for instruments. {}".format(
                    len(instrumentKeys)
                )
            )

            while True:
                message = await ws.recv()

                if isinstance(message, bytes):
                    data = cls.decode_protobuf(message)

                    if data.get("type") not in (
                        "initial_feed",
                        "live_feed",
                    ):
                        continue

                    feeds = data.get("feeds", {})

                    currentTs = IndianDateTime.fromtimestamp(data.get("currentTs", ""))

                    for instrument_key, feed in feeds.items():
                        mf: dict = feed.get("fullFeed", {}).get("marketFF", {})
                        if not mf:
                            continue

                        tick_doc = {
                            "timestamp": currentTs,
                            "instrument_key": instrument_key,
                            "_id": stock_instruments_and_symbol[instrument_key],
                            "ltpc": mf.get("ltpc", {}),
                            "marketOHLC": mf.get("marketOHLC", {}),
                            "vtt": mf.get("vtt", {}),
                            "tbq": mf.get("tbq", {}),
                            "tsq": mf.get("tsq", {}),
                            "oi": mf.get("oi", 0),
                        }

                        try:
                            cls.write_queue.put_nowait(tick_doc)
                        except asyncio.QueueFull:
                            await Telegram.send_message(
                                f"Upstox WS queue is full {cls.write_queue.qsize()}"
                            )

    @classmethod
    async def start(cls):

        if cls.run_ws_task and not cls.run_ws_task.done():
            await Telegram.send_message("Ticker is already running.")
            return

        retry = 0
        base_backoff = 5

        while retry < cls.retry_count:
            backoff = base_backoff * (2**retry)

            try:
                log.info("Starting Ticker services...")

                # Start DB writer if not running
                if not cls.db_writer_task or cls.db_writer_task.done():
                    cls.db_writer_task = asyncio.create_task(cls.db_writer())

                # Start WebSocket
                cls.run_ws_task = asyncio.create_task(cls.run_ws())

                # Wait until one of them crashes
                done, pending = await asyncio.wait(
                    [cls.run_ws_task, cls.db_writer_task],
                    return_when=asyncio.FIRST_EXCEPTION,
                )

                # If any task failed → raise its exception
                for task in done:
                    task.result()

                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

            except Exception as e:
                retry += 1
                log.error(f"Ticker crashed: {e}")

                await Telegram.send_message(
                    f"[Ticker] Crash detected\nError: {e}\nRetry {retry}/{cls.retry_count}"
                )

                # Cancel running tasks
                for task in [cls.run_ws_task, cls.db_writer_task]:
                    if task and not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass

                await Telegram.send_message(
                    f"[Ticker] Reconnecting in {backoff} seconds..."
                )

                await asyncio.sleep(backoff)

        cls.run_ws_task = None
        cls.db_writer_task = None
        await Telegram.send_message("[Ticker] Max retries reached. Stopping Ticker.")

        log.error("Ticker stopped after max retries.")

    @classmethod
    async def stop(cls):
        ws_task = cls.run_ws_task
        writer_task = cls.db_writer_task

        if ws_task is None and writer_task is None:
            await Telegram.send_message("Ticker is not running.")
            return

        if ws_task is not None:
            if not ws_task.done():
                ws_task.cancel()
                try:
                    await ws_task
                except asyncio.CancelledError:
                    pass

        if writer_task is not None:
            if not writer_task.done():
                writer_task.cancel()
                try:
                    await writer_task
                except asyncio.CancelledError:
                    pass

        cls.run_ws_task = None
        cls.db_writer_task = None

        await Telegram.send_message("Ticker stopped.")
        
    @classmethod
    async def status(cls):
        if cls.run_ws_task and not cls.run_ws_task.done():
            await Telegram.send_message("Ticker is running.")
        else:
            await Telegram.send_message("Ticker is not running.")

