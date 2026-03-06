# import asyncio
# from asyncio import Task
# import datetime
# import time
# from typing import Dict, Any, List
# import websockets
# import json
# import uuid
# from server.modules.token.enums import Developer
# from server.modules.token.repository import TokenRepository
# from server.utils.is_dt import ISDateTime
# from server.utils.logger import log
# from google.protobuf.json_format import MessageToDict
# from server.db.collections import Collections, TicksCollections
# import server.modules.ticker.marketfeed_pb2 as pb
# from server.modules.telegram.telegram import Telegram


# class Ticker:

#     run_ws_task: Task | None = None
#     db_writer_task: Task | None = None
#     retry_count = 3

#     write_queue: asyncio.Queue = asyncio.Queue(maxsize=50000)
#     docs: List[dict] = []
#     BATCH_SIZE = 2000
#     FLUSH_INTERVAL = 3  # seconds
#     URL = "wss://api.upstox.com/v3/feed/market-data-feed"

#     # -----------------------------------------
#     # Decode Protobuf
#     # -----------------------------------------
#     @staticmethod
#     def decode_protobuf(buffer: bytes) -> Dict[str, Any]:
#         obj = pb.FeedResponse()
#         obj.ParseFromString(buffer)
#         return MessageToDict(obj)

#     # -----------------------------------------
#     # Flush Batch to Mongo
#     # -----------------------------------------
#     @classmethod
#     async def flush_batch(cls):
#         if not cls.docs:
#             return
#         try:
#             await TicksCollections.ticks.insert_many(cls.docs)
#         except Exception as e:
#             await Telegram.send_message(f"[Ticker.flush_batch.Exception]: {e}")
#             print("Batch insert failed:", e)

#         cls.docs = []

#     # ----------------- ------------------------
#     # Database Writer
#     # -----------------------------------------
#     @classmethod
#     async def db_writer(cls):
#         print("[TickWriter] Started")

#         last_flush = asyncio.get_running_loop().time()

#         try:
#             while True:
#                 try:
#                     doc = await asyncio.wait_for(cls.write_queue.get(), timeout=1)
#                     cls.docs.append(doc)
#                     cls.write_queue.task_done()
#                 except asyncio.TimeoutError:
#                     pass

#                 now = asyncio.get_running_loop().time()

#                 if len(cls.docs) >= cls.BATCH_SIZE or (
#                     cls.docs and now - last_flush >= cls.FLUSH_INTERVAL
#                 ):
#                     await cls.flush_batch()
#                     last_flush = now

#         except asyncio.CancelledError:
#             await cls.flush_batch()
#             await Telegram.send_message(f"[Ticker.db_writer.stopped/Cancelled]")
#             print("[Ticker.db_writer.stopped]")

#     # -----------------------------------------
#     # WebSocket Listener
#     # -----------------------------------------
#     @classmethod
#     async def run_ws(cls):

#         AUTH_TOKEN = await TokenRepository.get_token(Developer.ANKIT)
#         headers = {"Authorization": f"Bearer {AUTH_TOKEN}"}

#         stocks = await Collections.stocks.find(
#             {}, {"_id": 0, "instrument_key": 1, "symbol": 1}
#         )

#         stock_instruments_and_symbol: Dict[str, str] = {
#             doc["instrument_key"]: doc["symbol"] for doc in stocks
#         }

#         STOCK_INSTRUMENTS = list(stock_instruments_and_symbol.keys())

#         instrumentKeys = STOCK_INSTRUMENTS

#         MARKET_START = datetime.time(9, 15, 0, 0)
#         MARKET_END = datetime.time(15, 30, 0, 0)

#         today = ISDateTime.now().date()

#         async with websockets.connect(
#             cls.URL,
#             additional_headers=headers,
#             ping_interval=20,
#             ping_timeout=10,
#         ) as ws:

#             payload = {
#                 "guid": str(uuid.uuid4()),
#                 "method": "sub",
#                 "data": {
#                     "mode": "full",
#                     "instrumentKeys": instrumentKeys,
#                 },
#             }

#             await Telegram.send_message(
#                 f"Upstox WS connected and subscribing... date : {today.isoformat()}"
#             )

#             await ws.send(json.dumps(payload).encode())

#             await Telegram.send_message(
#                 "Upstox WS subscription sent for instruments. {}".format(
#                     len(instrumentKeys)
#                 )
#             )

#             while True:
#                 message = await ws.recv()

#                 if isinstance(message, bytes):
#                     data = cls.decode_protobuf(message)

#                     if data.get("type") not in (
#                         "initial_feed",
#                         "live_feed",
#                     ):
#                         continue

#                     feeds = data.get("feeds", {})

#                     # currentTs = IndianDateTime.fromtimestamp(data.get("currentTs", ""))

#                     for instrument_key, feed in feeds.items():
#                         mf: dict = feed.get("fullFeed", {}).get("marketFF", {})
#                         if not mf:
#                             continue

#                         ltpc = mf.get("ltpc", {})
#                         if not ltpc:
#                             continue

#                         ltt = ltpc.get("ltt", 0)
#                         if not ltt:
#                             continue

#                         timestamp = ISDateTime.fromtimestamp(ltt)

#                         if timestamp.date() != today or not (
#                             MARKET_START <= timestamp.time() <= MARKET_END
#                         ):
#                             continue

#                         tick_doc = {
#                             "timestamp": timestamp,
#                             "instrument_key": instrument_key,
#                             "_id": stock_instruments_and_symbol[instrument_key],
#                             "ltp": ltpc.get("ltp", 0),
#                             "vtt": int(mf.get("vtt", 0)),
#                         }

#                         try:
#                             cls.write_queue.put_nowait(tick_doc)
#                         except asyncio.QueueFull:
#                             await Telegram.send_message(
#                                 f"Upstox WS queue is full {cls.write_queue.qsize()}"
#                             )

#     @classmethod
#     async def start(cls):

#         if cls.run_ws_task and not cls.run_ws_task.done():
#             await Telegram.send_message("Ticker is already running.")
#             return

#         retry = 0
#         base_backoff = 5

#         while retry < cls.retry_count:
#             backoff = base_backoff * (2**retry)

#             try:
#                 log.info("Starting Ticker services...")

#                 # Start DB writer if not running
#                 if not cls.db_writer_task or cls.db_writer_task.done():
#                     cls.db_writer_task = asyncio.create_task(cls.db_writer())

#                 # Start WebSocket
#                 cls.run_ws_task = asyncio.create_task(cls.run_ws())

#                 # Wait until one of them crashes
#                 done, pending = await asyncio.wait(
#                     [cls.run_ws_task, cls.db_writer_task],
#                     return_when=asyncio.FIRST_EXCEPTION,
#                 )

#                 # If any task failed → raise its exception
#                 for task in done:
#                     task.result()

#                 for task in pending:
#                     task.cancel()
#                     try:
#                         await task
#                     except asyncio.CancelledError:
#                         pass

#             except Exception as e:
#                 retry += 1
#                 log.error(f"Ticker crashed: {e}")

#                 await Telegram.send_message(
#                     f"[Ticker] Crash detected\nError: {e}\nRetry {retry}/{cls.retry_count}"
#                 )

#                 # Cancel running tasks
#                 for task in [cls.run_ws_task, cls.db_writer_task]:
#                     if task and not task.done():
#                         task.cancel()
#                         try:
#                             await task
#                         except asyncio.CancelledError:
#                             pass

#                 await Telegram.send_message(
#                     f"[Ticker] Reconnecting in {backoff} seconds..."
#                 )

#                 await asyncio.sleep(backoff)

#         cls.run_ws_task = None
#         cls.db_writer_task = None
#         await Telegram.send_message("[Ticker] Max retries reached. Stopping Ticker.")

#         log.error("Ticker stopped after max retries.")

#     @classmethod
#     async def stop(cls):
#         ws_task = cls.run_ws_task
#         writer_task = cls.db_writer_task

#         if ws_task is None and writer_task is None:
#             await Telegram.send_message("Ticker is not running.")
#             return

#         if ws_task is not None:
#             if not ws_task.done():
#                 ws_task.cancel()
#                 try:
#                     await ws_task
#                 except asyncio.CancelledError:
#                     pass

#         if writer_task is not None:
#             if not writer_task.done():
#                 writer_task.cancel()
#                 try:
#                     await writer_task
#                 except asyncio.CancelledError:
#                     pass

#         cls.run_ws_task = None
#         cls.db_writer_task = None

#         await Telegram.send_message("Ticker stopped.")

#     @classmethod
#     async def status(cls):
#         if cls.run_ws_task and not cls.run_ws_task.done():
#             await Telegram.send_message("Ticker is running.")
#         else:
#             await Telegram.send_message("Ticker is not running.")


# import asyncio
# from asyncio import Task
# import datetime
# import time
# from typing import Dict, Any, List
# import websockets
# import json
# import uuid
# from server.modules.token.enums import Developer
# from server.modules.token.repository import TokenRepository
# from server.utils.is_dt import ISDateTime
# from server.utils.logger import log
# from google.protobuf.json_format import MessageToDict
# from server.db.collections import Collections, TicksCollections
# import server.modules.ticker.marketfeed_pb2 as pb
# from server.modules.telegram.telegram import Telegram


# class Ticker:

#     run_ws_task: Task | None = None
#     db_writer_task: Task | None = None
#     retry_count = 3

#     write_queue: asyncio.Queue = asyncio.Queue(maxsize=50000)
#     docs: List[dict] = []
#     BATCH_SIZE = 2000
#     FLUSH_INTERVAL = 3  # seconds
#     URL = "wss://api.upstox.com/v3/feed/market-data-feed"

#     # -----------------------------------------
#     # Decode Protobuf
#     # -----------------------------------------
#     @staticmethod
#     def decode_protobuf(buffer: bytes) -> Dict[str, Any]:
#         obj = pb.FeedResponse()
#         obj.ParseFromString(buffer)
#         return MessageToDict(obj)

#     # -----------------------------------------
#     # Flush Batch to Mongo
#     # -----------------------------------------
#     @classmethod
#     async def flush_batch(cls):
#         if not cls.docs:
#             return
#         try:
#             await TicksCollections.ticks.insert_many(cls.docs)
#         except Exception as e:
#             await Telegram.send_message(f"[Ticker.flush_batch.Exception]: {e}")
#             print("Batch insert failed:", e)

#         cls.docs = []

#     # ----------------- ------------------------
#     # Database Writer
#     # -----------------------------------------
#     @classmethod
#     async def db_writer(cls):
#         print("[TickWriter] Started")

#         last_flush = asyncio.get_running_loop().time()

#         try:
#             while True:
#                 try:
#                     doc = await asyncio.wait_for(cls.write_queue.get(), timeout=1)
#                     cls.docs.append(doc)
#                     cls.write_queue.task_done()
#                 except asyncio.TimeoutError:
#                     pass

#                 now = asyncio.get_running_loop().time()

#                 if len(cls.docs) >= cls.BATCH_SIZE or (
#                     cls.docs and now - last_flush >= cls.FLUSH_INTERVAL
#                 ):
#                     await cls.flush_batch()
#                     last_flush = now

#         except asyncio.CancelledError:
#             await cls.flush_batch()
#             await Telegram.send_message(f"[Ticker.db_writer.stopped/Cancelled]")
#             print("[Ticker.db_writer.stopped]")

#     # -----------------------------------------
#     # WebSocket Listener
#     # -----------------------------------------
#     @classmethod
#     async def run_ws(cls):

#         AUTH_TOKEN = await TokenRepository.get_token(Developer.ANKIT)
#         headers = {"Authorization": f"Bearer {AUTH_TOKEN}"}

#         stocks = await Collections.stocks.find(
#             {}, {"_id": 0, "instrument_key": 1, "symbol": 1}
#         )

#         stock_map = {doc["instrument_key"]: doc["symbol"] for doc in stocks}

#         instrument_keys = list(stock_map.keys())

#         MARKET_START = datetime.time(9, 15)
#         MARKET_END = datetime.time(15, 30)

#         today = ISDateTime.now().date()

#         # 🔥 Reusable protobuf object
#         feed_response = pb.FeedResponse()

#         # 🔥 Local bindings (faster lookup)
#         write_queue = cls.write_queue
#         put_nowait = write_queue.put_nowait
#         queue_full = asyncio.QueueFull
#         fromtimestamp = ISDateTime.fromtimestamp
#         TypeLive = pb.Type.live_feed
#         TypeInit = pb.Type.initial_feed

#         async with websockets.connect(
#             cls.URL,
#             additional_headers=headers,
#             ping_interval=20,
#             ping_timeout=10,
#         ) as ws:

#             await ws.send(
#                 json.dumps(
#                     {
#                         "guid": str(uuid.uuid4()),
#                         "method": "sub",
#                         "data": {
#                             "mode": "full",
#                             "instrumentKeys": instrument_keys,
#                         },
#                     }
#                 ).encode()
#             )

#             while True:

#                 message = await ws.recv()

#                 # Ignore text frames
#                 if not isinstance(message, bytes):
#                     continue

#                 # 🔥 Reuse object (no re-allocation)
#                 feed_response.ParseFromString(message)

#                 t = feed_response.type
#                 if t != TypeLive and t != TypeInit:
#                     continue

#                 feeds = feed_response.feeds

#                 for instrument_key, feed in feeds.items():

#                     if not feed.HasField("fullFeed"):
#                         continue

#                     full_feed = feed.fullFeed

#                     if not full_feed.HasField("marketFF"):
#                         continue

#                     market_ff = full_feed.marketFF

#                     if not market_ff.HasField("ltpc"):
#                         continue

#                     ltpc = market_ff.ltpc
#                     ltt = ltpc.ltt

#                     if not ltt:
#                         continue

#                     timestamp = fromtimestamp(ltt)

#                     # Market time filter
#                     ts_time = timestamp.time()
#                     if timestamp.date() != today or not (
#                         MARKET_START <= ts_time <= MARKET_END
#                     ):
#                         continue

#                     try:
#                         put_nowait(
#                             {
#                                 "timestamp": timestamp,
#                                 "instrument_key": instrument_key,
#                                 "_id": stock_map[instrument_key],
#                                 "ltp": ltpc.ltp,
#                                 "vtt": market_ff.vtt,
#                                 "oi": market_ff.oi,
#                             }
#                         )
#                     except queue_full:
#                         # ⚠ Avoid await inside hot path if possible
#                         # Consider rate limiting telegram alerts
#                         pass

    # @classmethod
    # async def run_ws(cls):

    #     AUTH_TOKEN = await TokenRepository.get_token(Developer.ANKIT)
    #     headers = {"Authorization": f"Bearer {AUTH_TOKEN}"}

    #     stocks = await Collections.stocks.find(
    #         {}, {"_id": 0, "instrument_key": 1, "symbol": 1}
    #     )

    #     stock_instruments_and_symbol: Dict[str, str] = {
    #         doc["instrument_key"]: doc["symbol"] for doc in stocks
    #     }

    #     STOCK_INSTRUMENTS = list(stock_instruments_and_symbol.keys())

    #     instrumentKeys = STOCK_INSTRUMENTS

    #     MARKET_START = datetime.time(9, 15, 0, 0)
    #     MARKET_END = datetime.time(15, 30, 0, 0)

    #     today = ISDateTime.now().date()

    #     async with websockets.connect(
    #         cls.URL,
    #         additional_headers=headers,
    #         ping_interval=20,
    #         ping_timeout=10,
    #     ) as ws:

    #         payload = {
    #             "guid": str(uuid.uuid4()),
    #             "method": "sub",
    #             "data": {
    #                 "mode": "full",
    #                 "instrumentKeys": instrumentKeys,
    #             },
    #         }

    #         await Telegram.send_message(
    #             f"Upstox WS connected and subscribing... date : {today.isoformat()}"
    #         )

    #         await ws.send(json.dumps(payload).encode())

    #         await Telegram.send_message(
    #             "Upstox WS subscription sent for instruments. {}".format(
    #                 len(instrumentKeys)
    #             )
    #         )

    #         while True:
    #             message = await ws.recv()

    #             if isinstance(message, bytes):
    #                 data = cls.decode_protobuf(message)

    #                 if data.get("type") not in (
    #                     "initial_feed",
    #                     "live_feed",
    #                 ):
    #                     continue

    #                 feeds = data.get("feeds", {})

    #                 # currentTs = IndianDateTime.fromtimestamp(data.get("currentTs", ""))

    #                 for instrument_key, feed in feeds.items():
    #                     mf: dict = feed.get("fullFeed", {}).get("marketFF", {})
    #                     if not mf:
    #                         continue

    #                     ltpc = mf.get("ltpc", {})
    #                     if not ltpc:
    #                         continue

    #                     ltt = ltpc.get("ltt", 0)
    #                     if not ltt:
    #                         continue

    #                     timestamp = ISDateTime.fromtimestamp(ltt)

    #                     if timestamp.date() != today or not (
    #                         MARKET_START <= timestamp.time() <= MARKET_END
    #                     ):
    #                         continue

    #                     tick_doc = {
    #                         "timestamp": timestamp,
    #                         "instrument_key": instrument_key,
    #                         "_id": stock_instruments_and_symbol[instrument_key],
    #                         "ltp": ltpc.get("ltp", 0),
    #                         "vtt": int(mf.get("vtt", 0)),
    #                     }

    #                     try:
    #                         cls.write_queue.put_nowait(tick_doc)
    #                     except asyncio.QueueFull:
    #                         await Telegram.send_message(
    #                             f"Upstox WS queue is full {cls.write_queue.qsize()}"
    #                         )

    # @classmethod
    # async def start(cls):

    #     if cls.run_ws_task and not cls.run_ws_task.done():
    #         await Telegram.send_message("Ticker is already running.")
    #         return

    #     retry = 0
    #     base_backoff = 5

    #     while retry < cls.retry_count:
    #         backoff = base_backoff * (2**retry)

    #         try:
    #             log.info("Starting Ticker services...")

    #             # Start DB writer if not running
    #             if not cls.db_writer_task or cls.db_writer_task.done():
    #                 cls.db_writer_task = asyncio.create_task(cls.db_writer())

    #             # Start WebSocket
    #             cls.run_ws_task = asyncio.create_task(cls.run_ws())

    #             # Wait until one of them crashes
    #             done, pending = await asyncio.wait(
    #                 [cls.run_ws_task, cls.db_writer_task],
    #                 return_when=asyncio.FIRST_EXCEPTION,
    #             )

    #             # If any task failed → raise its exception
    #             for task in done:
    #                 task.result()

    #             for task in pending:
    #                 task.cancel()
    #                 try:
    #                     await task
    #                 except asyncio.CancelledError:
    #                     pass

    #         except Exception as e:
    #             retry += 1
    #             log.error(f"Ticker crashed: {e}")

    #             await Telegram.send_message(
    #                 f"[Ticker] Crash detected\nError: {e}\nRetry {retry}/{cls.retry_count}"
    #             )

    #             # Cancel running tasks
    #             for task in [cls.run_ws_task, cls.db_writer_task]:
    #                 if task and not task.done():
    #                     task.cancel()
    #                     try:
    #                         await task
    #                     except asyncio.CancelledError:
    #                         pass

    #             await Telegram.send_message(
    #                 f"[Ticker] Reconnecting in {backoff} seconds..."
    #             )

    #             await asyncio.sleep(backoff)

    #     cls.run_ws_task = None
    #     cls.db_writer_task = None
    #     await Telegram.send_message("[Ticker] Max retries reached. Stopping Ticker.")

    #     log.error("Ticker stopped after max retries.")

    # @classmethod
    # async def stop(cls):
    #     ws_task = cls.run_ws_task
    #     writer_task = cls.db_writer_task

    #     if ws_task is None and writer_task is None:
    #         await Telegram.send_message("Ticker is not running.")
    #         return

    #     if ws_task is not None:
    #         if not ws_task.done():
    #             ws_task.cancel()
    #             try:
    #                 await ws_task
    #             except asyncio.CancelledError:
    #                 pass

    #     if writer_task is not None:
    #         if not writer_task.done():
    #             writer_task.cancel()
    #             try:
    #                 await writer_task
    #             except asyncio.CancelledError:
    #                 pass

    #     cls.run_ws_task = None
    #     cls.db_writer_task = None

    #     await Telegram.send_message("Ticker stopped.")

    # @classmethod
    # async def status(cls):
    #     if cls.run_ws_task and not cls.run_ws_task.done():
    #         await Telegram.send_message("Ticker is running.")
    #     else:
    #         await Telegram.send_message("Ticker is not running.")


# import asyncio
# from asyncio import Task
# import datetime
# import json
# import uuid
# from typing import Dict, Any, List

# import websockets
# import clickhouse_connect
# from google.protobuf.json_format import MessageToDict

# import server.modules.ticker.marketfeed_pb2 as pb
# from server.modules.telegram.telegram import Telegram
# from server.modules.token.enums import Developer
# from server.modules.token.repository import TokenRepository
# from server.db.collections import Collections
# from server.utils.is_dt import ISDateTime
# from server.utils.logger import log


# class Ticker:

#     run_ws_task: Task | None = None
#     worker_tasks: List[Task] = []

#     retry_count = 3
#     URL = "wss://api.upstox.com/v3/feed/market-data-feed"

#     # Queue & batching
#     write_queue: asyncio.Queue = asyncio.Queue(maxsize=100_000)
#     BATCH_SIZE = 4000
#     WORKERS = 1

#     # -----------------------------------------
#     # Decode Protobuf
#     # -----------------------------------------
#     obj = pb.FeedResponse()

#     @classmethod
#     def decode_protobuf(cls, buffer: bytes) -> Dict[str, Any]:
#         cls.obj.ParseFromString(buffer)
#         return MessageToDict(cls.obj)

#     # -----------------------------------------
#     # ClickHouse Worker
#     # -----------------------------------------
#     @classmethod
#     async def clickhouse_worker(cls):

#         client = clickhouse_connect.get_client(
#             host="localhost",
#             username="default",
#             password="click1234",
#             database="market",
#         )

#         batch: List[tuple] = []
#         last_flush = asyncio.get_running_loop().time()
#         FLUSH_INTERVAL = 10  # seconds

#         while True:
#             try:
#                 try:
#                     item = await asyncio.wait_for(cls.write_queue.get(), timeout=1)
#                     batch.append(item)
#                     cls.write_queue.task_done()
#                 except asyncio.TimeoutError:
#                     pass

#                 now = asyncio.get_running_loop().time()

#                 # Flush if batch size reached
#                 if len(batch) >= cls.BATCH_SIZE:
#                     client.insert(
#                         "ticks",
#                         batch,
#                         column_names=["symbol", "ts", "ltp", "vtt"],
#                     )
#                     batch.clear()
#                     last_flush = now

#                 # Flush if time interval exceeded
#                 elif batch and (now - last_flush >= FLUSH_INTERVAL):
#                     client.insert(
#                         "ticks",
#                         batch,
#                         column_names=["symbol", "ts", "ltp", "vtt"],
#                     )
#                     batch.clear()
#                     last_flush = now

#             except asyncio.CancelledError:
#                 if batch:
#                     client.insert(
#                         "ticks",
#                         batch,
#                         column_names=["symbol", "ts", "ltp", "vtt"],
#                     )
#                 await Telegram.send_message(f"[Ticker.clickhouse_worker.stopped/Cancelled]")
#                 print("[Ticker.clickhouse_worker.stopped]")
#                 break

#             except Exception as e:
#                 log.error(f"ClickHouse worker error: {e}")
#                 await Telegram.send_message(f"[CH Worker Error]: {e}")

#     # -----------------------------------------
#     # WebSocket Listener
#     # -----------------------------------------
#     @classmethod
#     async def run_ws(cls):

#         AUTH_TOKEN = await TokenRepository.get_token(Developer.ANKIT)
#         headers = {"Authorization": f"Bearer {AUTH_TOKEN}"}

#         stocks = await Collections.stocks.find(
#             {}, {"_id": 0, "instrument_key": 1, "symbol": 1}
#         )

#         instrument_to_symbol: Dict[str, str] = {
#             doc["instrument_key"]: doc["symbol"] for doc in stocks
#         }

#         instrumentKeys = list(instrument_to_symbol.keys())

#         MARKET_START = datetime.time(9, 15)
#         MARKET_END = datetime.time(15, 30)

#         today = ISDateTime.now().date()

#         async with websockets.connect(
#             cls.URL,
#             additional_headers=headers,
#             ping_interval=20,
#             ping_timeout=10,
#         ) as ws:

#             payload = {
#                 "guid": str(uuid.uuid4()),
#                 "method": "sub",
#                 "data": {
#                     "mode": "full",
#                     "instrumentKeys": instrumentKeys,
#                 },
#             }

#             await ws.send(json.dumps(payload).encode())

#             await Telegram.send_message(
#                 f"Upstox WS connected. {len(instrumentKeys)} instruments subscribed."
#             )

#             while True:
#                 message = await ws.recv()

#                 if not isinstance(message, bytes):
#                     continue

#                 data = cls.decode_protobuf(message)

#                 if data.get("type") not in ("initial_feed", "live_feed"):
#                     continue

#                 feeds = data.get("feeds", {})

#                 for instrument_key, feed in feeds.items():

#                     mf = feed.get("fullFeed", {}).get("marketFF", {})
#                     if not mf:
#                         continue

#                     ltpc = mf.get("ltpc", {})
#                     ltt = ltpc.get("ltt")
#                     if not ltt:
#                         continue

#                     timestamp = ISDateTime.fromtimestamp(ltt)

#                     if timestamp.date() != today or not (
#                         MARKET_START <= timestamp.time() <= MARKET_END
#                     ):
#                         continue

#                     symbol = instrument_to_symbol.get(instrument_key)
#                     if not symbol:
#                         continue

#                     try:
#                         cls.write_queue.put_nowait(
#                             (
#                                 symbol,
#                                 timestamp,
#                                 float(ltpc.get("ltp", 0)),
#                                 int(mf.get("vtt", 0)),
#                             )
#                         )
#                     except asyncio.QueueFull:
#                         log.warning("Write queue full!")
                        
#     @classmethod
#     async def _run_supervisor(cls):

#         retry = 0
#         base_backoff = 5

#         while retry < cls.retry_count:
#             try:
#                 await cls.run_ws()
#                 retry = 0  # reset if success
#             except Exception as e:
#                 retry += 1
#                 backoff = base_backoff * (2 ** retry)

#                 log.error(f"WS crashed: {e}")

#                 await Telegram.send_message(
#                     f"[Ticker WS Crash]\n{e}\nRetry {retry}/{cls.retry_count}"
#                 )

#                 await asyncio.sleep(backoff)

#         await Telegram.send_message("Max WS retries reached.")

#     # -----------------------------------------
#     # Start
#     # -----------------------------------------
#     @classmethod
#     async def start(cls):

#         if cls.run_ws_task and not cls.run_ws_task.done():
#             await Telegram.send_message("Ticker already running.")
#             return

#         # Start workers
#         for _ in range(cls.WORKERS):
#             task = asyncio.create_task(cls.clickhouse_worker())
#             cls.worker_tasks.append(task)

#         cls.run_ws_task = asyncio.create_task(cls._run_supervisor())

#         await Telegram.send_message("Ticker started with async CH pool.")

#     # -----------------------------------------
#     # Stop
#     # -----------------------------------------
#     @classmethod
#     async def stop(cls):

#         if cls.run_ws_task:
#             cls.run_ws_task.cancel()
#             try:
#                 await cls.run_ws_task
#             except asyncio.CancelledError:
#                 pass

#         for task in cls.worker_tasks:
#             task.cancel()
#             try:
#                 await task
#             except asyncio.CancelledError:
#                 pass

#         cls.worker_tasks.clear()

#         await Telegram.send_message("Ticker stopped cleanly.")

#     # -----------------------------------------
#     # Status
#     # -----------------------------------------
#     @classmethod
#     async def status(cls):
#         if cls.run_ws_task and not cls.run_ws_task.done():
#             await Telegram.send_message("Ticker running.")
#         else:
#             await Telegram.send_message("Ticker stopped.")

