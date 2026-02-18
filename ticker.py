import asyncio
import websockets
import json
import uuid
from server.modules.token.enums import Developer
from server.modules.token.repository import TokenRepository
from server.utils.ist import IndianDateTime
from typing import Dict, Any, List
from google.protobuf.json_format import MessageToDict
from server.db.collections import Collections, TicksCollections
import server.modules.ticker.marketfeed_pb2 as pb
from server.db import mongodb_client, mongodb_ticks_client


class Ticker:

    write_queue: asyncio.Queue = asyncio.Queue(maxsize=50000)
    docs: List[dict] = []
    BATCH_SIZE = 2000
    FLUSH_INTERVAL = 5  # seconds
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
            print(f"Inserted batch: {len(cls.docs)}")
        except Exception as e:
            print("Batch insert failed:", e)

        cls.docs = []

    # ----------------- ------------------------
    # Database Writer
    # -----------------------------------------
    @classmethod
    async def db_writer(cls):
        print("[TickWriter] Started")

        last_flush = asyncio.get_event_loop().time()

        try:
            while True:
                try:
                    doc = await asyncio.wait_for(cls.write_queue.get(), timeout=0.5)
                    cls.docs.append(doc)
                    cls.write_queue.task_done()
                except asyncio.TimeoutError:
                    pass

                now = asyncio.get_event_loop().time()

                if len(cls.docs) >= cls.BATCH_SIZE or (
                    cls.docs and now - last_flush >= cls.FLUSH_INTERVAL
                ):
                    await cls.flush_batch()
                    last_flush = now

        except asyncio.CancelledError:
            await cls.flush_batch()
            print("[TickWriter] Stopped")

    # -----------------------------------------
    # WebSocket Listener
    # -----------------------------------------
    @classmethod
    async def ws(cls):

        AUTH_TOKEN = await TokenRepository.get_token(Developer.RACHIT)
        headers = {"Authorization": f"Bearer {AUTH_TOKEN}"}

        stocks = await Collections.stocks.find(
            {}, {"_id": 0, "instrument_key": 1, "symbol": 1}
        )

        stock_instruments_and_symbol: Dict[str, str] = {
            doc["instrument_key"]: doc["symbol"] for doc in stocks
        }

        STOCK_INSTRUMENTS = list(stock_instruments_and_symbol.keys())

        instrumentKeys = STOCK_INSTRUMENTS
        while True:
            try:
                print("Connecting to Upstox...")

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

                    await ws.send(json.dumps(payload).encode())
                    print("Subscribed for instruments:", len(instrumentKeys))

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

                            currentTs = IndianDateTime.fromtimestamp(
                                data.get("currentTs", "")
                            )

                            for instrument_key, feed in feeds.items():
                                ff = feed.get("fullFeed", {})
                                if not ff:
                                    continue

                                tick_doc = {
                                    "timestamp": currentTs,
                                    "instrument_key": instrument_key,
                                    "_id": stock_instruments_and_symbol[instrument_key],
                                    "fullFeed": ff,
                                }

                                await cls.write_queue.put(tick_doc)

            except Exception as e:
                print("WS Error:", e)
                print("Reconnecting in 5 sec...")
                await asyncio.sleep(5)


# ==========================================================
# MAIN
# ==========================================================


async def main():
    await mongodb_client.ensure_connection()
    await mongodb_ticks_client.ensure_connection()

    writer_task = asyncio.create_task(Ticker.db_writer())
    ws_task = asyncio.create_task(Ticker.ws())

    await asyncio.gather(writer_task, ws_task)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped")
