import asyncio
from asyncio import Task
from typing import Dict, Any
import websockets
import json
import uuid
from server.api.exceptions import AppException
from server.modules.ticker.ohlc_ticker import OhlcTicker
from server.modules.ticker.volume_ticker import VolumeTicker
from server.modules.token.enums import Developer
from server.modules.token.repository import TokenRepository
from server.utils.logger import log
from google.protobuf.json_format import MessageToDict
from server.db.collections import Collections
import server.modules.ticker.marketfeed_pb2 as pb


from server.modules.telegram.telegram import Telegram


class Ticker:

    # ==========================================================
    # CONFIG
    # ==========================================================

    URL = "wss://api.upstox.com/v3/feed/market-data-feed"

    ticker_task: Task | None = None
    ohlc_ticker_write_task: Task | None = None
    volume_ticker_write_task: Task | None = None

    retry_count = 3

    # ==========================================================
    # PROTOBUF DECODE
    # ==========================================================

    @classmethod
    def decode_protobuf(cls, buffer: bytes) -> Dict[str, Any]:
        obj = pb.FeedResponse()  # type: ignore
        obj.ParseFromString(buffer)
        return MessageToDict(obj)

    # ==========================================================
    # WS LOGIC
    # ==========================================================
    @classmethod
    async def safe(cls, call, message: str):
        try:
            await call
        except Exception as e:
            log.error("[Ticker.safe]: " + message + ": " + str(e))
            # await Telegram.send_message("[Ticker.safe]: " + message + ": " + str(e))

    @classmethod
    async def ws(cls):

        AUTH_TOKEN = await TokenRepository.get_token(Developer.ANKIT)
        HEADERS = {"Authorization": f"Bearer {AUTH_TOKEN}"}

        stocks = await Collections.stocks.find(
            {}, {"_id": 0, "instrument_key": 1, "symbol": 1}
        )

        indices = await Collections.indices.find(
            {}, {"_id": 0, "instrument_key": 1, "symbol": 1, "has_option": True}
        )

        stock_instruments_and_symbol: Dict[str, str] = {
            doc["instrument_key"]: doc["symbol"] for doc in stocks
        }

        index_instruments_and_symbol: Dict[str, str] = {
            doc["instrument_key"]: doc["symbol"] for doc in indices
        }

        STOCK_INSTRUMENTS = list(stock_instruments_and_symbol.keys())
        INDEX_INSTRUMENTS = list(index_instruments_and_symbol.keys())

        VolumeTicker.generate_state_for_instruments(stock_instruments_and_symbol)
        OhlcTicker.generate_state_for_instruments(
            {
                **stock_instruments_and_symbol,
                **index_instruments_and_symbol,
            }
        )

        instrumentKeys = STOCK_INSTRUMENTS + INDEX_INSTRUMENTS

        # stock_instruments_and_symbol: Dict[str, str] = {
        #     "MCX_FO|458305": "SILVERMIC FUT 27 FEB 26",
        #     "MCX_FO|467013": "CRUDEOIL FUT 19 FEB 26",
        # }

        # STOCK_INSTRUMENTS = list(stock_instruments_and_symbol.keys())

        # instrumentKeys = STOCK_INSTRUMENTS

        # VolumeTicker.generate_state_for_instruments(stock_instruments_and_symbol)
        # OhlcTicker.generate_state_for_instruments(stock_instruments_and_symbol)

        while True:
            try:
                log.info("Connecting to Upstox feed...")

                async with websockets.connect(
                    cls.URL,
                    additional_headers=HEADERS,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    payload = {
                        "guid": str(uuid.uuid4()),
                        "method": "sub",
                        "data": {"mode": "full", "instrumentKeys": instrumentKeys},
                    }

                    await ws.send(json.dumps(payload).encode("utf-8"))
                    msg = f"Subscription sent for instruments: {len(instrumentKeys)}"
                    log.info(msg)

                    await Telegram.send_message(msg)

                    while True:
                        msg = await ws.recv()

                        if isinstance(msg, bytes):
                            data = cls.decode_protobuf(msg)
                            if data.get("type") in ("initial_feed", "live_feed"):
                                feeds = data.get("feeds", {})
                                for instrument_key, feed in feeds.items():
                                    ff = feed.get("fullFeed", {})
                                    if not ff:
                                        continue

                                    market_ff = ff.get("marketFF", {})
                                    index_ff = ff.get("indexFF", {})

                                    if market_ff:
                                        await asyncio.gather(
                                            cls.safe(
                                                VolumeTicker.handle_feed(
                                                    instrument_key,
                                                    market_ff,
                                                ),
                                                message="VolumeTicker.market_ff",
                                            ),
                                            cls.safe(
                                                OhlcTicker.handle_feed(
                                                    instrument_key, market_ff
                                                ),
                                                message="OhlcTicker.market_ff",
                                            ),
                                        )

                                    if index_ff:
                                        await cls.safe(
                                            OhlcTicker.handle_feed(
                                                instrument_key=instrument_key,
                                                market_or_index_ff=index_ff,
                                            ),
                                            message="OhlcTicker.index_ff",
                                        )

            except AppException as e:
                msg = f"Ticker error {e.message} recconnecting in 5 seconds... retry left {cls.retry_count}"
                log.warning(msg)
                await Telegram.send_message(msg)
                await asyncio.sleep(5)
                cls.retry_count -= 1
                if cls.retry_count == 0:
                    await Telegram.send_message(
                        "Ticker max retries reached. Exiting..."
                    )
                    cls.ticker_task.done()
                    if (
                        cls.ohlc_ticker_write_task
                        and not cls.ohlc_ticker_write_task.done()
                    ):
                        cls.ohlc_ticker_write_task.cancel()
                        cls.ohlc_ticker_write_task = None
                    if (
                        cls.volume_ticker_write_task
                        and not cls.volume_ticker_write_task.done()
                    ):
                        cls.volume_ticker_write_task.cancel()
                        cls.volume_ticker_write_task = None
                    break

    @classmethod
    async def start(cls):
        if cls.ticker_task and not cls.ticker_task.done():
            await Telegram.send_message("Ticker is running.")
        else:
            await Telegram.send_message("Starting Ticker...")
            cls.volume_ticker_write_task = asyncio.create_task(VolumeTicker.db_writer())
            cls.ohlc_ticker_write_task = asyncio.create_task(OhlcTicker.db_writer())
            cls.ticker_task = asyncio.create_task(cls.ws())
