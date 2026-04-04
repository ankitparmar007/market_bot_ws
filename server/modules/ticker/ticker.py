import asyncio
from asyncio import Task
from datetime import time
import json
import uuid
from typing import Dict

import websockets

from server.db.collections import Collections
import server.modules.ticker.marketfeed_pb2 as pb
from server.modules.telegram.telegram import Telegram
# from server.modules.ticker.ohlc_ticker import OhlcModel, OhlcTicker
from server.modules.ticker.volume_ticker_clickhouse import VolumeTicker
from server.modules.token.enums import Developer
from server.modules.token.repository import TokenRepository
from server.utils.is_dt import ISDateTime
from server.utils.logger import log


class Ticker:
    volume_ticker: VolumeTicker
    # ohlc_ticker: OhlcTicker
    ws_task: Task | None = None

    retry_count = 3
    URL = "wss://api.upstox.com/v3/feed/market-data-feed"

    # @staticmethod
    # def extract_i1_ohlc_market(
    #     market_ff: pb.MarketFullFeed, symbol: str
    # ) -> OhlcModel | None:

    #     ohlc_list = market_ff.marketOHLC.ohlc

    #     for candle in ohlc_list:
    #         if candle.interval == "I1":
    #             return OhlcModel(
    #                 symbol=symbol,
    #                 timestamp=ISDateTime.from_timestamp(candle.ts),
    #                 open=candle.open,
    #                 high=candle.high,
    #                 low=candle.low,
    #                 close=candle.close,
    #                 volume=candle.vol,
    #                 # oi=oi,
    #             )

    #     return None

    # @staticmethod
    # def extract_i1_ohlc_index(
    #     index_ff: pb.IndexFullFeed, symbol: str
    # ) -> OhlcModel | None:

    #     ohlc_list = index_ff.marketOHLC.ohlc

    #     for candle in ohlc_list:
    #         if candle.interval == "I1":
    #             return OhlcModel(
    #                 symbol=symbol,
    #                 timestamp=ISDateTime.from_timestamp(candle.ts),
    #                 open=candle.open,
    #                 high=candle.high,
    #                 low=candle.low,
    #                 close=candle.close,
    #                 volume=candle.vol,
    #                 # oi=oi,
    #             )

    #     return None

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
        # indices = await Collections.indices.find(
        #     {}, {"_id": 0, "instrument_key": 1, "symbol": 1}
        # )

        instrument_to_symbol: Dict[str, str] = {
            doc["instrument_key"]: doc["symbol"] for doc in stocks
        }

        # instrument_to_symbol.update(
        #     {doc["instrument_key"]: doc["symbol"] for doc in indices}
        # )

        # instrument_to_symbol: Dict[str, str] = {
        #     "NSE_INDEX|Nifty 50": "Nifty50",
        #     "NSE_EQ|INE114A01011": "SAIL",
        # }

        instrumentKeys = list(instrument_to_symbol.keys())

        # https://www.nseindia.com/static/market-data/market-timings
        # Block Deal session 1 Open 	        08:45 hrs -- Start
        # Trade Modification cut-off time * 	16:15 hrs -- End
        MARKET_START = time(9, 15)
        MARKET_END = time(16, 00)

        today = ISDateTime.now().date()

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

            await Telegram.send_message(
                f"Upstox WS connected. {len(instrumentKeys)} instruments subscribed."
            )

            obj = pb.FeedResponse()

            INITIAL = pb.Type.initial_feed
            LIVE = pb.Type.live_feed

            while True:
                message = await ws.recv()

                if not isinstance(message, bytes):
                    continue

                obj.Clear()

                try:
                    obj.ParseFromString(message)
                except Exception:
                    log.warning(f"obj.ParseFromString failed")
                    continue

                if obj.type != INITIAL and obj.type != LIVE:
                    continue

                feeds = obj.feeds

                for instrument_key, feed in feeds.items():

                    symbol = instrument_to_symbol.get(instrument_key)
                    if symbol is None:
                        continue

                    if feed.WhichOneof("FeedUnion") != "fullFeed":
                        continue

                    full_feed = feed.fullFeed

                    feed_type = full_feed.WhichOneof("FullFeedUnion")

                    if feed_type == "marketFF":
                        marketFF = full_feed.marketFF
                        ltpc = marketFF.ltpc
                        ltt = ltpc.ltt

                        if ltt == 0:
                            continue

                        timestamp = ISDateTime.from_timestamp(ltt)

                        if timestamp.date() != today or not (
                            MARKET_START <= timestamp.time() <= MARKET_END
                        ):
                            continue

                        ltp = ltpc.ltp
                        vtt = marketFF.vtt
                        # oi = int(marketFF.oi)

                        # volume ticker
                        cls.volume_ticker.process_tick(
                            symbol=symbol,
                            ltp=ltp,
                            ltt=timestamp,
                            vtt=vtt,
                        )
                        
                        # ohlc ticker
                        # candle = cls.extract_i1_ohlc_market(marketFF, symbol=symbol)
                        # if candle:
                        #     cls.ohlc_ticker.process_ohlc(candle=candle)

                    # elif feed_type == "indexFF":
                    #     indexFF = full_feed.indexFF
                    #     candle = cls.extract_i1_ohlc_index(indexFF, symbol=symbol)
                    #     if candle:
                    #         cls.ohlc_ticker.process_ohlc(candle=candle)

    @classmethod
    async def _run_supervisor(cls):

        retry = 0
        base_backoff = 1

        while retry < cls.retry_count:
            try:
                await cls.run_ws()
                retry = 0  # reset if success
            except Exception as e:
                retry += 1
                backoff = base_backoff * (2**retry)

                log.error(f"WS crashed: {e}")

                await Telegram.send_message(
                    f"[Ticker WS Crash]\n{e}\nRetry {retry}/{cls.retry_count} in {backoff} seconds..."
                )

                await asyncio.sleep(backoff)

        await Telegram.send_message("Max WS retries reached. Stopping ticker... Please start manually after fixing the issue.")

        await cls.stop()

    # -----------------------------------------
    # Start
    # -----------------------------------------
    @classmethod
    async def start(cls):

        if cls.ws_task and not cls.ws_task.done():
            await Telegram.send_message("Ticker already running.")
            return

        cls.volume_ticker = VolumeTicker()
        # cls.ohlc_ticker = OhlcTicker()
        cls.ws_task = asyncio.create_task(cls._run_supervisor())

        await Telegram.send_message("Ticker started")

    # -----------------------------------------
    # Stop
    # -----------------------------------------
    @classmethod
    async def stop(cls):

        if cls.ws_task:
            cls.ws_task.cancel()
            try:
                await cls.ws_task
            except asyncio.CancelledError:
                pass
            await cls.volume_ticker.dispose()
            # await cls.ohlc_ticker.dispose()
            cls.ws_task = None
            await Telegram.send_message("Ticker stopped cleanly.")
        else:
            await Telegram.send_message("Ticker is already stopped.")

    # -----------------------------------------
    # Status
    # -----------------------------------------
    @classmethod
    async def status(cls):
        if cls.ws_task and not cls.ws_task.done():
            await Telegram.send_message("Ticker running.")
        else:
            await Telegram.send_message("Ticker not running.")
