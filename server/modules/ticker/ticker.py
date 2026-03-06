import asyncio
from asyncio import Task
import datetime
import json
import uuid
from typing import Dict

import websockets

import server.modules.ticker.marketfeed_pb2 as pb
from server.modules.telegram.telegram import Telegram
from server.modules.ticker.ohlc_ticker import OhlcModel, OhlcTicker
from server.modules.ticker.volume_ticker import VolumeTicker
from server.modules.token.enums import Developer
from server.modules.token.repository import TokenRepository
from server.db.collections import Collections
from server.utils.is_dt import ISDateTime
from server.utils.logger import log


class Ticker:
    volume_ticker: VolumeTicker
    ohlc_ticker: OhlcTicker
    run_ws_task: Task | None = None

    retry_count = 3
    URL = "wss://api.upstox.com/v3/feed/market-data-feed"

    @staticmethod
    def extract_i1_ohlc(market_ff) -> OhlcModel | None:

        if not market_ff.HasField("marketOHLC"):
            return None

        for candle in market_ff.marketOHLC.ohlc:

            if candle.interval == "I1":

                return OhlcModel(
                    ts=ISDateTime.fromtimestamp(candle.ts),
                    open=candle.open,
                    high=candle.high,
                    low=candle.low,
                    close=candle.close,
                    volume=candle.vol or 0,
                    oi=candle.oi or 0,
                )

        return None

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

        # instrument_to_symbol: Dict[str, str] = {
        #     doc["instrument_key"]: doc["symbol"] for doc in stocks
        # }
        instrument_to_symbol: Dict[str, str] = {"MCX_FO|454818": "GOLD FUT 02 APR 26"}

        instrumentKeys = list(instrument_to_symbol.keys())

        MARKET_START = datetime.time(9, 15)
        MARKET_END = datetime.time(22, 30)

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

                obj.ParseFromString(message)
                
                

                if obj.type != INITIAL and obj.type != LIVE:
                    continue

                # feeds = data.get("feeds", {})
                
                print(f"obj {obj}")
                

                for instrument_key, feed in obj.feeds.items():
                    
                    print(f"instrument_key {instrument_key} feed {feed}")
                    

                    if not feed.HasField("fullFeed"):
                        continue

                    full_feed = feed.fullFeed
                    
                    print(full_feed)

                    if not full_feed.HasField("marketFF"):
                        continue

                    mf = full_feed.marketFF

                    if not mf.HasField("ltpc"):
                        continue

                    ltpc = mf.ltpc
                    ltt = ltpc.ltt

                    timestamp = ISDateTime.fromtimestamp(ltt)

                    if timestamp.date() != today or not (
                        MARKET_START <= timestamp.time() <= MARKET_END
                    ):
                        continue

                    ltp = ltpc.ltp
                    vvt = mf.vtt

                    symbol = instrument_to_symbol.get(instrument_key)
                    if not symbol:
                        continue

                    await cls.volume_ticker.process_tick(
                        symbol=symbol,
                        ltp=ltp,
                        ltt=timestamp,
                        vtt=vvt,
                    )
                    candle = cls.extract_i1_ohlc(mf)
                    if candle:
                        await cls.ohlc_ticker.process_ohlc(symbol=symbol, candle=candle)

    @classmethod
    async def _run_supervisor(cls):

        retry = 0
        base_backoff = 5

        while retry < cls.retry_count:
            try:
                await cls.run_ws()
                retry = 0  # reset if success
            except Exception as e:
                retry += 1
                backoff = base_backoff * (2**retry)

                log.error(f"WS crashed: {e}")

                await Telegram.send_message(
                    f"[Ticker WS Crash]\n{e}\nRetry {retry}/{cls.retry_count}"
                )

                await asyncio.sleep(backoff)

        await Telegram.send_message("Max WS retries reached.")

    # -----------------------------------------
    # Start
    # -----------------------------------------
    @classmethod
    async def start(cls):

        if cls.run_ws_task and not cls.run_ws_task.done():
            await Telegram.send_message("Ticker already running.")
            return

        cls.volume_ticker = VolumeTicker()
        cls.ohlc_ticker = OhlcTicker()
        cls.run_ws_task = asyncio.create_task(cls._run_supervisor())

        await Telegram.send_message("Ticker started")

    # -----------------------------------------
    # Stop
    # -----------------------------------------
    @classmethod
    async def stop(cls):

        if cls.run_ws_task:
            cls.run_ws_task.cancel()
            try:
                await cls.run_ws_task
            except asyncio.CancelledError:
                pass
        if cls.volume_ticker:
            await cls.volume_ticker.dispose()
        if cls.ohlc_ticker:
            await cls.ohlc_ticker.dispose()

        await Telegram.send_message("Ticker stopped cleanly.")

    # -----------------------------------------
    # Status
    # -----------------------------------------
    @classmethod
    async def status(cls):
        if cls.run_ws_task and not cls.run_ws_task.done():
            await Telegram.send_message("Ticker running.")
        else:
            await Telegram.send_message("Ticker stopped.")
