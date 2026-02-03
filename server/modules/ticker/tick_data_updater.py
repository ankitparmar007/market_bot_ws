import asyncio
from typing import Dict, Any
import websockets
import json
import uuid
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
    async def safe(cls, call):
        try:
            await call
        except Exception as e:
            log.error(e)

    @classmethod
    async def run_ws(cls):
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
        OhlcTicker.generate_state_for_instruments(stock_instruments_and_symbol)
        OhlcTicker.generate_state_for_instruments(index_instruments_and_symbol)

        instrumentKeys = STOCK_INSTRUMENTS + INDEX_INSTRUMENTS

        # stock_instruments_and_symbol: Dict[str, str] = {
        #     "MCX_FO|458305": "SILVERMIC FUT 27 FEB 26",
        #     "MCX_FO|467013": "CRUDEOIL FUT 19 FEB 26",
        # }

        # STOCK_INSTRUMENTS = list(stock_instruments_and_symbol.keys())

        # instrumentKeys = STOCK_INSTRUMENTS

        # VolumeTicker.generate_state_for_instruments(stock_instruments_and_symbol)
        # OhlcTicker.generate_state_for_instruments(stock_instruments_and_symbol)

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

            await Telegram.send_message("Upstox WS connected and subscribing...")

            await ws.send(json.dumps(payload).encode("utf-8"))
            log.info(f"Subscription sent for instruments: {len(instrumentKeys)}")

            await Telegram.send_message(
                "Upstox WS subscription sent for instruments. {}".format(
                    len(instrumentKeys)
                )
            )

            while True:
                msg = await ws.recv()

                if isinstance(msg, bytes):
                    data = cls.decode_protobuf(msg)
                    if data.get("type") in ("initial_feed", "live_feed"):
                        await cls.safe(VolumeTicker.handle_feed(data))
                        await cls.safe(OhlcTicker.handle_feed(data))
                        # await cls.handle_feed(data)
                else:
                    log.debug(f"Text msg: {msg}")
