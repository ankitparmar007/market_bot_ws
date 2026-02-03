from asyncio import Queue
import asyncio
from typing import Dict, Any

from server.db.collections import Collections
from server.utils.ist import IndianDateTime
from server.utils.logger import log


# ==========================================================
# JSON FILE CONFIG
# ==========================================================

# JSON_FILE = "market_ohlc_i1.json"
# _json_lock = Lock()


# ==========================================================
# TICKER
# ==========================================================


class OhlcTicker:

    # per-instrument runtime state
    instruments_and_symbols_state: Dict[str, Dict[str, Any]] = {}

    # @classmethod
    # def append_ohlc_to_json(cls, row: dict):
    #     with _json_lock:
    #         if os.path.exists(JSON_FILE):
    #             with open(JSON_FILE, "r", encoding="utf-8") as f:
    #                 data = json.load(f)
    #         else:
    #             data = []

    #         data.append(row)

    #         with open(JSON_FILE, "w", encoding="utf-8") as f:
    #             json.dump(data, f, ensure_ascii=False, indent=2)

    # ==========================================================
    # ASYNC DB WRITE QUEUE
    # ==========================================================

    write_queue: Queue[dict] = Queue()

    docs = []

    @classmethod
    async def flush_batch(cls):
        if not cls.docs:
            return

        try:

            await Collections.intraday_all_history.insert_many(cls.docs)

        except Exception as e:
            log.error(f"Batch insert failed: {e}, retrying...")

            for doc in cls.docs:
                await cls.write_queue.put(doc)

            await asyncio.sleep(1)
        cls.docs = []

    @classmethod
    async def db_writer(cls):
        log.info("[OhlcTicker.db_writer] started")

        BATCH_SIZE = 10
        try:
            while True:
                # Wait until a doc is available
                doc = await cls.write_queue.get()
                cls.docs.append(doc)
                cls.write_queue.task_done()

                # Flush when batch is full
                if len(cls.docs) >= BATCH_SIZE:
                    await cls.flush_batch()
        except asyncio.CancelledError:
            while not cls.write_queue.empty():
                doc = await cls.write_queue.get()
                cls.docs.append(doc)
                cls.write_queue.task_done()

            await cls.flush_batch()
            log.info("[OhlcTicker.db_writer] stopped flushed all docs")

    # ==========================================================
    # INIT STATE
    # ==========================================================

    @classmethod
    def generate_state_for_instruments(cls, instruments_and_symbols: Dict[str, str]):
        cls.instruments_and_symbols_state = {
            instrument: {
                "symbol": symbol,
                "last_ohlc_minute": None,  # to prevent duplicate writes
            }
            for instrument, symbol in instruments_and_symbols.items()
        }

    # ==========================================================
    # EXTRACT I1 OHLC (REFERENCE-BASED)
    # ==========================================================

    @staticmethod
    def extract_i1_ohlc(market_ff: dict) -> dict | None:
        ohlc_list = market_ff.get("marketOHLC", {}).get("ohlc", [])
        for candle in ohlc_list:
            if candle.get("interval") == "I1":
                return candle
        return None

    # ==========================================================
    # HANDLE FEED
    # ==========================================================

    @classmethod
    async def handle_feed(cls, data: Dict[str, Any]):
        feeds = data.get("feeds") or {}
        for instrument_key, feed in feeds.items():
            ff = feed.get("fullFeed", {})
            market_ff = ff.get("marketFF") or ff.get("indexFF") or {}
            if not market_ff:
                continue

            i1 = cls.extract_i1_ohlc(market_ff)
            if not i1:
                continue

            st = cls.instruments_and_symbols_state.get(instrument_key)
            if not st:
                continue

            symbol = st["symbol"]

            # Convert exchange ts → IST minute
            ts = IndianDateTime.fromtimestamp(i1["ts"])
            minute_ts = ts.replace(second=0, microsecond=0)

            # Prevent duplicate writes for same minute
            if st["last_ohlc_minute"] == minute_ts:
                continue

            st["last_ohlc_minute"] = minute_ts

            row = {
                "instrument_key": instrument_key,
                "symbol": symbol,
                "timestamp": minute_ts.isoformat(),
                "open": float(i1["open"]),
                "high": float(i1["high"]),
                "low": float(i1["low"]),
                "close": float(i1["close"]),
                "volume": int(i1.get("vol", 0)),
                # OI snapshot at candle close
                "oi": int(market_ff.get("oi", 0)),
            }

            await cls.write_queue.put(row)

            # cls.append_ohlc_to_json(row)

            # log.info(
            #     f"I1 saved | {symbol} | {minute_ts.strftime('%H:%M')} | "
            #     f"O:{row['open']} H:{row['high']} "
            #     f"L:{row['low']} C:{row['close']} "
            #     f"V:{row['volume']} OI:{row['oi']}"
            # )
