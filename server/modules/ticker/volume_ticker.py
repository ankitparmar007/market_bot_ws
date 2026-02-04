from typing import Dict, Any, Optional
import asyncio
from datetime import datetime
from server.utils.logger import log
from server.db.collections import Collections

from asyncio import Queue


from server.utils.ist import IndianDateTime


class VolumeTicker:

    # ==========================================================
    # STATE: PER-INSTRUMENT
    # ==========================================================

    instruments_and_symbols_state: Dict[str, Dict[str, Any]] = {}

    @classmethod
    def generate_state_for_instruments(cls, instruments_and_symbols: Dict[str, str]):
        cls.instruments_and_symbols_state = {
            instrument: {
                "prev_direction": None,
                "prev_ltt": None,  # datetime of last tick
                "prev_vtt": None,  # last cumulative volume
                "prev_ltp": None,  # last price
                "prev_trade_key": None,  # (ltp, ltt) to dedupe
                "minute_buy": 0.0,
                "minute_sell": 0.0,
                "minute_volume": 0.0,
                "symbol": symbol,
            }
            for instrument, symbol in instruments_and_symbols.items()
        }

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

            await Collections.volume_history.insert_many(cls.docs)

        except Exception as e:
            log.error(f"Batch insert failed: {e}, retrying...")

            for doc in cls.docs:
                await cls.write_queue.put(doc)

            await asyncio.sleep(1)
        cls.docs = []

    @classmethod
    async def db_writer(cls):
        log.info("[VolumeTicker.db_writer] started")

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
            log.info("[VolumeTicker.db_writer] stopped flushed all docs")

    # ==========================================================
    # DIRECTION CLASSIFICATION (PRICE-BASED)
    # ==========================================================

    @classmethod
    def get_direction(cls, instrument_key: str, ltp: float) -> str:
        st = cls.instruments_and_symbols_state[instrument_key]
        prev_ltp: Optional[float] = st["prev_ltp"]

        if prev_ltp is None:
            st["prev_ltp"] = ltp
            st["prev_direction"] = "neutral"
            return "neutral"

        if ltp > prev_ltp:
            st["prev_ltp"] = ltp
            st["prev_direction"] = "buy"
            return "buy"

        if ltp < prev_ltp:
            st["prev_ltp"] = ltp
            st["prev_direction"] = "sell"
            return "sell"

        # If price unchanged, keep previous direction, default to neutral
        return st["prev_direction"] or "neutral"

    # ==========================================================
    # PROCESS TICK FOR ONE INSTRUMENT
    # ==========================================================

    @classmethod
    async def process_tick(
        cls, instrument_key: str, ltp: float, ltt: datetime, vtt: int
    ):
        """
        Process each tick:
        - dedupe
        - calculate true traded volume
        - handle minute rollover
        - attribute volume to buy/sell using direction
        - enqueue minute result to DB writer
        """
        st = cls.instruments_and_symbols_state[instrument_key]
        symbol = st.get("symbol", "")

        # ---- De-duplicate repeated ticks ----
        trade_key = (ltp, ltt)
        if st["prev_trade_key"] == trade_key:
            return
        st["prev_trade_key"] = trade_key

        # ---- Compute true traded volume from cumulative vtt ----
        vol_delta = 0
        if st["prev_vtt"] is not None:
            vol_delta = vtt - st["prev_vtt"]
            if vol_delta < 0:
                # Safety: if vtt resets or goes backwards
                vol_delta = 0
        st["prev_vtt"] = vtt

        # ---- Minute rollover ----
        prev_ltt: Optional[datetime] = st["prev_ltt"]
        if prev_ltt and (ltt.minute != prev_ltt.minute):
            # Use previous tick time truncated to minute as the bucket timestamp
            ts_minute = prev_ltt.replace(second=0, microsecond=0)

            buy = st["minute_buy"]
            sell = st["minute_sell"]
            total = st["minute_volume"]
            delta = buy - sell

            # log.info(
            #     f"\n=== 1 MIN RESULTS [{ts_minute.strftime('%H:%M')}] ==="
            #     f"\nInstrument: {instrument_key}"
            #     f"\nBuy Vol   : {buy:.0f}"
            #     f"\nSell Vol  : {sell:.0f}"
            #     f"\nTotal Vol : {total:.0f}"
            #     f"\nDelta     : {delta:.0f}\n"
            # )

            # Enqueue for DB writing
            await cls.write_queue.put(
                {
                    "timestamp": ts_minute.isoformat(),
                    "instrument_key": instrument_key,
                    "symbol": symbol,
                    "buy": int(buy),
                    "sell": int(sell),
                    "total": int(total),
                    "delta": int(delta),
                }
            )

            # Reset minute counters
            st["minute_buy"] = 0.0
            st["minute_sell"] = 0.0
            st["minute_volume"] = 0.0

        # ---- Determine direction for this tick ----
        direction = cls.get_direction(instrument_key, ltp)

        # ---- Allocate true traded volume to buy/sell ----
        if vol_delta > 0:
            if direction == "buy":
                st["minute_buy"] += vol_delta
            elif direction == "sell":
                st["minute_sell"] += vol_delta

            st["minute_volume"] += vol_delta

        # log.info(
        #     f"{direction.upper()}: {instrument_key}, {ltt}, vtt={vtt}, LTP={ltp}, "
        #     f"BUY={st['minute_buy']:.0f}, SELL={st['minute_sell']:.0f}, "
        #     f"ΔVOL={vol_delta:.0f}, MTV={st['minute_volume']:.0f}"
        # )

        st["prev_ltt"] = ltt

    # ==========================================================
    # PARSE AND HANDLE FEED (ALL INSTRUMENTS)
    # ==========================================================

    @classmethod
    async def handle_feed(cls, instrument_key: str, market_ff: Dict[str, Any]):

        ltpc = market_ff.get("ltpc", {})
        if not ltpc:
            return

        ltp_str = ltpc.get("ltp")
        ltt_str = ltpc.get("ltt")
        if ltp_str is None or ltt_str is None:
            return

        ltp = float(ltp_str)
        ltt = IndianDateTime.fromtimestamp(ltt_str)

        vtt_str = market_ff.get("vtt")
        if vtt_str is None:
            return

        vtt = int(vtt_str)

        await cls.process_tick(instrument_key, ltp, ltt, vtt)
