from server.api.exceptions import DatabaseException
from server.utils.logger import log

from typing import Dict
import asyncio
from datetime import datetime
from server.modules.ticker.models import Direction, VolumeDeltaModel
from server.utils.logger import log
from server.db.collections import Collections

from asyncio import Queue


class VolumeTicker:

    def __init__(self):

        # per-symbol state
        self.symbols_state: Dict[str, VolumeDeltaModel] = {}

        # queue
        self.write_queue: Queue[dict] = Queue(maxsize=10000)

        self.docs = []

        # batch
        self.BATCH_SIZE = 100

        # start db writer automatically
        self.writer_task = asyncio.create_task(self.db_writer())

        log.info("VolumeTicker initialized")

    # ==========================================================
    # FLUSH
    # ==========================================================

    async def flush_batch(self):

        if not self.docs:
            return

        try:
            await Collections.volume_history_all.insert_many(self.docs)

        except DatabaseException as e:
            log.error(f"VolumeTicker Batch insert failed: {e}, retrying...")

            # for doc in self.docs:
            #     await self.write_queue.put(doc)

            # await asyncio.sleep(1)

        self.docs = []

    # ==========================================================
    # DB WRITER
    # ==========================================================

    async def db_writer(self):

        log.info("[VolumeTicker.db_writer] started")

        try:
            while True:

                doc = await self.write_queue.get()
                self.docs.append(doc)
                self.write_queue.task_done()

                if len(self.docs) >= self.BATCH_SIZE:
                    await self.flush_batch()

        except asyncio.CancelledError:

            while not self.write_queue.empty():
                doc = await self.write_queue.get()
                self.docs.append(doc)
                self.write_queue.task_done()

            await self.flush_batch()

            log.info("[VolumeTicker.db_writer] stopped flushed all docs")

    # ==========================================================
    # DIRECTION
    # ==========================================================

    def get_direction(self, symbol: str, ltp: float) -> Direction:

        state = self.symbols_state[symbol]

        if state.prev_ltp is None:
            state.prev_ltp = ltp

        if ltp > state.prev_ltp:
            state.prev_ltp = ltp
            state.prev_direction = Direction.buy

        if ltp < state.prev_ltp:
            state.prev_ltp = ltp
            state.prev_direction = Direction.sell

        return state.prev_direction

    # ==========================================================
    # PROCESS TICK
    # ==========================================================

    async def process_tick(self, symbol: str, ltp: float, ltt: datetime, vtt: int):

        state = self.symbols_state.get(symbol)

        if not state:
            state = VolumeDeltaModel(symbol=symbol)
            self.symbols_state[symbol] = state

        trade_key = (ltp, ltt)
        if state.prev_trade_key == trade_key:
            return

        state.prev_trade_key = trade_key

        vol_delta = 0
        if state.prev_vtt:
            vol_delta = vtt - state.prev_vtt
            if vol_delta < 0:
                vol_delta = 0

        state.prev_vtt = vtt

        if state.prev_ltt and (ltt.minute != state.prev_ltt.minute):

            ts_minute = state.prev_ltt.replace(second=0, microsecond=0)

            buy = state.minute_buy
            sell = state.minute_sell
            total = state.minute_volume
            delta = buy - sell

            await self.write_queue.put(
                {
                    "timestamp": ts_minute.isoformat(),
                    "symbol": symbol,
                    "buy": buy,
                    "sell": sell,
                    "total": total,
                    "delta": delta,
                }
            )

            state.minute_buy = 0
            state.minute_sell = 0
            state.minute_volume = 0

        direction = self.get_direction(symbol, ltp)

        if vol_delta > 0:

            if direction == Direction.buy:
                state.minute_buy += vol_delta

            elif direction == Direction.sell:
                state.minute_sell += vol_delta

            state.minute_volume += vol_delta

        state.prev_ltt = ltt

    # ==========================================================
    # DISPOSE
    # ==========================================================

    async def dispose(self):

        log.info("Stopping VolumeTicker...")

        # --------------------------------
        # Flush last minute for all symbols
        # --------------------------------
        for symbol, state in self.symbols_state.items():

            if state.prev_ltt is None:
                continue

            if state.minute_volume == 0:
                continue

            ts_minute = state.prev_ltt.replace(second=0, microsecond=0)

            buy = state.minute_buy
            sell = state.minute_sell
            total = state.minute_volume
            delta = buy - sell

            await self.write_queue.put(
                {
                    "timestamp": ts_minute.isoformat(),
                    "symbol": symbol,
                    "buy": buy,
                    "sell": sell,
                    "total": total,
                    "delta": delta,
                }
            )

        self.writer_task.cancel()

        try:
            await self.writer_task
        except asyncio.CancelledError:
            pass

        log.info("VolumeTicker stopped")
