from server.utils.logger import log
from typing import Dict, List
import asyncio
from datetime import datetime
from asyncio import Queue

from server.modules.ticker.models import Direction, VolumeDeltaModel, VolumeDetailModel
from server.db.tables import Tables


class VolumeTicker:

    def __init__(self):

        self.symbols_state: Dict[str, VolumeDetailModel] = {}

        self.write_queue: Queue[VolumeDeltaModel] = Queue(maxsize=100000)

        self.rows: List[VolumeDeltaModel] = []

        self.BATCH_SIZE = 100

        self.FLUSH_TIMEOUT = 5

        self.writer_task = asyncio.create_task(self.db_writer())

        log.info("[VolumeTicker] initialized")

    # ==========================================================
    # FLUSH
    # ==========================================================

    async def flush_batch(self):

        if not self.rows:
            return

        try:

            rows = [
                (
                    doc.symbol,
                    doc.timestamp,
                    doc.buy,
                    doc.sell,
                    doc.total,
                    doc.delta,
                )
                for doc in self.rows
            ]

            await Tables.volume_history.insert(
                rows=rows,
                column_names=[
                    "symbol",
                    "timestamp",
                    "buy",
                    "sell",
                    "total",
                    "delta",
                ],
            )

        except Exception as e:
            log.error(f"[VolumeTicker.flush_batch] Batch insert failed: {e}")

        self.rows = []

    # ==========================================================
    # DB WRITER
    # ==========================================================

    async def db_writer(self):

        log.info("[VolumeTicker.db_writer] started")

        try:
            while True:
                try:
                    doc = await asyncio.wait_for(
                        self.write_queue.get(), timeout=self.FLUSH_TIMEOUT
                    )

                    self.rows.append(doc)

                    if len(self.rows) >= self.BATCH_SIZE:
                        await self.flush_batch()

                except asyncio.TimeoutError:
                    # timeout → flush partial batch
                    await self.flush_batch()

        except asyncio.CancelledError:

            while not self.write_queue.empty():
                doc = await self.write_queue.get()
                self.rows.append(doc)

            await self.flush_batch()

            log.info("[VolumeTicker.db_writer] stopped flushed all rows")

    # ==========================================================
    # DIRECTION
    # ==========================================================

    def get_direction(self, state: VolumeDetailModel, ltp: float) -> Direction:

        if state.prev_ltp is None:
            state.prev_ltp = ltp

        elif ltp > state.prev_ltp:
            state.prev_ltp = ltp
            state.prev_direction = Direction.buy

        elif ltp < state.prev_ltp:
            state.prev_ltp = ltp
            state.prev_direction = Direction.sell

        return state.prev_direction

    # ==========================================================
    # PROCESS TICK
    # ==========================================================

    def process_tick(self, symbol: str, ltp: float, ltt: datetime, vtt: int):

        state = self.symbols_state.get(symbol)

        if not state:
            state = VolumeDetailModel(symbol=symbol)
            self.symbols_state[symbol] = state

        if state.prev_ltp == ltp and state.prev_ltt == ltt:
            return

        vol_delta = 0

        if state.prev_vtt is not None:
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

            try:

                self.write_queue.put_nowait(
                    VolumeDeltaModel(
                        timestamp=ts_minute,
                        symbol=symbol,
                        buy=buy,
                        sell=sell,
                        total=total,
                        delta=delta,
                    )
                )

            except asyncio.QueueFull:
                log.error("[VolumeTicker.process_tick] queue full, dropping tick")

            state.minute_buy = 0
            state.minute_sell = 0
            state.minute_volume = 0

        direction = self.get_direction(state, ltp)

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

        log.info("[VolumeTicker.dispose] started")

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

            try:

                self.write_queue.put_nowait(
                    VolumeDeltaModel(
                        timestamp=ts_minute,
                        symbol=symbol,
                        buy=buy,
                        sell=sell,
                        total=total,
                        delta=delta,
                    )
                )

            except asyncio.QueueFull:
                log.error("[VolumeTicker.dispose] queue full")

        self.writer_task.cancel()

        try:
            await self.writer_task
        except asyncio.CancelledError:
            pass

        log.info("[VolumeTicker.dispose] stopped")
