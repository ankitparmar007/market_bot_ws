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

        if ltp > state.ltp:
            state.direction = Direction.buy

        elif ltp < state.ltp:
            state.direction = Direction.sell

        return state.direction

    # ==========================================================
    # PROCESS TICK
    # ==========================================================

    def process_tick(self, symbol: str, ltp: float, ltt: datetime, vtt: int):

        state = self.symbols_state.get(symbol)

        if state == None:
            state = VolumeDetailModel(symbol=symbol, ltt=ltt, vtt=vtt, ltp=ltp)
            self.symbols_state[symbol] = state
            return

        if state.ltp == ltp and state.ltt == ltt and state.vtt == vtt:
            return

        vol_delta = 0

        vol_delta = vtt - state.vtt
        if vol_delta < 0:
            vol_delta = 0

        if ltt.minute != state.ltt.minute:

            ts_minute = state.ltt.replace(second=0, microsecond=0)

            try:

                self.write_queue.put_nowait(
                    VolumeDeltaModel(
                        timestamp=ts_minute,
                        symbol=symbol,
                        buy=state.minute_buy_vol,
                        sell=state.minute_sell_vol,
                        total=state.minute_total_vol,
                        delta=state.minute_buy_vol - state.minute_sell_vol,
                    )
                )

            except asyncio.QueueFull:
                log.error("[VolumeTicker.process_tick] queue full, dropping tick")

            state.minute_buy_vol = 0
            state.minute_sell_vol = 0
            state.minute_total_vol = 0

        if vol_delta > 0:
            direction = self.get_direction(state, ltp)

            if direction == Direction.buy:
                state.minute_buy_vol += vol_delta

            elif direction == Direction.sell:
                state.minute_sell_vol += vol_delta

            state.minute_total_vol += vol_delta

        state.ltt = ltt
        state.vtt = vtt
        state.ltp = ltp

    # ==========================================================
    # DISPOSE
    # ==========================================================

    async def dispose(self):

        log.info("[VolumeTicker.dispose] started")

        for symbol, state in self.symbols_state.items():

            ts_minute = state.ltt.replace(second=0, microsecond=0)

            try:

                self.write_queue.put_nowait(
                    VolumeDeltaModel(
                        timestamp=ts_minute,
                        symbol=symbol,
                        buy=state.minute_buy_vol,
                        sell=state.minute_sell_vol,
                        total=state.minute_total_vol,
                        delta=state.minute_buy_vol - state.minute_sell_vol,
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
