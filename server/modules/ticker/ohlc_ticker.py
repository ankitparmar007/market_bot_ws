import asyncio
from asyncio import Queue
from typing import Dict, List

from server.db.tables import Tables
from server.modules.ticker.models import OhlcModel
from server.utils.logger import log


class OhlcTicker:

    def __init__(self):

        # symbol -> previous candle
        self.symbol_state: Dict[str, OhlcModel | None] = {}

        self.write_queue: Queue[OhlcModel] = Queue(maxsize=100000)

        self.rows: List[OhlcModel] = []

        self.FLUSH_TIMEOUT = 5

        # batch
        self.BATCH_SIZE = 100

        self.writer_task = asyncio.create_task(self.db_writer())

        log.info("[OhlcTicker] initialized")

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
                    doc.open,
                    doc.high,
                    doc.low,
                    doc.close,
                    doc.volume,
                )
                for doc in self.rows
            ]

            await Tables.intraday_history.insert(
                rows=rows,
                column_names=[
                    "symbol",
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    # "oi",
                ],
            )

        except Exception as e:

            log.error(f"[OhlcTicker.flush_batch] Batch insert failed: {e}")

            # for doc in self.docs:
            #     await self.write_queue.put(doc)

            # await asyncio.sleep(1)

        self.rows = []

    # ==========================================================
    # DB WRITER
    # ==========================================================

    async def db_writer(self):

        log.info("[OhlcTicker.db_writer] started")

        try:
            while True:

                try:
                    doc = await asyncio.wait_for(
                        self.write_queue.get(), timeout=self.FLUSH_TIMEOUT
                    )

                    self.rows.append(doc)
                    self.write_queue.task_done()

                    if len(self.rows) >= self.BATCH_SIZE:
                        await self.flush_batch()

                except asyncio.TimeoutError:
                    # timeout → flush partial batch
                    await self.flush_batch()

        except asyncio.CancelledError:

            while not self.write_queue.empty():
                doc = await self.write_queue.get()
                self.rows.append(doc)
                self.write_queue.task_done()

            await self.flush_batch()

            log.info("[OhlcTicker.db_writer] stopped flushed all docs")

    # ==========================================================
    # PROCESS OHLC
    # ==========================================================

    def process_ohlc(self, candle: OhlcModel):

        prev_candle = self.symbol_state.get(candle.symbol)

        # create state lazily (same as VolumeTicker)
        if prev_candle is None:
            self.symbol_state[candle.symbol] = candle
            return

        # minute changed → write previous candle
        if candle.timestamp > prev_candle.timestamp:
            try:
                self.write_queue.put_nowait(prev_candle)
            except asyncio.QueueFull:
                log.error("[OhlcTicker.process_ohlc] queue full, dropping tick")

        self.symbol_state[candle.symbol] = candle

    # ==========================================================
    # DISPOSE
    # ==========================================================

    async def dispose(self):

        log.info("[OhlcTicker.dispose] started")

        # flush last candles
        for symbol, candle in self.symbol_state.items():

            if candle is None:
                continue

            try:
                self.write_queue.put_nowait(candle)
            except asyncio.QueueFull:
                log.error("[OhlcTicker.dispose] queue full, dropping tick")

        self.writer_task.cancel()

        try:
            await self.writer_task
        except asyncio.CancelledError:
            pass

        log.info("[OhlcTicker.dispose] stopped")
