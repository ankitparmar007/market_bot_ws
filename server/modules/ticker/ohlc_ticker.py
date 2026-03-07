import asyncio
from asyncio import Queue
from typing import Dict

from server.db.collections import TicksCollections
from server.modules.ticker.models import OhlcModel
from server.utils.logger import log


class OhlcTicker:

    def __init__(self):

        # symbol -> previous candle
        self.symbol_state: Dict[str, OhlcModel | None] = {}

        self.write_queue: Queue[dict] = Queue(maxsize=100000)

        self.docs = []

        self.FLUSH_TIMEOUT = 5

        # batch
        self.BATCH_SIZE = 100

        self.writer_task = asyncio.create_task(self.db_writer())

        log.info("[OhlcTicker] initialized")

    # ==========================================================
    # FLUSH
    # ==========================================================

    async def flush_batch(self):

        if not self.docs:
            return

        try:
            await TicksCollections.intraday_history.insert_many(self.docs)

        except Exception as e:

            log.error(f"[OhlcTicker.flush_batch] Batch insert failed: {e}")

            # for doc in self.docs:
            #     await self.write_queue.put(doc)

            # await asyncio.sleep(1)

        self.docs = []

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

                    self.docs.append(doc)
                    self.write_queue.task_done()

                    if len(self.docs) >= self.BATCH_SIZE:
                        await self.flush_batch()

                except asyncio.TimeoutError:
                    # timeout → flush partial batch
                    await self.flush_batch()

        except asyncio.CancelledError:

            while not self.write_queue.empty():
                doc = await self.write_queue.get()
                self.docs.append(doc)
                self.write_queue.task_done()

            await self.flush_batch()

            log.info("[OhlcTicker.db_writer] stopped flushed all docs")

    # ==========================================================
    # PROCESS OHLC
    # ==========================================================

    async def process_ohlc(self, symbol: str, candle: OhlcModel):

        prev_candle = self.symbol_state.get(symbol)

        # create state lazily (same as VolumeTicker)
        if prev_candle is None:
            self.symbol_state[symbol] = candle
            return

        # minute changed → write previous candle
        if prev_candle.ts != candle.ts:

            try:
                await self.write_queue.put(
                    {
                        "symbol": symbol,
                        "timestamp": prev_candle.ts,
                        "open": prev_candle.open,
                        "high": prev_candle.high,
                        "low": prev_candle.low,
                        "close": prev_candle.close,
                        "volume": prev_candle.volume,
                        "oi": prev_candle.oi,
                    }
                )
            except asyncio.CancelledError:
                log.error("[OhlcTicker.process_ohlc] queue full, dropping tick")

        self.symbol_state[symbol] = candle

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
                await self.write_queue.put(
                    {
                        "symbol": symbol,
                        "timestamp": candle.ts,
                        "open": candle.open,
                        "high": candle.high,
                        "low": candle.low,
                        "close": candle.close,
                        "volume": candle.volume,
                        "oi": candle.oi,
                    }
                )
            except asyncio.CancelledError:
                log.error("[OhlcTicker.dispose] queue full, dropping tick")

        self.writer_task.cancel()

        try:
            await self.writer_task
        except asyncio.CancelledError:
            pass

        log.info("[OhlcTicker.dispose] stopped")
