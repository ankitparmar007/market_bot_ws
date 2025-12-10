from typing import Dict, Any, Optional
import asyncio
import websockets
import json
import uuid
from datetime import datetime
import logging

from google.protobuf.json_format import MessageToDict
from db.collections import Collections
import marketfeed_pb2 as pb

from asyncio import Queue, Task
from datetime import timedelta, timezone

from telegram import Telegram


# ==========================================================
# CONFIG
# ==========================================================

URL = "wss://api.upstox.com/v3/feed/market-data-feed"
IST = timezone(timedelta(hours=5, minutes=30, seconds=0, microseconds=0))


# ==========================================================
# LOGGING
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
)
log = logging.getLogger("upstox_feed")


# ==========================================================
# STATE: PER-INSTRUMENT
# ==========================================================

# One state dict per instrument
state: Dict[str, Dict[str, Any]] = {}


def generate_state_for_instruments(INSTRUMENTS):
    global state
    state = {
        instrument: {
            "prev_direction": None,
            "prev_ltt": None,  # datetime of last tick
            "prev_vtt": None,  # last cumulative volume
            "prev_ltp": None,  # last price
            "prev_trade_key": None,  # (ltp, ltt) to dedupe
            "minute_buy": 0.0,
            "minute_sell": 0.0,
            "minute_volume": 0.0,
        }
        for instrument in INSTRUMENTS
    }


# ==========================================================
# ASYNC DB WRITE QUEUE
# ==========================================================

write_queue: Queue[dict] = Queue()

docs = []


async def flush_batch():
    global docs
    if not docs:
        return

    try:

        def _bulk():
            Collections.volume_history.insert_many(docs, ordered=False)

        await asyncio.to_thread(_bulk)
        log.info(f"Inserted batch of {len(docs)} docs")

    except Exception as e:
        log.error(f"Batch insert failed: {e}, retrying...")

        for doc in docs:
            await write_queue.put(doc)

        await asyncio.sleep(1)
    docs = []


async def db_writer():
    global docs
    log.info("DB writer started")

    BATCH_SIZE = 10

    while True:
        # Wait until a doc is available
        doc = await write_queue.get()
        docs.append(doc)
        write_queue.task_done()

        # Flush when batch is full
        if len(docs) >= BATCH_SIZE:
            await flush_batch()


# ==========================================================
# PROTOBUF DECODE
# ==========================================================


def decode_protobuf(buffer: bytes) -> Dict[str, Any]:
    obj = pb.FeedResponse()  # type: ignore
    obj.ParseFromString(buffer)
    return MessageToDict(obj)


# ==========================================================
# DIRECTION CLASSIFICATION (PRICE-BASED)
# ==========================================================


def get_direction(instrument_key: str, ltp: float) -> str:
    st = state[instrument_key]
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


async def process_tick(instrument_key: str, ltp: float, ltt: datetime, vtt: int):
    """
    Process each tick:
    - dedupe
    - calculate true traded volume
    - handle minute rollover
    - attribute volume to buy/sell using direction
    - enqueue minute result to DB writer
    """
    st = state[instrument_key]

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

        log.info(
            f"\n=== 1 MIN RESULTS [{ts_minute.strftime('%H:%M')}] ==="
            f"\nInstrument: {instrument_key}"
            f"\nBuy Vol   : {buy:.0f}"
            f"\nSell Vol  : {sell:.0f}"
            f"\nTotal Vol : {total:.0f}"
            f"\nDelta     : {delta:.0f}\n"
        )

        # Enqueue for DB writing
        await write_queue.put(
            {
                "timestamp": ts_minute.isoformat(),
                "instrument_key": instrument_key,
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
    direction = get_direction(instrument_key, ltp)

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


async def handle_feed(data: Dict[str, Any]):
    feeds = data.get("feeds") or {}
    for instrument_key, feed in feeds.items():
        ff = feed.get("fullFeed", {})
        if not ff:
            continue

        market_ff = ff.get("marketFF", {})
        if not market_ff:
            continue

        ltpc = market_ff.get("ltpc", {})
        if not ltpc:
            continue

        ltp_str = ltpc.get("ltp")
        ltt_str = ltpc.get("ltt")
        if ltp_str is None or ltt_str is None:
            continue

        ltp = float(ltp_str)
        ltt = datetime.fromtimestamp(float(ltt_str) / 1000, IST)

        vtt_str = market_ff.get("vtt")
        if vtt_str is None:
            continue

        vtt = int(vtt_str)

        await process_tick(instrument_key, ltp, ltt, vtt)


# ==========================================================
# WS LOGIC
# ==========================================================


async def run_ws():
    res1 = Collections.token.find_one({"_id": "ankit"}, {"_id": 0, "access_token": 1})
    AUTH_TOKEN = res1["access_token"]
    HEADERS = {"Authorization": f"Bearer {AUTH_TOKEN}"}

    res2 = Collections.stocks.find({}, {"_id": 0, "instrument_key": 1})
    INSTRUMENTS = [doc["instrument_key"] for doc in res2]
    generate_state_for_instruments(INSTRUMENTS)

    async with websockets.connect(
        URL,
        additional_headers=HEADERS,
        ping_interval=20,
        ping_timeout=10,
        close_timeout=5,
    ) as ws:
        payload = {
            "guid": str(uuid.uuid4()),
            "method": "sub",
            "data": {"mode": "full", "instrumentKeys": INSTRUMENTS},
        }

        await Telegram.send_message("Upstox WS connected and subscribing...")

        await ws.send(json.dumps(payload).encode("utf-8"))
        log.info(f"Subscription sent for instruments: {len(INSTRUMENTS)}")

        await Telegram.send_message(
            "Upstox WS subscription sent for instruments. {}".format(len(INSTRUMENTS))
        )

        while True:
            msg = await ws.recv()

            if isinstance(msg, bytes):
                data = decode_protobuf(msg)
                if data.get("type") in ("initial_feed", "live_feed"):
                    await handle_feed(data)
            else:
                log.debug(f"Text msg: {msg}")


# ==========================================================
# MAIN
# ==========================================================

listen_messages: Task | None = None
writer_task: Task | None = None
ws_task: Task | None = None


retry_count = 3


async def listen_upstox():
    global retry_count, ws_task
    while True:
        try:
            log.info("Connecting to Upstox feed...")
            await run_ws()
        except Exception as e:
            log.error(f"WS error: {e}")
            await Telegram.send_message(
                f"Upstox WS error: {e}, Reconnecting in 5 seconds... retry left: {retry_count}"
            )
            retry_count -= 1
            if retry_count == 0:
                retry_count = 3
                await Telegram.send_message(
                    "Max retries reached. Please check errors and restart."
                )
                ws_task = None
                break

        log.warning("Reconnecting in 5 seconds...")
        await asyncio.sleep(5)


async def telgram_message_task_func(text: str):
    global ws_task  # must be at top!

    print("Callback received →", text)

    if text.lower() == "/start":
        if ws_task and not ws_task.done():
            await Telegram.send_message("WS is already running.")
        else:
            await Telegram.send_message("Starting WS...")
            ws_task = asyncio.create_task(listen_upstox())
            # YES — this starts immediately

    elif text.lower() == "/stop":
        if ws_task and not ws_task.done():
            await Telegram.send_message("Stopping WS...")
            ws_task.cancel()
            ws_task = None
            await Telegram.send_message("WS stopped.")
        else:
            await Telegram.send_message("WS is not running.")

    elif text.lower() == "/status":
        if ws_task and not ws_task.done():
            await Telegram.send_message("WS is running.")
        else:
            await Telegram.send_message("WS is not running.")

    elif text.lower() == "/docs":
        global docs
        await Telegram.send_message(f"Current buffer size: {len(docs)}")

    elif text.lower() == "/flush_docs":
        await flush_batch()
        await Telegram.send_message("Flushed docs buffer.")

    else:
        await Telegram.send_message(
            f"Unknown command: {text},\n available: /start, /stop, /status, /docs, /flush_docs"
        )


async def main():
    await Telegram.start()
    Telegram.set_message_callback(telgram_message_task_func)
    listen_messages = asyncio.create_task(Telegram.listen_messages())
    writer_task = asyncio.create_task(db_writer())

    # Start DB writer and WS listener concurrently

    await asyncio.gather(listen_messages, writer_task)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("Exiting...")
