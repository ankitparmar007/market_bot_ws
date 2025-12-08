# from typing import Optional, Tuple
# import asyncio
# import websockets
# import json
# import uuid
# from datetime import datetime
# import logging

# from google.protobuf.json_format import MessageToDict
# import marketfeed_pb2 as pb
# from config import Config

# # ==========================================================
# # CONFIG
# # ==========================================================

# URL = "wss://api.upstox.com/v3/feed/market-data-feed"
# HEADERS = {"Authorization": f"Bearer {Config.AUTH_TOKEN_UPSTOX}"}

# INSTRUMENTS = ["NSE_EQ|INE271C01023"]

# # ==========================================================
# # LOGGING
# # ==========================================================

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s — %(levelname)s — %(message)s",
# )
# log = logging.getLogger("upstox_feed")

# # ==========================================================
# # STATE
# # ==========================================================

# prev_direction: Optional[str] = None
# prev_ltt: Optional[datetime] = None
# prev_vtt: Optional[int] = None
# prev_ltp: Optional[float] = None
# prev_trade_key: Optional[Tuple] = None  # dedupe: (ltp, ltt)

# minute_buy = 0.0
# minute_sell = 0.0
# minute_volume = 0.0

# # ==========================================================
# # DECODE PROTOBUF
# # ==========================================================


# def decode_protobuf(buffer):
#     obj = pb.FeedResponse()
#     obj.ParseFromString(buffer)
#     return MessageToDict(obj)


# # ==========================================================
# # DIRECTION CLASSIFICATION (MID-PRICE)
# # ==========================================================


# def get_direction(ltp):
#     global prev_direction, prev_ltp

#     if prev_ltp is None:
#         prev_direction = "neutral"
#         prev_ltp = ltp
#         return "neutral"

#     if ltp > prev_ltp:
#         prev_direction = "buy"
#         prev_ltp = ltp
#         return "buy"
#     elif ltp < prev_ltp:
#         prev_direction = "sell"
#         prev_ltp = ltp
#         return "sell"
#     else:
#         return prev_direction or "neutral"


# # ==========================================================
# # PROCESS TICK
# # ==========================================================


# def process_tick(instrument_key, ltp, ltt: datetime, vtt: int):
#     global prev_ltt, minute_buy, minute_sell, minute_volume
#     global prev_vtt, prev_trade_key

#     # dedupe repeated ticks
#     trade_key = (ltp, ltt)
#     if prev_trade_key == trade_key:
#         return
#     prev_trade_key = trade_key

#     # compute true traded volume
#     vol_delta = 0
#     if prev_vtt is not None:
#         vol_delta = vtt - prev_vtt
#         if vol_delta < 0:
#             vol_delta = 0
#     prev_vtt = vtt

#     # minute rollover
#     if prev_ltt and ltt.minute != prev_ltt.minute:
#         delta = minute_buy - minute_sell

#         log.info(
#             f"\n=== 1 MIN RESULTS [{prev_ltt.strftime('%H:%M')}] ==="
#             f"\nInstrument: {instrument_key}"
#             f"\nBuy Vol   : {minute_buy:.0f}"
#             f"\nSell Vol  : {minute_sell:.0f}"
#             f"\nTotal Vol : {minute_volume:.0f}"
#             f"\nDelta     : {delta:.0f}\n"
#         )

#         with open("volume_delta.txt", "a") as f:
#             f.write(
#                 f"{prev_ltt.strftime('%Y-%m-%d %H:%M:%S')},"
#                 f" buy {minute_buy:.0f},"
#                 f" sell {minute_sell:.0f},"
#                 f" delta {delta:.0f},"
#                 f" total {minute_volume:.0f}\n"
#             )

#         minute_buy = 0
#         minute_sell = 0
#         minute_volume = 0

#     # determine direction
#     direction = get_direction(ltp)

#     # allocate true volume
#     if vol_delta > 0:
#         if direction == "buy":
#             minute_buy += vol_delta
#         elif direction == "sell":
#             minute_sell += vol_delta

#         minute_volume += vol_delta

#     log.info(
#         f"{direction.upper()}: {instrument_key}, {ltt}, vtt={vtt}, LTP={ltp},"
#         f" BUY={minute_buy:.0f}, SELL={minute_sell:.0f}, ΔVOL={vol_delta:.0f}, MTV={minute_volume:.0f}"
#     )

#     prev_ltt = ltt


# # ==========================================================
# # PARSE AND HANDLE FEED
# # ==========================================================


# def handle_feed(data):
#     feeds = data.get("feeds", {})

#     for key in INSTRUMENTS:
#         ff = feeds.get(key, {}).get("fullFeed", {})
#         if not ff:
#             continue

#         market_ff = ff.get("marketFF", {})
#         if not market_ff:
#             continue

#         ltpc = market_ff.get("ltpc", {})
#         if not ltpc:
#             continue

#         ltp = float(ltpc["ltp"])
#         ltt = datetime.fromtimestamp(float(ltpc["ltt"]) / 1000)

#         vtt_str = market_ff.get("vtt")
#         if vtt_str is None:
#             return

#         vtt = int(vtt_str)

#         process_tick(key, ltp, ltt, vtt)


# # ==========================================================
# # WS LOGIC - unchanged
# # ==========================================================


# async def run_ws():
#     async with websockets.connect(
#         URL,
#         additional_headers=HEADERS,
#         ping_interval=20,
#         ping_timeout=10,
#         close_timeout=5,
#     ) as ws:

#         payload = {
#             "guid": str(uuid.uuid4()),
#             "method": "sub",
#             "data": {"mode": "full", "instrumentKeys": INSTRUMENTS},
#         }

#         await ws.send(json.dumps(payload).encode("utf-8"))
#         log.info("Subscription sent")

#         while True:
#             msg = await ws.recv()

#             if isinstance(msg, bytes):
#                 data = decode_protobuf(msg)
#                 if data.get("type") in ("initial_feed", "live_feed"):
#                     handle_feed(data)
#             else:
#                 log.debug(f"Text msg: {msg}")


# async def listen_upstox():
#     while True:
#         try:
#             log.info("Connecting...")
#             await run_ws()
#         except Exception as e:
#             log.error(f"WS error: {e}")
#         log.warning("Reconnecting in 5 seconds...")
#         await asyncio.sleep(5)


# if __name__ == "__main__":
#     try:
#         asyncio.run(listen_upstox())
#     except KeyboardInterrupt:
#         log.warning("Exiting...")


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

from asyncio import Queue


# ==========================================================
# CONFIG
# ==========================================================

URL = "wss://api.upstox.com/v3/feed/market-data-feed"

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


# async def db_writer():
#     """
#     Background task that consumes from write_queue and writes to DB.
#     This ensures feed processing never blocks on DB I/O.
#     """
#     log.info("DB writer started")

#     while True:
#         doc = await write_queue.get()

#         try:
#             # Use a thread for the blocking Mongo operation
#             def _insert():
#                 Collections.volume_history.insert_one(doc)

#             await asyncio.to_thread(_insert)

#         except Exception as e:
#             log.error(f"DB write failed: {e}, re-queuing document and retrying...")
#             # Re-queue for retry
#             await write_queue.put(doc)
#             await asyncio.sleep(1)

#         finally:
#             write_queue.task_done()

async def flush_batch(docs):
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


async def db_writer():
    log.info("DB writer started")

    BATCH_SIZE = 10
    buffer = []

    while True:
        # Wait until a doc is available
        doc = await write_queue.get()
        buffer.append(doc)
        write_queue.task_done()

        # Flush when batch is full
        if len(buffer) >= BATCH_SIZE:
            await flush_batch(buffer)
            buffer = []



# ==========================================================
# PROTOBUF DECODE
# ==========================================================


def decode_protobuf(buffer: bytes) -> Dict[str, Any]:
    obj = pb.FeedResponse()
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
                "timestamp": ts_minute,
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

    log.info(
        f"{direction.upper()}: {instrument_key}, {ltt}, vtt={vtt}, LTP={ltp}, "
        f"BUY={st['minute_buy']:.0f}, SELL={st['minute_sell']:.0f}, "
        f"ΔVOL={vol_delta:.0f}, MTV={st['minute_volume']:.0f}"
    )

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
        ltt = datetime.fromtimestamp(float(ltt_str) / 1000)

        vtt_str = market_ff.get("vtt")
        if vtt_str is None:
            continue

        vtt = int(vtt_str)

        await process_tick(instrument_key, ltp, ltt, vtt)


# ==========================================================
# WS LOGIC
# ==========================================================


async def run_ws():
    res1 = Collections.token.find_one({"_id": "rachit"}, {"_id": 0, "access_token": 1})
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

        await ws.send(json.dumps(payload).encode("utf-8"))
        log.info(f"Subscription sent for instruments: {len(INSTRUMENTS)}")

        while True:
            msg = await ws.recv()

            if isinstance(msg, bytes):
                data = decode_protobuf(msg)
                if data.get("type") in ("initial_feed", "live_feed"):
                    await handle_feed(data)
            else:
                log.debug(f"Text msg: {msg}")


async def listen_upstox():
    while True:
        try:
            log.info("Connecting to Upstox feed...")
            await run_ws()
        except Exception as e:
            log.error(f"WS error: {e}")
        log.warning("Reconnecting in 5 seconds...")
        await asyncio.sleep(5)


# ==========================================================
# MAIN
# ==========================================================


async def main():
    # Start DB writer and WS listener concurrently
    writer_task = asyncio.create_task(db_writer())
    ws_task = asyncio.create_task(listen_upstox())

    await asyncio.gather(writer_task, ws_task)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("Exiting...")
