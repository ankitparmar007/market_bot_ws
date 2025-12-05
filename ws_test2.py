from typing import Optional, Tuple
import asyncio
import websockets
import json
import uuid
from datetime import datetime
import logging

from google.protobuf.json_format import MessageToDict
import marketfeed_pb2 as pb
from config import Config

# ==========================================================
# CONFIG
# ==========================================================

URL = "wss://api.upstox.com/v3/feed/market-data-feed"
HEADERS = {"Authorization": f"Bearer {Config.AUTH_TOKEN_UPSTOX}"}

INSTRUMENTS = ["MCX_FO|465849"]

# ==========================================================
# LOGGING
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
)
log = logging.getLogger("upstox_feed")

# ==========================================================
# STATE
# ==========================================================

prev_direction: Optional[str] = None
prev_ltt: Optional[datetime] = None
prev_vtt: Optional[int] = None
prev_ltp: Optional[float] = None
prev_trade_key: Optional[Tuple] = None  # dedupe: (ltp, ltt)

minute_buy = 0.0
minute_sell = 0.0
minute_volume = 0.0

# ==========================================================
# DECODE PROTOBUF
# ==========================================================


def decode_protobuf(buffer):
    obj = pb.FeedResponse()
    obj.ParseFromString(buffer)
    return MessageToDict(obj)


# ==========================================================
# DIRECTION CLASSIFICATION (MID-PRICE)
# ==========================================================


def get_direction(ltp):
    global prev_direction, prev_ltp

    if prev_ltp is None:
        prev_direction = "neutral"
        prev_ltp = ltp
        return "neutral"

    if ltp > prev_ltp:
        prev_direction = "buy"
        prev_ltp = ltp
        return "buy"
    elif ltp < prev_ltp:
        prev_direction = "sell"
        prev_ltp = ltp
        return "sell"
    else:
        return prev_direction or "neutral"


# ==========================================================
# PROCESS TICK
# ==========================================================


def process_tick(ltp, ltt: datetime, vtt: int):
    global prev_ltt, minute_buy, minute_sell, minute_volume
    global prev_vtt, prev_trade_key

    # dedupe repeated ticks
    trade_key = (ltp, ltt)
    if prev_trade_key == trade_key:
        return
    prev_trade_key = trade_key

    # compute true traded volume
    vol_delta = 0
    if prev_vtt is not None:
        vol_delta = vtt - prev_vtt
        if vol_delta < 0:
            vol_delta = 0
    prev_vtt = vtt

    # minute rollover
    if prev_ltt and ltt.minute != prev_ltt.minute:
        delta = minute_buy - minute_sell

        log.info(
            f"\n=== 1 MIN RESULTS [{prev_ltt.strftime('%H:%M')}] ==="
            f"\nBuy Vol   : {minute_buy:.0f}"
            f"\nSell Vol  : {minute_sell:.0f}"
            f"\nTotal Vol : {minute_volume:.0f}"
            f"\nDelta     : {delta:.0f}\n"
        )

        with open("volume_delta.txt", "a") as f:
            f.write(
                f"{prev_ltt.strftime('%Y-%m-%d %H:%M:%S')},"
                f" buy {minute_buy:.0f},"
                f" sell {minute_sell:.0f},"
                f" delta {delta:.0f},"
                f" total {minute_volume:.0f}\n"
            )

        minute_buy = 0
        minute_sell = 0
        minute_volume = 0

    # determine direction
    direction = get_direction(ltp)

    # allocate true volume
    if vol_delta > 0:
        if direction == "buy":
            minute_buy += vol_delta
        elif direction == "sell":
            minute_sell += vol_delta

        minute_volume += vol_delta

    log.info(
        f"{direction.upper()}: {ltt}, ΔVOL={vol_delta}, vtt={vtt}, LTP={ltp},"
        f" BUY={minute_buy:.0f}, SELL={minute_sell:.0f}, TOT={minute_volume:.0f}"
    )

    prev_ltt = ltt


# ==========================================================
# PARSE AND HANDLE FEED
# ==========================================================


def handle_feed(data):
    feeds = data.get("feeds", {})

    for key in INSTRUMENTS:
        ff = feeds.get(key, {}).get("fullFeed", {})
        if not ff:
            continue

        market_ff = ff.get("marketFF", {})
        if not market_ff:
            continue

        ltpc = market_ff.get("ltpc", {})
        if not ltpc:
            continue

        ltp = float(ltpc["ltp"])
        ltt = datetime.fromtimestamp(float(ltpc["ltt"]) / 1000)

        vtt_str = market_ff.get("vtt")
        if vtt_str is None:
            return

        vtt = int(vtt_str)

        ml = market_ff.get("marketLevel", {})
        quotes = ml.get("bidAskQuote", [])
        if not quotes:
            return

        best = quotes[0]

        process_tick(ltp, ltt, vtt)


# ==========================================================
# WS LOGIC - unchanged
# ==========================================================


async def run_ws():
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
        log.info("Subscription sent")

        while True:
            msg = await ws.recv()

            if isinstance(msg, bytes):
                data = decode_protobuf(msg)
                if data.get("type") in ("initial_feed", "live_feed"):
                    handle_feed(data)
            else:
                log.debug(f"Text msg: {msg}")


async def listen_upstox():
    while True:
        try:
            log.info("Connecting...")
            await run_ws()
        except Exception as e:
            log.error(f"WS error: {e}")
        log.warning("Reconnecting in 5 seconds...")
        await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(listen_upstox())
    except KeyboardInterrupt:
        log.warning("Exiting...")
