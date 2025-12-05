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

# INSTRUMENTS = [
#     "NSE_EQ|INE114A01011",  # change to your instrument
# ]

# # ==========================================================
# # LOGGING
# # ==========================================================

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s — %(levelname)s — %(message)s",
# )
# log = logging.getLogger("upstox_feed")


# # ==========================================================
# # DECODE PROTOBUF
# # ==========================================================

# def decode_protobuf(buffer):
#     obj = pb.FeedResponse()
#     obj.ParseFromString(buffer)
#     return MessageToDict(obj)


# # ==========================================================
# # STATE
# # ==========================================================

# prev_direction: Optional[str] = None
# prev_ltt: Optional[datetime] = None          # for minute buckets
# prev_trade_key: Optional[Tuple] = None       # (ltp, ltq, ltt) to avoid duplicates
# prev_vtt: Optional[int] = None               # cumulative volume today

# minute_buy = 0
# minute_sell = 0
# minute_total = 0  # this will be based on vtt delta


# # ==========================================================
# # CLASSIFY TRADE
# # ==========================================================

# def classify_trade(ltp: float, ltq: int, bid: float, ask: float) -> str:
#     """
#     Returns 'buy', 'sell' or 'neutral' and updates minute_buy/minute_sell.
#     """
#     global minute_buy, minute_sell, prev_direction

#     # Ignore non-positive volumes just in case
#     if ltq <= 0:
#         return "neutral"

#     # Buy aggressor: trade hit/above ask
#     if ltp >= ask:
#         minute_buy += ltq
#         prev_direction = "buy"
#         return "buy"

#     # Sell aggressor: trade hit/below bid
#     if ltp <= bid:
#         minute_sell += ltq
#         prev_direction = "sell"
#         return "sell"

#     # Trade between bid-ask (mid-price) -> fallback to previous direction
#     if prev_direction == "buy":
#         minute_buy += ltq
#         return "buy"
#     elif prev_direction == "sell":
#         minute_sell += ltq
#         return "sell"

#     return "neutral"


# # ==========================================================
# # PROCESS TICK
# # ==========================================================

# def process_tick(ltp: float, ltt: datetime, ltq: int, bid: float, ask: float, vtt: int):
#     global prev_ltt, minute_buy, minute_sell, minute_total, prev_trade_key, prev_vtt

#     # 1) De-duplicate identical ticks (Upstox may resend same ltpc)
#     trade_key = (ltp, ltq, ltt)
#     if prev_trade_key == trade_key:
#         # Same last trade repeated, ignore for volume
#         return
#     prev_trade_key = trade_key

#     # 2) Handle minute rollover BEFORE counting new tick
#     if prev_ltt and ltt.minute != prev_ltt.minute:
#         delta = minute_buy - minute_sell

#         log.info(
#             f"\n=== 1 MIN RESULTS [{prev_ltt.strftime('%H:%M')}] ==="
#             f"\nBuy Vol   : {minute_buy}"
#             f"\nSell Vol  : {minute_sell}"
#             f"\nTotal Vol : {minute_total}  (should match 1-min candle vol)"
#             f"\nDelta     : {delta}\n"
#         )

#         with open("volume_delta.txt", "a") as f:
#             f.write(
#                 f"{prev_ltt.strftime('%Y-%m-%d %H:%M:%S')},"
#                 f" buy {minute_buy},"
#                 f" sell {minute_sell},"
#                 f" delta {delta},"
#                 f" total {minute_total}\n"
#             )

#         minute_buy = 0
#         minute_sell = 0
#         minute_total = 0

#     # 3) Compute volume delta from vtt (cumulative volume)
#     vol_delta = 0
#     if prev_vtt is not None:
#         vol_delta = vtt - prev_vtt
#         # Guard against negative or crazy values (day change, feed reset, etc.)
#         if vol_delta < 0:
#             vol_delta = 0
#     prev_vtt = vtt

#     # 4) Classify trade direction using ltq, bid/ask
#     direction = classify_trade(ltp, ltq, bid, ask)

#     # 5) Use vtt delta for total minute volume
#     minute_total += vol_delta

#     log.info(
#         f"{direction.upper()}: t={ltt}, LTP={ltp}, LTQ={ltq}, "
#         f"ΔVOL={vol_delta}, MIN_TOTAL={minute_total}, BID={bid}, ASK={ask}"
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

#         # LTPC
#         ltpc = market_ff.get("ltpc", {})
#         if not ltpc:
#             continue

#         ltp = float(ltpc["ltp"])
#         ltq = int(ltpc["ltq"])
#         ltt = datetime.fromtimestamp(float(ltpc["ltt"]) / 1000)

#         # vtt = volume traded today (string)
#         vtt_str = market_ff.get("vtt")
#         if vtt_str is None:
#             # If vtt missing, fallback to ltq-based total
#             vtt = 0
#         else:
#             vtt = int(vtt_str)

#         # Orderbook
#         ml = market_ff.get("marketLevel", {})
#         quotes = ml.get("bidAskQuote", [])

#         # Skip if no orderbook (e.g., pre-open/closed)
#         if not quotes:
#             return

#         best = quotes[0]
#         bid = float(best["bidP"])
#         ask = float(best["askP"])

#         process_tick(ltp, ltt, ltq, bid, ask, vtt)


# # ==========================================================
# # WEBSOCKET LOOP
# # ==========================================================

# async def run_ws():
#     async with websockets.connect(
#         URL,
#         additional_headers=HEADERS,
#         ping_interval=20,
#         ping_timeout=10,
#         close_timeout=5,
#         max_queue=None,
#     ) as ws:

#         # subscribe
#         payload = {
#             "guid": str(uuid.uuid4()),
#             "method": "sub",
#             "data": {
#                 "mode": "full",
#                 "instrumentKeys": INSTRUMENTS,
#             },
#         }

#         # IMPORTANT: send as TEXT, not bytes
#         await ws.send(json.dumps(payload).encode("utf-8"))
#         log.info("Subscription sent")

#         # listen
#         while True:
#             try:
#                 msg = await ws.recv()
#             except Exception as e:
#                 log.error(f"WS recv error: {e}")
#                 raise

#             if isinstance(msg, bytes):
#                 data = decode_protobuf(msg)
#                 # print(data)
#                 if data.get("type") in ("initial_feed", "live_feed"):
#                     handle_feed(data)

#             else:
#                 log.debug(f"Text msg: {msg}")


# # ==========================================================
# # RECONNECT WRAPPER
# # ==========================================================

# async def listen_upstox():
#     while True:
#         try:
#             log.info("Connecting...")
#             await run_ws()
#         except Exception as e:
#             log.error(f"WS error: {e}")

#         log.warning("Reconnecting in 5 seconds...")
#         await asyncio.sleep(5)


# # ==========================================================
# # MAIN
# # ==========================================================

# if __name__ == "__main__":
#     try:
#         asyncio.run(listen_upstox())
#     except KeyboardInterrupt:
#         log.warning("Exiting...")
