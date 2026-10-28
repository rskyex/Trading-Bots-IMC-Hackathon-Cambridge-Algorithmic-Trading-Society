"""
Reads the Coinbase matches feed and tracks, in real time, for the current session:
  - number of trades (TRADES)
  - high minus low price (RANGE)

Settlement:
  TRADES = number of BTC-USD trades during the session
  RANGE  = high minus low BTC-USD trade price during the session
"""

import json
import math
import time
from collections import deque
from datetime import datetime
from threading import Lock, Thread

import websocket  # pip install websocket-client

FEED_URLS = [
    "wss://ws-feed.exchange.coinbase.com/",   # primary
    "wss://es-feed.exchange.coinbase.com/",   # backup
]


def parse_ts(s: str) -> float:
    # "2026-10-27T18:15:52.123456Z" -> epoch seconds
    s = s.rstrip("Z")
    if "." in s:
        head, frac = s.split(".")
        s = head + "." + frac[:6]
        fmt = "%Y-%m-%dT%H:%M:%S.%f"
    else:
        fmt = "%Y-%m-%dT%H:%M:%S"
    from datetime import timezone
    return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp()


def norm_pdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)


def norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def expected_overshoot(S, d):
    """Expected amount by which the running max of a Brownian motion exceeds distance d.
    Reflection principle: max ~ |N(0, S^2)| with S = sigma * sqrt(time left)."""
    if S <= 0:
        return 0.0
    if d <= 0:
        return S * math.sqrt(2 / math.pi)
    z = d / S
    return 2 * (S * norm_pdf(z) - d * (1 - norm_cdf(z)))


class CoinbaseTracker:
    def __init__(self, product_id="BTC-USD"):
        self.product_id = product_id
        self.lock = Lock()
        self.start = None
        self.end = None
        self.recent = deque()         # (t, price) for the last 30 minutes
        self.count = 0
        self.hi = None
        self.lo = None
        self.last_price = None
        self.last_msg_time = 0.0
        self.seen_ids = set()

    # ---- session window ----
    def set_window(self, start_epoch, length_sec):
        with self.lock:
            self.start = start_epoch
            self.end = start_epoch + length_sec
            self.count = 0
            self.hi = self.lo = None
            self.seen_ids.clear()
            # recount trades already received
            for t, p in self.recent:
                self._add_to_window(t, p, None)

    def _add_to_window(self, t, price, trade_id):
        if self.start is None or not (self.start <= t < self.end):
            return
        if trade_id is not None:
            if trade_id in self.seen_ids:
                return
            self.seen_ids.add(trade_id)
        self.count += 1
        self.hi = price if self.hi is None else max(self.hi, price)
        self.lo = price if self.lo is None else min(self.lo, price)

    # ---- feed ----
    def on_message(self, _ws, msg):
        m = json.loads(msg)
        if m.get("type") != "match" or m.get("product_id") != self.product_id:
            return
        t = parse_ts(m["time"])
        p = float(m["price"])
        with self.lock:
            self.last_price = p
            self.last_msg_time = time.time()
            self.recent.append((t, p))
            while self.recent and self.recent[0][0] < t - 1800:
                self.recent.popleft()
            self._add_to_window(t, p, m.get("trade_id"))

    def run_forever(self):
        i = 0
        while True:
            url = FEED_URLS[i % len(FEED_URLS)]
            try:
                print("connecting", url)
                ws = websocket.WebSocketApp(
                    url,
                    on_open=lambda w: w.send(json.dumps({
                        "type": "subscribe",
                        "product_ids": [self.product_id],
                        "channels": ["matches", "heartbeat"],
                    })),
                    on_message=self.on_message,
                    on_error=lambda w, e: print("feed error:", e),
                )
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                print("feed crashed:", e)
            i += 1
            time.sleep(1)

    def start_thread(self):
        Thread(target=self.run_forever, daemon=True).start()

    # ---- estimates ----
    def alive(self):
        return time.time() - self.last_msg_time < 10

    def _rate_and_sigma(self, now):
        """Recent trade rate (trades/s) and price volatility ($/sqrt(s))"""
        pts = [x for x in self.recent if x[0] > now - 120]
        if len(pts) < 5:
            pts = list(self.recent)
        if len(pts) < 2:
            return None, None
        span = max(pts[-1][0] - pts[0][0], 1.0)
        rate = len(pts) / span
        # sigma from one second price changes
        by_sec = {}
        for t, p in pts:
            by_sec[int(t)] = p
        secs = sorted(by_sec)
        diffs = [(by_sec[b] - by_sec[a]) ** 2 / (b - a)
                 for a, b in zip(secs, secs[1:]) if 0 < b - a <= 5]
        sigma = math.sqrt(sum(diffs) / len(diffs)) if diffs else None
        return rate, sigma

    def fair_values(self):
        """Returns {"TRADES": (fair, std), "RANGE": (fair, std)}, or None if unknown"""
        with self.lock:
            if self.start is None or not self.alive():
                return None
            now = time.time()
            if now > self.end + 120:
                # more than 2 min after session end: wait for the next session, use the book
                return None
            if now >= self.end:
                # just after session end: settlement values are already known
                if self.hi is None:
                    return None
                return {"TRADES": (float(self.count), 1.0),
                        "RANGE": (self.hi - self.lo, 0.3)}
            rate, sigma = self._rate_and_sigma(now)
            if rate is None:
                return None
            T = self.end - self.start
            tau = max(0.0, self.end - max(now, self.start))   # seconds left
            out = {}

            # TRADES: trades so far + rate x time left
            exp_more = rate * tau
            out["TRADES"] = (self.count + exp_more,
                             math.sqrt(exp_more) + 0.1 * exp_more)

            # RANGE
            if sigma is not None:
                if now < self.start or self.hi is None:
                    S = sigma * math.sqrt(T)
                    fair = S * math.sqrt(8 / math.pi)
                    out["RANGE"] = (fair, 0.45 * fair)
                else:
                    S = sigma * math.sqrt(tau)
                    p = self.last_price
                    r = self.hi - self.lo
                    up = expected_overshoot(S, self.hi - p)
                    dn = expected_overshoot(S, p - self.lo)
                    out["RANGE"] = (r + up + dn, 0.6 * S + 0.5)
            return out

    def status(self):
        with self.lock:
            r = None if self.hi is None else self.hi - self.lo
            return f"cb: count={self.count} range={r} last={self.last_price}"
