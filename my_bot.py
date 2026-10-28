"""
Market making bot for TRADES / RANGE
- Fair value from live Coinbase data (falls back to order book microprice)
- Quotes both sides and shifts prices against the current position (inventory skew)
- Never exceeds the configured position limits
- Takes clearly mispriced resting orders immediately

Usage:
    python my_bot.py
(Place next to the organisers' base_bot.py and models.py. Requires Python 3.11+)
"""

import time
from threading import Event, Lock, Thread

import sys
from datetime import datetime

from base_bot import BaseBot
from coinbase_feed import CoinbaseTracker
from models import OrderBook, OrderRequest, Side, Trade

# Session start time and length.
# Can be passed on the command line, e.g.  python my_bot.py 18:15:51
SESSION_START = None   # None = treat launch time as session start
SESSION_LENGTH = 900   # seconds (15 minute session)
# Second argument sets the length in minutes:  python my_bot.py 18:30:00 15
COINBASE_PRODUCT = "BTC-USD"
STD_EDGE_MULT = 0.6    # quote half width = this x model uncertainty (std)
STD_TAKE_MULT = 1.2    # take resting orders mispriced by more than this x std

# ===== Settings =====
CMI_URL = "https://alpha.cmi-exch.com"   # exchange URL
# Products priced from the Coinbase model (others use the order book).
# Can be changed live in params.json
USE_COINBASE_FOR = {"RANGE"}
USERNAME = "example_bot"           # placeholder, real login goes in creds.py
PASSWORD = "password"              # placeholder, real login goes in creds.py
# Third argument selects the credentials module:  python my_bot.py 18:35:00 60 creds_alt
import importlib
_CREDS = sys.argv[3] if len(sys.argv) > 3 else "creds"
try:
    # Credentials live in a separate, git-ignored file
    _c = importlib.import_module(_CREDS.removesuffix(".py"))
    USERNAME, PASSWORD = _c.USERNAME, _c.PASSWORD
    print(f"login: {USERNAME}  ({_CREDS})")
except ImportError:
    print(f"!!! {_CREDS}.py not found. Running with placeholder credentials !!!")

CONFIG = {
    # limit: position limit / size: quote size
    # edge_pct: quote distance from fair value (fraction) / min_edge_ticks: minimum distance in ticks
    # skew_ticks_per_unit: ticks of price shift per unit of position
    # take_edge_pct: cross the spread when an order is this far from fair value
    "TRADES": dict(limit=20, size=2, edge_pct=0.0015, min_edge_ticks=2,
                   skew_ticks_per_unit=0.5, take_edge_pct=0.004),
    "RANGE":  dict(limit=60, size=3, edge_pct=0.04, min_edge_ticks=2,
                   skew_ticks_per_unit=0.2, take_edge_pct=0.10),
}
# Fallback starting position if the exchange API cannot be read
START_POS = {}   # normally read from the exchange at startup
REQUOTE_INTERVAL = 0.5   # seconds between requotes
POSITION_SYNC = 2.0      # seconds between position syncs with the exchange
# ===========================================


# ===== params.json is reloaded every few seconds, so settings change without a restart =====
import json, os
PARAMS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "params.json")
PAUSED = set()
RANGE_FLOOR = False
RANGE_MIN = 0.0
JUMP_PCT = 0.03
JUMP_PAUSE = 30.0
_params_mtime = None


def load_params(bot):
    global USE_COINBASE_FOR, STD_EDGE_MULT, STD_TAKE_MULT, PAUSED, RANGE_FLOOR, RANGE_MIN, JUMP_PCT, JUMP_PAUSE, _params_mtime
    try:
        m = os.path.getmtime(PARAMS_FILE)
    except OSError:
        return
    if m == _params_mtime:
        return
    _params_mtime = m
    try:
        with open(PARAMS_FILE, encoding="utf-8") as f:
            prm = json.load(f)
    except Exception as e:
        print("params.json read error:", e)
        return
    if "use_coinbase_for" in prm:
        USE_COINBASE_FOR = set(prm["use_coinbase_for"])
    STD_EDGE_MULT = prm.get("std_edge_mult", STD_EDGE_MULT)
    STD_TAKE_MULT = prm.get("std_take_mult", STD_TAKE_MULT)
    PAUSED = set(prm.get("paused", []))
    RANGE_FLOOR = bool(prm.get("range_floor", False))
    RANGE_MIN = float(prm.get("range_min", 0))
    JUMP_PCT = float(prm.get("jump_pct", JUMP_PCT))
    JUMP_PAUSE = float(prm.get("jump_pause_sec", JUMP_PAUSE))
    for prod, over in prm.get("config", {}).items():
        if prod in CONFIG:
            CONFIG[prod].update(over)
    if prm.get("session_start"):
        hh, mm, ss = map(int, prm["session_start"].split(":"))
        st = datetime.now().replace(hour=hh, minute=mm, second=ss, microsecond=0).timestamp()
        ln = int(float(prm.get("session_length_min", 15)) * 60)
        if bot.cb.start != st or bot.cb.end != st + ln:
            bot.cb.set_window(st, ln)
    bot.quoted = {p: (None, None) for p in CONFIG}   # force requote on the next loop
    print(f">>> params.json loaded: coinbase={USE_COINBASE_FOR} paused={PAUSED} "
          f"floor={RANGE_FLOOR} range_min={RANGE_MIN} RANGE={CONFIG['RANGE']} TRADES={CONFIG['TRADES']}")


def round_to_tick(x, tick, down=True):
    n = x / tick
    n = int(n) if down else -int(-n)
    return round(n * tick, 10)


class MMBot(BaseBot):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.lock = Lock()
        self.books: dict[str, OrderBook] = {}
        self.pos = {p: START_POS.get(p, 0) for p in CONFIG}
        self.last_trade = {p: None for p in CONFIG}
        self.fair = {p: None for p in CONFIG}
        self.live = {p: {} for p in CONFIG}      # order_id -> (side, price)
        self.quoted = {p: (None, None) for p in CONFIG}
        self.running = True
        self.cb = CoinbaseTracker(COINBASE_PRODUCT)
        self.std = {p: None for p in CONFIG}
        self.seen_trades = set()
        self.mid_hist = {p: [] for p in CONFIG}
        self.jump_until = {p: 0.0 for p in CONFIG}

    # ---------- events ----------
    def on_orderbook(self, ob: OrderBook):
        if ob.product in CONFIG:
            with self.lock:
                self.books[ob.product] = ob

    def on_trades(self, trades: list[Trade]):
        with self.lock:
            for t in trades:
                if t.product not in CONFIG:
                    continue
                if t.id is not None:          # do not count the same fill twice
                    if t.id in self.seen_trades:
                        continue
                    self.seen_trades.add(t.id)
                self.last_trade[t.product] = t.price
                if t.buyer == self.username:
                    self.pos[t.product] += t.volume
                if t.seller == self.username:
                    self.pos[t.product] -= t.volume

    # ---------- fair value ----------
    def estimate_fair(self, p, ob):
        # Primary: model based on live Coinbase data
        try:
            fv = self.cb.fair_values()
        except Exception as e:
            fv = None
            print("fair calc error:", e)
        if fv and p in fv and p in USE_COINBASE_FOR:
            self.fair[p], self.std[p] = fv[p]
            return self.fair[p]
        self.std[p] = None
        # Fallback: order book microprice, excluding our own orders
        bids = [o for o in ob.buy_orders if o.volume - o.own_volume > 0]
        asks = [o for o in ob.sell_orders if o.volume - o.own_volume > 0]
        est = None
        if bids and asks:
            b, a = bids[0], asks[0]
            bv, av = b.volume - b.own_volume, a.volume - a.own_volume
            est = (b.price * av + a.price * bv) / (bv + av)  # microprice
        elif self.last_trade[p] is not None:
            est = self.last_trade[p]
        elif bids:
            est = bids[0].price
        elif asks:
            est = asks[0].price
        if est is None:
            return self.fair[p]
        lt = self.last_trade[p]
        if lt is not None:
            est = 0.7 * est + 0.3 * lt
        prev = self.fair[p]
        self.fair[p] = est if prev is None else 0.5 * prev + 0.5 * est
        if p == "RANGE" and RANGE_FLOOR and self.cb.hi is not None:
            # A range can only widen, so the realised range is a floor
            self.fair[p] = max(self.fair[p], self.cb.hi - self.cb.lo + 3)
        if p == "RANGE" and RANGE_MIN > 0:
            # Manual floor from params.json (realised range checked by hand)
            self.fair[p] = max(self.fair[p], RANGE_MIN)
        return self.fair[p]

    # ---------- order management ----------
    def cancel_product(self, p):
        for oid in list(self.live[p].keys()):
            self.cancel_order_by_id(oid)
        self.live[p].clear()

    def place(self, p, side, price, vol):
        if vol <= 0:
            return
        r = self.send_order(OrderRequest(product=p, side=side, price=price, volume=vol))
        if r is not None and r.filled < r.volume:
            self.live[p][r.id] = (side, price)

    def step(self, p):
        cfg = CONFIG[p]
        with self.lock:
            ob = self.books.get(p)
            pos = self.pos[p]
        if ob is None:
            return
        tick = ob.tick_size or 1
        # ---- Jump stop: if the mid moves sharply within a few seconds, stop quoting for a while ----
        now = time.time()
        if ob.buy_orders and ob.sell_orders:
            mid = (ob.buy_orders[0].price + ob.sell_orders[0].price) / 2
            h = self.mid_hist[p]
            h.append((now, mid))
            while h and h[0][0] < now - 5:
                h.pop(0)
            ref = h[0][1]
            if ref > 0 and abs(mid - ref) / ref > JUMP_PCT:
                if now > self.jump_until[p]:
                    print(f"!!! [{p}] jump {ref:.0f} -> {mid:.0f}. Pausing quotes for {JUMP_PAUSE:.0f}s")
                self.jump_until[p] = now + JUMP_PAUSE
                h.clear()
        if now < self.jump_until[p]:
            if self.live[p]:
                self.cancel_product(p)
            return
        fair = self.estimate_fair(p, ob)
        if fair is None:
            return

        limit = cfg["limit"]
        # 1) take clearly mispriced orders
        std = self.std[p]
        if std is not None:
            take = max(cfg["min_edge_ticks"] * tick, STD_TAKE_MULT * std)
        else:
            take = fair * cfg["take_edge_pct"]
        asks = [o for o in ob.sell_orders if o.volume - o.own_volume > 0]
        bids = [o for o in ob.buy_orders if o.volume - o.own_volume > 0]
        if asks and asks[0].price < fair - take and pos < limit:
            v = int(min(asks[0].volume - asks[0].own_volume, limit - pos, cfg["size"] * 2))
            if v > 0:
                self.cancel_product(p)
                self.place(p, Side.BUY, asks[0].price, v)
                return
        if bids and bids[0].price > fair + take and pos > -limit:
            v = int(min(bids[0].volume - bids[0].own_volume, limit + pos, cfg["size"] * 2))
            if v > 0:
                self.cancel_product(p)
                self.place(p, Side.SELL, bids[0].price, v)
                return

        # 2) market making
        if std is not None:
            edge = max(STD_EDGE_MULT * std, cfg["min_edge_ticks"] * tick)
        else:
            edge = max(fair * cfg["edge_pct"], cfg["min_edge_ticks"] * tick)
        center = fair - pos * cfg["skew_ticks_per_unit"] * tick
        bid = round_to_tick(center - edge, tick, down=True)
        ask = round_to_tick(center + edge, tick, down=False)
        # stay passive: do not cross other participants' quotes
        if asks:
            bid = min(bid, asks[0].price - tick)
        if bids:
            ask = max(ask, bids[0].price + tick)
        if ask <= bid:
            ask = bid + tick

        if self.quoted[p] == (bid, ask) and self.live[p]:
            return  # unchanged quotes are left in place

        self.cancel_product(p)
        buy_vol = int(min(cfg["size"], limit - pos))
        sell_vol = int(min(cfg["size"], limit + pos))
        self.place(p, Side.BUY, bid, buy_vol)
        self.place(p, Side.SELL, ask, sell_vol)
        self.quoted[p] = (bid, ask)
        src = "coinbase" if std is not None else "book"
        print(f"[{p}] pos={pos:+} fair={fair:.2f} ({src}) quote {bid} / {ask}  {self.cb.status()}")

    def sync_positions(self):
        try:
            import requests as _rq
            r = _rq.get(f"{self._cmi_url}/api/position/current-user", headers=self._get_headers())
            net = {}
            if r.ok and not getattr(self, "_printed_pos", False):
                print("POSITION API:", r.text[:300])
                self._printed_pos = True
            for row in (r.json() if r.ok else []):
                v = row.get("net_position", row.get("netPosition", row.get("netVolume")))
                if "product" in row and v is not None:
                    net[row["product"]] = v
            if net:
                with self.lock:
                    for p in CONFIG:
                        if p in net:
                            self.pos[p] = net[p]
        except Exception as e:
            print("position sync failed:", e)

    def loop(self):
        last_sync = 0
        last_log = 0
        while self.running:
            if time.time() - last_log > 10:
                left = (self.cb.end or 0) - time.time()
                print(f"=== {left:.0f}s left | {self.cb.status()} | pos {self.pos} ===")
                last_log = time.time()
            if time.time() - last_sync > POSITION_SYNC:
                self.sync_positions()
                last_sync = time.time()
            load_params(self)
            for p in CONFIG:
                if p in PAUSED:
                    if self.live[p]:
                        self.cancel_product(p)
                    continue
                try:
                    self.step(p)
                except Exception as e:
                    print(f"[{p}] error:", e)
            time.sleep(REQUOTE_INTERVAL)


def main():
    start_str = sys.argv[1] if len(sys.argv) > 1 else SESSION_START
    length = int(float(sys.argv[2]) * 60) if len(sys.argv) > 2 else SESSION_LENGTH
    if start_str:
        hh, mm, ss = map(int, start_str.split(":"))
        start = datetime.now().replace(hour=hh, minute=mm, second=ss, microsecond=0).timestamp()
    else:
        start = time.time()
        start_str = datetime.now().strftime("%H:%M:%S")
    print(f"session: {start_str} + {length}s")

    bot = MMBot(CMI_URL, USERNAME, PASSWORD)
    bot.cb.set_window(start, length)
    bot.cb.start_thread()
    bot.cancel_all_orders()
    bot.sync_positions()
    print("start pos:", bot.pos)
    bot.start()
    t = Thread(target=bot.loop, daemon=True)
    t.start()
    try:
        while True:          # short sleeps so Ctrl+C works on Windows
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("Stopping bot...")
        bot.running = False
        bot.cancel_all_orders()
        bot.stop()


if __name__ == "__main__":
    main()
