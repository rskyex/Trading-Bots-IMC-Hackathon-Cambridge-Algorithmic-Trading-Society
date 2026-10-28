"""
Emergency script to close a position at a capped price

Usage:
    python flatten.py                      -> buy back 40 RANGE at 155 or better
    python flatten.py RANGE BUY 40 155     -> product, side, volume, limit price
    python flatten.py TRADES SELL 2 2900   -> sell 2 TRADES at 2900 or better

Never fills at a worse price than the limit.
"""
import sys
import time

from base_bot import BaseBot
from models import OrderRequest, Side
from my_bot import CMI_URL, USERNAME, PASSWORD


class Flat(BaseBot):
    def on_orderbook(self, ob):
        pass

    def on_trades(self, trades):
        pass


def main():
    product = sys.argv[1] if len(sys.argv) > 1 else "RANGE"
    side = Side(sys.argv[2].upper()) if len(sys.argv) > 2 else Side.BUY
    volume = int(sys.argv[3]) if len(sys.argv) > 3 else 40
    limit_price = float(sys.argv[4]) if len(sys.argv) > 4 else 155

    bot = Flat(CMI_URL, USERNAME, PASSWORD)
    print("Cancelling all open orders")
    bot.cancel_all_orders()

    print(f"Sending {side} {volume} {product} @ {limit_price}")
    r = bot.send_order(OrderRequest(product=product, side=side, price=limit_price, volume=volume))
    if r:
        print(f"filled {r.filled} / {r.volume}  status={r.status} {r.message or ''}")
    print("Waiting 15s for fills")
    time.sleep(15)

    print("Cancelling any remaining order")
    bot.cancel_all_orders()
    print("Done. Check your position on the exchange.")


if __name__ == "__main__":
    main()
