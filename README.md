# Trading bots

Bots I wrote for student trading competitions.

| Folder | Competition | Status |
| --- | --- | --- |
| `imc-market-making/` | IMC trading hackathon (live simulated exchange) | Finished in the top 10 |

---

## IMC market making bot

### The game

Two products traded on a live simulated exchange, each settling on real Bitcoin data from Coinbase over a 15 minute session:

* **TRADES** settles on the number of BTC/USD trades on Coinbase during the session.
* **RANGE** settles on the session high minus the session low of the BTC/USD price. One contract is worth 10 times the price.

Position limits were ±20 for TRADES and ±60 for RANGE.

### Strategy

The bot is a market maker. Every half second it computes a fair value for each product, quotes a bid and an ask around it, and takes any resting order that is clearly mispriced.

**Fair value.** The bot listens to the Coinbase `matches` websocket and tracks the session in real time.

* TRADES: trades so far plus the recent trade rate times the time remaining. The uncertainty grows with the time remaining and sets how wide the quotes are.
* RANGE: the current high minus low, plus the expected extra distance the price will travel beyond the current high and low before the session ends. This uses the reflection principle for Brownian motion, with volatility estimated from one second price changes.
* Because a range can only widen, the realised range is a hard floor on fair value.
* After the session ends but before the exchange closes, both settlement values are known exactly, and the bot trades against any stale quotes.
* When the feed is unavailable, or the session timing is not trusted, the bot falls back to the order book microprice.

**Quoting.** Quote width scales with the model's uncertainty, so quotes are wide early in a session and tighten as the outcome becomes clear. Prices are shifted against the current position (inventory skew) so the bot leans towards flattening.

### Risk management

* **Position limits** below the exchange maximum, set per product in `params.json`.
* **Inventory skew** so that positions are worked back towards zero rather than sitting at the limit.
* **Jump stop.** If the order book mid moves more than 3% within 5 seconds (for example on a news headline), the bot cancels its orders in that product and stops quoting for 30 seconds.
* **Live parameters.** `params.json` is reloaded every few seconds, so limits, spreads, session times or a full pause can be changed without restarting the bot.
* **Position sync** from the exchange API every 2 seconds, with trade id de-duplication, so the bot's view of its position stays correct.
* **`flatten.py`**, a one-off script to close a position at a capped price.

### Results and what went wrong

Finished in the **top 10** overall.

* **Wrong session start.** In the first session the bot assumed the session started when it was launched. It underestimated RANGE, sold close to the full limit, and the position went sharply against it. The fix was to anchor the session window to the real start time and cap RANGE exposure.
* **Position tracking.** Early on, trade events were counted twice, so the bot believed it was at its limit when it was not. Reading the position directly from the exchange fixed this.
* **News shock.** A crypto headline moved TRADES from about 2,800 to over 4,000 in seconds. The bot was paused by hand at the time; the automatic jump stop was added afterwards.
* In the second session, with these fixes in place, the bot ended at about +6,000, almost all from market making TRADES with a near flat position.

Main lesson: most of the losses came from wrong assumptions about the rules and timing, not from the pricing model. Checking those assumptions against the data, and sizing positions so that being wrong is survivable, mattered more than model precision.

### Running it

The exchange's starter files (`base_bot.py`, `models.py`) are provided by the organisers and are not included here.

```
pip install requests sseclient-py websocket-client
cp creds.example.py creds.py      # then fill in your login
python my_bot.py HH:MM:SS 15      # session start time, length in minutes
```

