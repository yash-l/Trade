# HYDRA Ω-APEX v2.2.3 — 5paisa LIVE-DATA PAPER App

This is the runnable HYDRA application. **Default mode is PAPER.** No real order is sent unless separate LIVE gates are deliberately enabled.

## v2.2.3: real 5paisa data integration

The 5paisa path now runs end to end for live-data PAPER trading:

`5paisa NIFTY live WebSocket → 1m candles → 3m/5m APEX → signal → Scrip Master → nearest valid NIFTY option → real level-5 bid/ask depth → risk gate → PAPER buy at ask → live option WebSocket → trailing SL/target → PAPER sell at bid`

Implemented:

- Direct XStream/5paisa REST + WebSocket adapter with TLS certificate verification enabled.
- OAuth RequestToken → AccessToken support.
- NIFTY ScripCode auto-resolution from 5paisa Scrip Master; manual ScripCode remains supported.
- Automatic NIFTY option selection from Scrip Master:
  - CE for LONG APEX signal, PE for SHORT.
  - nearest normal-Apex expiry with `HYDRA_5PAISA_MIN_DTE=2` by default, so 0–1 DTE is skipped unless configured otherwise.
  - evaluates ATM and one-strike ITM candidates.
- Real 5-level 5paisa Market Depth used to obtain executable best bid/ask before PAPER entry.
- Spread and capital filters before risk approval.
- Live option `MarketFeedV3` WebSocket after PAPER entry.
- PAPER entry at real ask and exit at real bid, never ideal LTP fills.
- Stale option quote guard; REST depth refresh is attempted before a PAPER exit if the WebSocket quote is too old.
- Broker tick timestamps are preferred over device time when available.
- WebSocket reconnect with exponential backoff.
- Out-of-order tick rejection and live-data-gap journaling; HYDRA does not fabricate missing candles.
- Read-only live-data diagnostics endpoint/script; diagnostics never place orders.

## Existing HYDRA controls

- APEX CORE v1: **5m regime → 3m breakout/retest → 1m trigger**.
- Synchronized 1m → 3m → 5m candles.
- ₹15,000 candidate capital baseline.
- ₹275 normal candidate risk, ₹375 hard all-in cap, ₹600 daily guard, maximum 2 completed trades/day, one position.
- TRUE NET P&L cost engine with effective-dated 2026 charge schedule.
- Bid/ask-aware PAPER execution.
- Trailing position manager:
  - +1.0R breakeven/profit lock.
  - +1.4R 3-minute structure trailing stop.
  - +1.6R trail-able runner target.
  - target extends in configurable R steps, capped at 4R by default.
  - ratchet-only: LONG stop/target never loosen downward; SHORT never loosen upward.
- SQLite WAL append-only journal + hash chain.
- 5paisa order book/trade book/positions/margin/order-status read APIs for reconciliation.
- RemoteOrderID-ready protected limit-order function.
- Real-money mode blocked by default.
- Local mobile dashboard: `http://127.0.0.1:8181/?ui=221`.

## Termux install

```bash
cd ~
rm -rf HYDRA-OMEGA-APEX-v2.2.3-5PAISA-LIVE-DATA-PAPER-APP
unzip -o /sdcard/Download/HYDRA-OMEGA-APEX-v2.2.3-5PAISA-LIVE-DATA-PAPER.zip -d ~
cd ~/HYDRA-OMEGA-APEX-v2.2.3-5PAISA-LIVE-DATA-PAPER-APP
chmod +x install-termux.sh start.sh stop.sh health.sh verify-ui.sh
bash install-termux.sh
./start.sh
```

Dashboard:

`http://127.0.0.1:8181/?ui=221`

## Connect your 5paisa account — PAPER data only

First edit `.env` and fill these three values from your 5paisa/XStream developer account:

```env
HYDRA_5PAISA_APP_KEY=YOUR_APP_KEY
HYDRA_5PAISA_USER_ID=YOUR_USER_ID
HYDRA_5PAISA_ENCRYPTION_KEY=YOUR_ENCRYPTION_KEY
```

Keep:

```env
HYDRA_MODE=paper
HYDRA_DATA_MODE=demo
HYDRA_LIVE_ENABLED=false
```

Generate OAuth URL:

```bash
python scripts/5paisa_setup.py oauth-url
```

Open the printed URL, sign in to 5paisa, and copy the returned `RequestToken` from the redirect URL. Exchange it immediately:

```bash
python scripts/5paisa_setup.py exchange 'PASTE_REQUEST_TOKEN_HERE'
```

The script stores the AccessToken and ClientCode in `.env` without printing the AccessToken, sets:

```env
HYDRA_MODE=paper
HYDRA_DATA_MODE=5paisa
```

Then restart:

```bash
./stop.sh
./start.sh
```

## Read-only 5paisa check

Before waiting for APEX signals:

```bash
python scripts/5paisa_live_check.py
```

or, while the app is running:

```bash
curl -s http://127.0.0.1:8181/api/5paisa/live-data-check
```

This check reads authentication, NIFTY market data and margin. **Orders sent: 0.**

Watch live status:

```bash
curl -s http://127.0.0.1:8181/api/status
```

Useful events to watch:

- `UNDERLYING_RESOLVED`
- `SIGNAL`
- `OPTION_SELECTION`
- `RISK_DECISION`
- `PAPER_OPEN`
- `TRAIL_UPDATE`
- `PAPER_CLOSE`
- `LIVE_DATA_GAP`
- `WS_ERROR`

Logs:

```bash
tail -f .runtime/hydra.log
```

## 5paisa settings

```env
# Leave 0 to auto-resolve NIFTY from Scrip Master.
HYDRA_5PAISA_NIFTY_SCRIP_CODE=0
HYDRA_5PAISA_NIFTY_EXCH_TYPE=C
HYDRA_5PAISA_NIFTY_SYMBOL=NIFTY
HYDRA_5PAISA_OPTION_STRIKE_STEP=50
HYDRA_5PAISA_MIN_DTE=2

HYDRA_MAX_SPREAD_PCT=1.0
HYDRA_PREFERRED_SPREAD_PCT=0.5
HYDRA_MAX_QUOTE_AGE_SEC=3.0
HYDRA_MAX_CAPITAL_PER_TRADE=8000
```

If Scrip Master auto-resolution fails on a future 5paisa schema change, set `HYDRA_5PAISA_NIFTY_SCRIP_CODE` manually and HYDRA will use it.

## LIVE money remains blocked

The live order function exists only behind all of these gates:

```env
HYDRA_MODE=live
HYDRA_LIVE_ENABLED=true
HYDRA_LIVE_CONFIRM=I_UNDERSTAND_REAL_ORDERS
HYDRA_STATIC_IP_CONFIRMED=true
HYDRA_ALGO_ID=...
```

**Do not enable these for this release.** v2.2.3 is intended for real 5paisa data + PAPER execution so the strategy can accumulate realistic forward evidence.

## Validation

```bash
PYTHONPATH=. python -m compileall -q hydra scripts tests
PYTHONPATH=. python -m pytest -q
PYTHONPATH=. python scripts/selftest.py
```

The release tests live option resolution and depth parsing with deterministic mocked 5paisa responses. A real 5paisa session cannot be verified during packaging because no user AccessToken/ClientCode is stored in this release.

## Status

**LIVE DATA CAPABLE / PAPER EXECUTION / REAL-MONEY LIVE NOT VERIFIED OR APPROVED.**

No profitability claim. Demo results are not market evidence. Real 5paisa forward PAPER results must be collected before any live-money promotion.

## v2.3.0 Premium Trading UI
The frontend is now a dark-first mobile/desktop trading control surface with Overview, APEX Core, Trades, Adaptive, Broker, Research, System, Settings and More pages. Controls are mapped to real runtime/API actions or explicitly locked. Settings are whitelist-limited and restart-aware.
