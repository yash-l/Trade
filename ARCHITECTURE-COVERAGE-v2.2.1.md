# HYDRA Ω-APEX v2.2.2 — Coverage Archive

This release packages the pending-work **capabilities** into the application baseline. Real broker credentials, NSE market-hours execution, historical option datasets, static-IP approval, and 100–200 live-market PAPER trades remain external verification gates and are therefore not falsely marked complete.

## P0 capability archive
- Order/trade confirmation WebSocket with bounded reconnect.
- RemoteOrderID-centric order FSM.
- Broker startup reconciliation snapshot (OrderBook + TradeBook + NetPosition).
- Historical 1m candle gap-recovery coordinator.
- Existing LIVE fail-closed gates preserved.

## P1 research capability archive
- Purged/embargo splits.
- CPCV split generator.
- 10,000-run Monte Carlo.
- Profit concentration analysis.
- PBO screening and DSR proxy, explicitly labelled non-formal until full trial matrix inputs exist.
- Ablation/negative-control utilities.
- Rolling edge-drift and change-point proxy.
- Execution slippage/latency/spread report.
- Experiment registry.

## Evidence gates still required
- Actual 5paisa market-hours PAPER session.
- Actual order confirmation stream observed with user's account.
- Broker/static-IP/algo compliance confirmation.
- Historical NIFTY option bid/ask/chain dataset.
- 100–200 qualifying PAPER trades.
- Formal CPCV/DSR/PBO calculations using the completed experiment matrix.

## Safety
LIVE remains disabled by default. No generated research statistic is a probability of profit or a claim of trading profitability.
