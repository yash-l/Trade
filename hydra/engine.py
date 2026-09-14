from __future__ import annotations

import asyncio
from datetime import timedelta

from .broker.demo import DemoMarket
from .broker.fivepaisa import FivePaisaAdapter, FivePaisaError, OptionContract
from .candles import CandleStore
from .config import Settings
from .costs import EffectiveCostEngine
from .journal import Journal
from .models import Candle, Quote, Signal
from .paper import PaperBroker
from .position_manager import PositionManager
from .risk import RiskEngine
from .strategy.apex_core import ApexCore
from .timeutil import now_ist
from .adaptive import AdaptiveLayer


class HydraEngine:
    def __init__(self, cfg: Settings):
        self.cfg = cfg
        self.errors = cfg.validate()
        self.store = CandleStore()
        self.strategy = ApexCore(cfg.score_threshold)
        self.costs = EffectiveCostEngine()
        self.risk = RiskEngine(cfg, self.costs)
        self.paper = PaperBroker(cfg, self.costs)
        self.position_manager = PositionManager(cfg)
        self.journal = Journal(cfg.db_path)
        self.adaptive = AdaptiveLayer(cfg, self.journal)
        self.demo = DemoMarket()
        self.five = FivePaisaAdapter(cfg)
        self.running = False
        self.task = None
        self.option_task: asyncio.Task | None = None
        self.latest_candle: Candle | None = None
        self.latest_quote: Quote | None = None
        self.last_signal: Signal | None = None
        self.selected_contract: OptionContract | None = None
        self.nifty_scrip_code: int = int(cfg.nifty_scrip_code or 0)
        self.status = "BLOCKED" if self.errors else "READY"
        self.feed_status = "OFFLINE"
        self.health = {
            "data": "UNKNOWN",
            "option_data": "UNKNOWN",
            "broker": "UNKNOWN",
            "risk": "CONFIRMED",
            "journal": "CONFIRMED",
        }
        self.journal.append(
            "BOOT", {"version": "2.3.0", "mode": cfg.mode, "data_mode": cfg.data_mode, "errors": self.errors}
        )

    async def start(self):
        if self.running:
            return
        if self.errors:
            self.status = "BLOCKED"
            return
        self.running = True
        self.status = "PAPER_READY" if self.cfg.mode == "paper" else "LIVE_ARMED"
        if self.cfg.data_mode == "demo":
            self.task = asyncio.create_task(self._run_demo())
        elif self.cfg.data_mode == "5paisa":
            self.task = asyncio.create_task(self._run_5paisa())

    async def stop(self):
        self.running = False
        if self.task:
            self.task.cancel()
        await self._stop_option_stream()
        await self.five.close()
        self.status = "STOPPED"

    async def _run_demo(self):
        self.feed_status = "DEMO_ACCELERATED"
        self.health["data"] = "CONFIRMED"
        self.health["option_data"] = "DEMO"
        self.health["broker"] = "DEMO"
        while self.running:
            c = self.demo.next_candle()
            await self.process_candle(c)
            await asyncio.sleep(0.25)
            if self.demo.i > 170:
                # Never orphan a PAPER position across an accelerated demo reset.
                if self.paper.position:
                    reset_q = await self._paper_quote_for_position(c.close)
                    if reset_q and reset_q.bid > 0 and reset_q.ask > 0:
                        p = self.paper.position
                        row = self.paper.close(reset_q, c.close, "DEMO_RESET", c.ts)
                        if row:
                            self.risk.record_close(row["net"])
                            self.journal.trade(row)
                            self.journal.append("PAPER_CLOSE", row)
                            self.journal.append("TRADE_OUTCOME", {
                                "position_id": p.id, "signal_id": p.signal_id,
                                "net": row["net"], "highest_r": row["highest_r"],
                                "captured_r": row["captured_r"], "reason": "DEMO_RESET",
                                "bucket_key": p.adaptive_bucket,
                            })
                            await self._stop_option_stream()
                        else:
                            continue
                    else:
                        # Defer reset rather than orphaning the position.
                        continue
                self.demo = DemoMarket()
                self.strategy = ApexCore(self.cfg.score_threshold)
                self.store = CandleStore()

    async def _run_5paisa(self):
        try:
            self.feed_status = "RESOLVING_5PAISA"
            self.nifty_scrip_code = await self.five.resolve_nifty_underlying()
            self.journal.append(
                "UNDERLYING_RESOLVED",
                {
                    "symbol": self.cfg.nifty_symbol,
                    "scrip_code": self.nifty_scrip_code,
                    "exchange_type": self.cfg.nifty_exch_type,
                },
            )
        except Exception as exc:
            self.status = "BLOCKED"
            self.health["data"] = "UNKNOWN"
            self.journal.append("DATA_BLOCK", {"reason": str(exc)[:400]})
            return

        self.feed_status = "CONNECTING_5PAISA"
        self.health["broker"] = "CONFIRMED" if self.five.access_token else "UNKNOWN"
        bucket: list[tuple[float, float]] = []
        current_minute = None

        async def onmsg(data):
            nonlocal bucket, current_minute
            items = data if isinstance(data, list) else [data]
            for x in items:
                if not isinstance(x, dict):
                    continue
                try:
                    token = int(float(x.get("Token", -1)))
                except Exception:
                    continue
                if token != self.nifty_scrip_code:
                    continue
                price = float(x.get("LastRate") or 0)
                vol = float(x.get("LastQty") or 0)
                tick_ts = self.five.tick_datetime(x)
                minute = tick_ts.replace(second=0, microsecond=0)
                if price <= 0:
                    continue
                if current_minute is None:
                    current_minute = minute
                if minute < current_minute:
                    self.journal.append(
                        "OUT_OF_ORDER_TICK",
                        {"tick_minute": minute.isoformat(), "current_minute": current_minute.isoformat()},
                    )
                    continue
                if minute != current_minute and bucket:
                    if minute - current_minute > timedelta(minutes=2):
                        self.health["data"] = "DEGRADED"
                        self.journal.append(
                            "LIVE_DATA_GAP",
                            {
                                "from": current_minute.isoformat(),
                                "to": minute.isoformat(),
                                "action": "no synthetic missing candles; current completed bar only",
                            },
                        )
                    vals = [z[0] for z in bucket]
                    c = Candle(
                        current_minute,
                        vals[0],
                        max(vals),
                        min(vals),
                        vals[-1],
                        sum(z[1] for z in bucket),
                    )
                    bucket = []
                    current_minute = minute
                    await self.process_candle(c)
                bucket.append((price, vol))
                self.feed_status = "5PAISA_LIVE"
                self.health["data"] = "CONFIRMED"
                self.health["broker"] = "CONFIRMED"

        inst = [
            {
                "Exch": "N",
                "ExchType": self.cfg.nifty_exch_type,
                "ScripCode": self.nifty_scrip_code,
            }
        ]
        backoff = 1
        while self.running:
            try:
                await self.five.market_stream(inst, onmsg)
                backoff = 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.health["data"] = "UNKNOWN"
                self.feed_status = "RECONNECTING"
                self.journal.append("WS_ERROR", {"stream": "underlying", "error": str(exc)[:300]})
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def _quote_age(self, q: Quote | None) -> float | None:
        if not q:
            return None
        try:
            return max(0.0, (now_ist() - q.ts).total_seconds())
        except Exception:
            return None

    async def _refresh_option_quote(self) -> Quote | None:
        if not self.selected_contract:
            return None
        try:
            q = await self.five.executable_quote(self.selected_contract)
            self.latest_quote = q
            self.health["option_data"] = "CONFIRMED"
            self.journal.append(
                "OPTION_QUOTE_REFRESH",
                {"source": "REST_DEPTH", "quote": q.dict()},
            )
            return q
        except Exception as exc:
            self.health["option_data"] = "UNKNOWN"
            self.journal.append("OPTION_QUOTE_ERROR", {"error": str(exc)[:300]})
            return None

    async def _start_option_stream(self, contract: OptionContract):
        await self._stop_option_stream()
        self.selected_contract = contract
        self.option_task = asyncio.create_task(self._run_option_stream(contract))

    async def _stop_option_stream(self):
        if self.option_task:
            self.option_task.cancel()
            try:
                await self.option_task
            except (asyncio.CancelledError, Exception):
                pass
            self.option_task = None
        if not self.paper.position:
            self.selected_contract = None

    async def _run_option_stream(self, contract: OptionContract):
        inst = [{"Exch": contract.exch, "ExchType": contract.exch_type, "ScripCode": contract.scrip_code}]
        backoff = 1

        async def onmsg(data):
            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict):
                    continue
                try:
                    token = int(float(item.get("Token", -1)))
                except Exception:
                    continue
                if token != contract.scrip_code:
                    continue
                q = self.five.quote_from_feed_item(
                    item,
                    symbol=contract.symbol,
                    scrip_code=contract.scrip_code,
                    previous=self.latest_quote if self.latest_quote and self.latest_quote.scrip_code == contract.scrip_code else None,
                    option_type=contract.option_type,
                    strike=contract.strike,
                    expiry=contract.expiry.isoformat(),
                )
                if q:
                    self.latest_quote = q
                    if q.bid > 0 and q.ask > 0:
                        self.health["option_data"] = "CONFIRMED"

        while self.running and self.paper.position and self.selected_contract and self.selected_contract.scrip_code == contract.scrip_code:
            try:
                await self.five.market_stream(inst, onmsg)
                backoff = 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.health["option_data"] = "UNKNOWN"
                self.journal.append("WS_ERROR", {"stream": "option", "error": str(exc)[:300]})
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 15)

    async def _paper_quote_for_position(self, underlying: float) -> Quote | None:
        if not self.paper.position:
            return None
        if self.cfg.data_mode == "demo":
            q = self.demo.option_quote(self.paper.position.side, underlying)
            self.latest_quote = q
            return q
        if self.cfg.data_mode == "5paisa":
            q = self.latest_quote
            age = self._quote_age(q)
            if (
                q is None
                or q.scrip_code != self.paper.position.scrip_code
                or q.bid <= 0
                or q.ask <= 0
                or age is None
                or age > self.cfg.max_quote_age_sec
            ):
                q = await self._refresh_option_quote()
            return q
        return None

    async def _manage_open_position(self, c: Candle):
        if not self.paper.position:
            return
        q = await self._paper_quote_for_position(c.close)
        p = self.paper.position
        last3 = self.store.three[-1] if self.store.three else None
        tu = self.position_manager.update(p, c, last3)
        if (
            tu.stop_moved
            or tu.target_moved
            or tu.breakeven_activated
            or tu.trailing_stop_activated
            or tu.trailing_target_activated
        ):
            self.journal.append("TRAIL_UPDATE", {"position_id": p.id, **tu.dict()})

        reason = None
        if p.side == "LONG" and c.close <= p.stop_underlying:
            reason = "TRAIL_STOP" if p.breakeven_active or p.trailing_stop_active else "STRUCTURE_STOP"
        elif p.side == "SHORT" and c.close >= p.stop_underlying:
            reason = "TRAIL_STOP" if p.breakeven_active or p.trailing_stop_active else "STRUCTURE_STOP"
        elif p.side == "LONG" and c.close >= p.target_underlying:
            reason = "TRAIL_TARGET" if p.trailing_target_active else "TARGET"
        elif p.side == "SHORT" and c.close <= p.target_underlying:
            reason = "TRAIL_TARGET" if p.trailing_target_active else "TARGET"
        elif (c.ts.hour * 60 + c.ts.minute) >= (15 * 60 + 10):
            reason = "TIME_EXIT"

        if not reason:
            return
        if not q or q.bid <= 0 or q.ask <= 0:
            self.journal.append(
                "PAPER_EXIT_WAIT_QUOTE",
                {"position_id": p.id, "reason": reason, "underlying": c.close},
            )
            return
        row = self.paper.close(q, c.close, reason, c.ts)
        self.risk.record_close(row["net"])
        self.journal.trade(row)
        self.journal.append("PAPER_CLOSE", row)
        self.journal.append("TRADE_OUTCOME", {"position_id": p.id, "signal_id": p.signal_id, "net": row["net"], "highest_r": row["highest_r"], "captured_r": row["captured_r"], "reason": reason, "bucket_key": p.adaptive_bucket})
        await self._stop_option_stream()

    async def _handle_5paisa_signal(self, sig: Signal):
        try:
            contract, q, checked = await self.five.best_executable_option(
                spot=sig.underlying,
                side=sig.side,
                asof=sig.ts.date(),
            )
            self.journal.append(
                "OPTION_SELECTION",
                {
                    "signal": sig.dict(),
                    "selected": contract.dict(),
                    "selected_quote": q.dict(),
                    "checked": checked,
                },
            )
            if contract.lot_size and contract.lot_size != self.cfg.lot_size:
                self.journal.append(
                    "RISK_DECISION",
                    {
                        "approved": False,
                        "reason": "LOT_SIZE_CONFLICT",
                        "configured_lot": self.cfg.lot_size,
                        "master_lot": contract.lot_size,
                    },
                )
                return
            self.latest_quote = q
            d = self.risk.can_trade(sig, q)
            self.journal.append(
                "RISK_DECISION",
                {"signal": sig.dict(), "quote": q.dict(), "decision": d.dict()},
            )
            if not d.approved:
                return
            trail=self.adaptive.trail_params(sig.adaptive_bucket)
            if trail["changed"]:
                event_kind = "ADAPTIVE_PARAM_ADJUSTED_SHADOW" if self.cfg.adaptive_trail_shadow_only else "ADAPTIVE_PARAM_ADJUSTED"
                self.journal.append(event_kind, {"scope":"TRAIL","bucket_key":sig.adaptive_bucket,**trail,"applied":not self.cfg.adaptive_trail_shadow_only})
            applied_trail = None if self.cfg.adaptive_trail_shadow_only else trail
            p = self.paper.open(sig, q, applied_trail)
            self.selected_contract = contract
            self.health["option_data"] = "CONFIRMED"
            self.journal.append("PAPER_OPEN", {**p.dict(), "source": "5PAISA_LIVE_DEPTH"})
            await self._start_option_stream(contract)
        except Exception as exc:
            self.health["option_data"] = "UNKNOWN"
            self.journal.append(
                "SIGNAL_WAIT_OPTION_QUOTE",
                {"signal": sig.dict(), "reason": str(exc)[:500]},
            )

    async def process_candle(self, c: Candle):
        self.latest_candle = c
        sig = self.strategy.on_1m(c)
        new3, new5 = self.store.add_1m(c)
        if new3:
            self.strategy.on_3m(new3)
        if new5:
            self.strategy.on_5m(new5)

        if self.paper.position:
            await self._manage_open_position(c)

        if sig:
            self.last_signal = sig
            spread_pct = self.latest_quote.spread_pct() if self.latest_quote and self.latest_quote.bid>0 and self.latest_quote.ask>0 else None
            features=dict(sig.features or {})
            features["spread_pct"]=spread_pct
            self.journal.append("SIGNAL_FEATURES", {"signal_id": f"{sig.ts.isoformat()}|{sig.side}|{sig.score}", **features})
            self.journal.append("SIGNAL", sig.dict())
            if self.paper.position:
                return
            adaptive_gate=self.adaptive.entry_decision(sig.adaptive_bucket, self.cfg.score_threshold)
            if adaptive_gate["adjusted"]:
                self.journal.append("ADAPTIVE_PARAM_ADJUSTED", {"scope":"ENTRY","bucket_key":sig.adaptive_bucket,"base_threshold":self.cfg.score_threshold,"new_threshold":adaptive_gate["threshold"],"reason":adaptive_gate["reason"],"losses":adaptive_gate["losses"]})
            if sig.score < adaptive_gate["threshold"]:
                self.journal.append("AI_GATE_SHADOW" if self.cfg.adaptive_shadow_only else "AI_GATE", {"bucket_key":sig.adaptive_bucket,"score":sig.score,"threshold":adaptive_gate["threshold"],"action":"WOULD_SKIP"})
                if not self.cfg.adaptive_shadow_only:
                    return
            if self.cfg.data_mode == "demo":
                q = self.demo.option_quote(sig.side, sig.underlying)
                self.latest_quote = q
                d = self.risk.can_trade(sig, q)
                self.journal.append(
                    "RISK_DECISION",
                    {"signal": sig.dict(), "quote": q.dict(), "decision": d.dict()},
                )
                if d.approved:
                    trail=self.adaptive.trail_params(sig.adaptive_bucket)
                    if trail["changed"]:
                        event_kind = "ADAPTIVE_PARAM_ADJUSTED_SHADOW" if self.cfg.adaptive_trail_shadow_only else "ADAPTIVE_PARAM_ADJUSTED"
                        self.journal.append(event_kind, {"scope":"TRAIL","bucket_key":sig.adaptive_bucket,**trail,"applied":not self.cfg.adaptive_trail_shadow_only})
                    applied_trail = None if self.cfg.adaptive_trail_shadow_only else trail
                    p = self.paper.open(sig, q, applied_trail)
                    self.journal.append("PAPER_OPEN", p.dict())
            elif self.cfg.data_mode == "5paisa":
                await self._handle_5paisa_signal(sig)

    def adaptive_snapshot(self):
        events = self.journal.events(5000)
        outcomes = [e["payload"] for e in events if e.get("kind") == "TRADE_OUTCOME"]
        adjustments = [e for e in events if str(e.get("kind","")).startswith("ADAPTIVE_")]
        buckets = {}
        for row in outcomes:
            key = str(row.get("bucket_key") or "UNBUCKETED")
            b = buckets.setdefault(key, {"key":key,"count":0,"losses":0,"net":0.0,"highest":[],"captured":[],"adjustments":0})
            b["count"] += 1
            net = float(row.get("net") or 0)
            b["net"] += net
            b["losses"] += int(net < 0)
            b["highest"].append(float(row.get("highest_r") or 0))
            b["captured"].append(float(row.get("captured_r") or 0))
        for e in adjustments:
            key = str(e.get("payload",{}).get("bucket_key") or "UNBUCKETED")
            if key in buckets: buckets[key]["adjustments"] += 1
        bucket_rows=[]
        for b in buckets.values():
            bucket_rows.append({"key":b["key"],"count":b["count"],"losses":b["losses"],"net":round(b["net"],2),"avg_highest_r":round(sum(b["highest"])/len(b["highest"]),2) if b["highest"] else 0,"avg_captured_r":round(sum(b["captured"])/len(b["captured"]),2) if b["captured"] else 0,"adjustments":b["adjustments"]})
        bucket_rows.sort(key=lambda x:(-x["count"],x["key"]))
        labeled=len(outcomes); target=max(50,int(self.cfg.adaptive_min_shadow_trades))
        return {
            "enabled":bool(self.cfg.adaptive_enabled),
            "shadow_mode":bool(self.cfg.adaptive_shadow_only or self.cfg.adaptive_trail_shadow_only),
            "entry_shadow_only":bool(self.cfg.adaptive_shadow_only),
            "trail_shadow_only":bool(self.cfg.adaptive_trail_shadow_only),
            "min_shadow_trades":target,
            "labeled_trades":labeled,
            "adjustment_count":len(adjustments),
            "defaults":{"breakeven_trigger_r":self.cfg.breakeven_trigger_r,"stop_trigger_r":self.cfg.trailing_stop_trigger_r,"stop_buffer_r":self.cfg.trailing_stop_buffer_r,"target_trigger_r":self.cfg.trailing_target_trigger_r,"target_gap_r":self.cfg.trailing_target_gap_r,"max_target_r":self.cfg.max_target_r},
            "buckets":bucket_rows[:50],
            "events":[e for e in events if str(e.get("kind","")).startswith("ADAPTIVE_") or str(e.get("kind","")).startswith("AI_GATE_")][:40],
        }

    def snapshot(self):
        trades = self.journal.trades(100)
        net = sum(float(x["net"] or 0) for x in trades)
        wins = sum(1 for x in trades if float(x["net"] or 0) > 0)
        losses = sum(1 for x in trades if float(x["net"] or 0) < 0)
        qage = self._quote_age(self.latest_quote)
        return {
            "version": "2.3.0",
            "status": self.status,
            "mode": self.cfg.mode,
            "data_mode": self.cfg.data_mode,
            "live_enabled": bool(self.cfg.live_enabled),
            "static_ip_confirmed": bool(self.cfg.static_ip_confirmed),
            "algo_id": int(self.cfg.algo_id),
            "feed_status": self.feed_status,
            "health": self.health,
            "fivepaisa": {
                "authenticated": bool(self.five.access_token and self.five.client_code),
                "nifty_scrip_code": self.nifty_scrip_code or None,
                "selected_contract": self.selected_contract.dict() if self.selected_contract else None,
                "option_quote_age_sec": round(qage, 3) if qage is not None else None,
            },
            "capital_start": self.cfg.capital,
            "paper_equity": self.cfg.capital + net,
            "true_net_pnl": net,
            "trades": len(trades),
            "wins": wins,
            "losses": losses,
            "risk": {
                "normal": self.cfg.normal_risk,
                "hard": self.cfg.hard_risk,
                "daily": self.cfg.daily_loss,
                "max_trades": self.cfg.max_trades,
                "used_today": self.risk.trades,
                "daily_net": self.risk.daily_net,
            },
            "adaptive": self.adaptive_snapshot(),
            "trailing": {
                "breakeven_trigger_r": self.cfg.breakeven_trigger_r,
                "breakeven_lock_r": self.cfg.breakeven_lock_r,
                "stop_trigger_r": self.cfg.trailing_stop_trigger_r,
                "stop_buffer_r": self.cfg.trailing_stop_buffer_r,
                "target_trigger_r": self.cfg.trailing_target_trigger_r,
                "target_gap_r": self.cfg.trailing_target_gap_r,
                "target_step_r": self.cfg.trailing_target_step_r,
                "max_target_r": self.cfg.max_target_r,
            },
            "regime": self.strategy.latest_regime,
            "last_candle": self.latest_candle.dict() if self.latest_candle else None,
            "last_signal": self.last_signal.dict() if self.last_signal else None,
            "position": self.paper.position.dict() if self.paper.position else None,
            "quote": self.latest_quote.dict() if self.latest_quote else None,
            "errors": self.errors,
            "trades_recent": trades[:20],
            "events": self.journal.events(40),
        }
