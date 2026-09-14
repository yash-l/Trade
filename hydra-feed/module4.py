cat << 'EOF' > module4.py
import time
import asyncio
import logging
import uuid
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import redis.asyncio as aioredis
import orjson

logger = logging.getLogger("RiskFirewall")
logging.basicConfig(level=logging.INFO, format='%(message)s')
IST = ZoneInfo("Asia/Kolkata")

# ============================================================
# 1. PROP-DESK CONFIGURATION
# ============================================================
@dataclass
class RiskConfig:
    INITIAL_CAPITAL: float = 50000.0
    MAX_RISK_PER_TRADE_PCT: float = 0.01
    MAX_CAPITAL_PER_TRADE_PCT: float = 0.20
    MIN_CAPITAL_FLOOR: float = 0.70          
    
    # VaR & Drawdowns
    MAX_PORTFOLIO_VAR_PCT: float = 0.03
    MAX_DAILY_LOSS_INR: float = 2500.0
    MAX_DRAWDOWN_PCT: float = 0.05
    DRAWDOWN_THROTTLE_PCT: float = 0.03
    
    # Beta & Correlation
    MAX_PORTFOLIO_BETA: float = 3.0
    INDEX_BETAS: dict = field(default_factory=lambda: {
        "NIFTY": 1.0, "BANKNIFTY": 1.5, "FINNIFTY": 1.2, "MIDCPNIFTY": 1.3
    })
    
    MAX_PORTFOLIO_DELTA: float = 500.0
    MAX_PORTFOLIO_GAMMA: float = 50.0
    MAX_POSITION_AGE_MINUTES: int = 120
    
    EXPIRY_SQUARE_OFF_TIME: int = 1445
    TRADE_END_TIME: int = 1510

    LOT_SIZES: dict = field(default_factory=lambda: {
        "NIFTY": 25, "BANKNIFTY": 15, "FINNIFTY": 25, "MIDCPNIFTY": 50
    })

# ============================================================
# 2. ATOMIC LUA SCRIPT (SRE PATCHED)
# ============================================================
# Check and reserve happens ENTIRELY inside Redis to prevent race conditions
LUA_RESERVE = """
local key = KEYS[1]
local requested = tonumber(ARGV[1])
local max_margin = tonumber(ARGV[2])
local current_reserved = tonumber(redis.call('GET', key) or '0')

if current_reserved + requested > max_margin then
    return -1 -- Rejected: Margin Exceeded
end

redis.call('INCRBYFLOAT', key, requested)
return tonumber(redis.call('GET', key))
"""

# ============================================================
# 3. QUANT ENGINE
# ============================================================
class QuantEngine:
    @staticmethod
    def approximate_greeks(spot: float, strike: float, opt_type: str, is_expiry: bool):
        moneyness = spot / strike
        distance = abs(1.0 - moneyness)
        if opt_type == "CE": delta = 0.8 if moneyness > 1.01 else (0.2 if moneyness < 0.99 else 0.5)
        else: delta = -0.8 if moneyness < 0.99 else (-0.2 if moneyness > 1.01 else -0.5)
        gamma = max(0.0, 1.0 - (distance * 40)) * (2.0 if is_expiry else 0.5)
        return delta, gamma

    @staticmethod
    def stress_loss(qty: int, spot: float, delta: float, gamma: float):
        shock = spot * 0.02
        return abs(qty * ((delta * (-shock if delta > 0 else shock)) + 0.5 * gamma * (shock ** 2)))

# ============================================================
# 4. THE APEX FIREWALL
# ============================================================
class HydraRiskFirewall:
    def __init__(self, redis_url: str, account_id: str):
        self.config = RiskConfig()
        self.redis_url = redis_url
        self.account_id = account_id
        self.ns = f"acct:{account_id}"
        self.node_id = f"{self.ns}:risk_node_{uuid.uuid4().hex[:6]}"
        
        # Keys
        self.reserved_key = f"{self.ns}:capital_reserved"
        self.killed_key = f"{self.ns}:SYSTEM:KILLED"
        
        self.redis = None
        self.lua_reserve = None
        self.quant = QuantEngine()
        self.signal_semaphore = asyncio.Semaphore(5)

        # State
        self.net_liquidity = self.config.INITIAL_CAPITAL
        self.peak_equity = self.config.INITIAL_CAPITAL
        self.broker_free_margin = self.config.INITIAL_CAPITAL
        self.previous_margin = self.config.INITIAL_CAPITAL
        
        self.current_drawdown_pct = 0.0
        self.current_portfolio_var = 0.0
        self.current_portfolio_beta = 0.0
        self.net_delta = 0.0
        self.net_gamma = 0.0
        
        self.loss_streak = 0
        self.active_families = []
        self.trade_timestamps = []
        self.last_feedback_time = time.time()

    # --- LIFECYCLE & KILLS ---
    async def trigger_kill(self, reason: str):
        await self.redis.set(self.killed_key, "1")
        await self.redis.xadd(f"{self.ns}:signals:force_exit", {"action": "PANIC_FLATTEN", "reason": reason})
        logger.critical(f"馃拃 HARD KILL TRIGGERED: {reason}")

    async def _heartbeat(self):
        while True:
            await self.redis.setex(f"{self.ns}:risk:heartbeat", 5, time.time())
            await asyncio.sleep(2)

    async def _daily_reset_scheduler(self):
        while True:
            now = datetime.now(IST)
            tomorrow = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            await asyncio.sleep((tomorrow - now).total_seconds())
            self.peak_equity = self.config.INITIAL_CAPITAL
            self.loss_streak = 0
            self.trade_timestamps.clear()
            await self.redis.delete(self.killed_key)
            logger.info("馃寘 Midnight IST Reset Complete.")

    # --- STATE SYNCING ---
    async def sync_portfolio(self):
        while True:
            await asyncio.sleep(1)
            try:
                margin_data = await self.redis.get(f"{self.ns}:broker:live_margin")
                if margin_data:
                    margin = orjson.loads(margin_data)
                    self.broker_free_margin = float(margin.get("free_margin", 0.0))
                    self.net_liquidity = float(margin.get("net_liquidity", self.config.INITIAL_CAPITAL))
                    
                    # Margin Spike Detection
                    if self.previous_margin > 0:
                        if ((self.previous_margin - self.broker_free_margin) / self.previous_margin) > 0.30:
                            await self.trigger_kill("BROKER_MARGIN_SPIKE")
                    self.previous_margin = self.broker_free_margin

                pnl_data = await self.redis.get(f"{self.ns}:portfolio:live_pnl")
                if pnl_data:
                    pnl = orjson.loads(pnl_data)
                    equity = self.config.INITIAL_CAPITAL + float(pnl.get("realized", 0)) + float(pnl.get("unrealized", 0))

                    if equity > self.peak_equity: self.peak_equity = equity
                    self.current_drawdown_pct = (self.peak_equity - equity) / self.peak_equity

                    if (equity - self.config.INITIAL_CAPITAL) <= -self.config.MAX_DAILY_LOSS_INR:
                        await self.trigger_kill(f"DAILY_LOSS_LIMIT: {equity - self.config.INITIAL_CAPITAL}")
                    if self.current_drawdown_pct >= self.config.MAX_DRAWDOWN_PCT:
                        await self.trigger_kill(f"DRAWDOWN_LIMIT: {self.current_drawdown_pct*100:.1f}%")

                positions_data = await self.redis.get(f"{self.ns}:portfolio:active_positions")
                if positions_data:
                    positions = orjson.loads(positions_data)
                    self.active_families = [p.get("family", "UNK") for p in positions.values()]
                    
                    self.current_portfolio_var = sum(abs(p.get("expected_risk", 0)) for p in positions.values())
                    self.current_portfolio_beta = sum(self.config.INDEX_BETAS.get(p.get("family", "UNK"), 1.0) for p in positions.values())
                    self.net_delta = sum(p.get("indep_delta", 0) * p.get("qty", 0) for p in positions.values())
                    self.net_gamma = sum(p.get("indep_gamma", 0) * p.get("qty", 0) for p in positions.values())

                    if abs(self.net_delta) > self.config.MAX_PORTFOLIO_DELTA: await self.trigger_kill("DELTA_LIMIT")
                    if self.net_gamma > self.config.MAX_PORTFOLIO_GAMMA: await self.trigger_kill("GAMMA_LIMIT")
            except Exception: pass

    # --- WATCHDOGS ---
    async def _execution_feedback_monitor(self):
        while True:
            try:
                streams = await self.redis.xreadgroup(
                    groupname="risk_feedback_group", consumername=self.node_id,
                    streams={f"{self.ns}:execution:feedback": ">"}, count=10, block=1000
                )
                if streams:
                    for _, messages in streams:
                        for msg_id, raw_feedback in messages:
                            self.last_feedback_time = time.time()
                            action = raw_feedback.get("action")
                            
                            if action == "FILLED":
                                expected, actual = float(raw_feedback.get("expected_price", 1)), float(raw_feedback.get("fill_price", 1))
                                slip_pct = abs(actual - expected) / expected
                                
                                # SRE PATCH: Pro Slippage Escalation Ladder
                                if slip_pct >= 0.08:
                                    await self.trigger_kill(f"EXTREME_SLIPPAGE: {slip_pct*100:.2f}%")
                                elif slip_pct >= 0.04:
                                    await self.redis.setex(f"{self.ns}:SYSTEM:COOLDOWN", 300, "1")
                                    logger.warning(f"COOLDOWN TRIGGERED: {slip_pct*100:.2f}% slippage detected.")
                                elif slip_pct >= 0.02:
                                    logger.warning(f"WARNING: {slip_pct*100:.2f}% slippage observed.")
                            
                            if action in ["CLOSED", "REJECTED", "CANCELLED"]:
                                released = float(raw_feedback.get("capital_released", 0.0))
                                if released > 0: await self.redis.decrbyfloat(self.reserved_key, released)

                            await self.redis.xack(f"{self.ns}:execution:feedback", "risk_feedback_group", msg_id)
            except Exception: await asyncio.sleep(1)

    async def _execution_silence_watchdog(self):
        while True:
            await asyncio.sleep(5)
            if len(self.active_families) > 0 and (time.time() - self.last_feedback_time) > 10.0:
                await self.trigger_kill("EXECUTION_STALL_DETECTED")

    # --- ROUTER & SIZER ---
    async def consume_signals(self):
        while True:
            try:
                streams = await self.redis.xreadgroup(
                    groupname="risk_group", consumername=self.node_id,
                    streams={f"{self.ns}:signals:pre_risk": ">"}, count=10, block=100
                )
                for _, messages in streams:
                    for msg_id, signal in messages:
                        asyncio.create_task(self._route_with_semaphore(msg_id, signal))
            except Exception: await asyncio.sleep(1)

    async def _route_with_semaphore(self, msg_id: str, signal: dict):
        async with self.signal_semaphore:
            await self.handle_signal(msg_id, signal)

    async def handle_signal(self, msg_id: str, signal: dict):
        lock_key = f"{self.ns}:lock:signal:{msg_id}"
        if not await self.redis.setnx(lock_key, "1"): return
        await self.redis.expire(lock_key, 10)

        action = signal.get("action")
        if action == "CLOSE_POSITION":
            await self.redis.xadd(f"{self.ns}:signals:alpha", signal)
            await self.redis.xack(f"{self.ns}:signals:pre_risk", "risk_group", msg_id)
            return

        if await self.redis.get(self.killed_key) or await self.redis.get(f"{self.ns}:SYSTEM:COOLDOWN"):
            await self.redis.xack(f"{self.ns}:signals:pre_risk", "risk_group", msg_id); return

        family = signal.get("symbol", "UNKNOWN").split()[0].upper()
        if await self.redis.get(f"{self.ns}:SYSTEM:BLOCK:{signal.get('symbol')}"):
            await self.redis.xack(f"{self.ns}:signals:pre_risk", "risk_group", msg_id); return

        # Beta Correlation & Frequency
        beta_weight = self.config.INDEX_BETAS.get(family, 1.0)
        if (self.current_portfolio_beta + beta_weight) > self.config.MAX_PORTFOLIO_BETA:
            await self.redis.xack(f"{self.ns}:signals:pre_risk", "risk_group", msg_id); return

        current_time = time.time()
        self.trade_timestamps = [t for t in self.trade_timestamps if current_time - t < 3600]
        if self.current_drawdown_pct > self.config.DRAWDOWN_THROTTLE_PCT and len(self.trade_timestamps) >= 1:
            await self.redis.xack(f"{self.ns}:signals:pre_risk", "risk_group", msg_id); return

        # Sizing & Pre-Trade VaR Stacking
        lot_size = self.config.LOT_SIZES.get(family, 1)
        entry, sl = float(signal.get("spot_entry", 0)), float(signal.get("spot_sl", 0))
        spot, strike = float(signal.get("underlying_spot", 1)), float(signal.get("target_strike", 1))
        
        effective_entry, effective_sl = entry * 1.005, sl - (entry * 0.005) if (entry > sl) else sl + (entry * 0.005)
        risk_per_unit = abs(effective_entry - effective_sl)

        if risk_per_unit <= 0:
            await self.redis.xack(f"{self.ns}:signals:pre_risk", "risk_group", msg_id); return

        risk_budget = self.net_liquidity * self.config.MAX_RISK_PER_TRADE_PCT * float(signal.get("confidence", 0.5)) * (0.5 if self.loss_streak >= 3 else 1.0)
        lots = math.floor(risk_budget / (risk_per_unit * lot_size))
        qty, capital_required = lots * lot_size, lots * lot_size * effective_entry

        if lots <= 0 or capital_required > (self.net_liquidity * self.config.MAX_CAPITAL_PER_TRADE_PCT):
            await self.redis.xack(f"{self.ns}:signals:pre_risk", "risk_group", msg_id); return

        delta, gamma = self.quant.approximate_greeks(spot, strike, signal.get("opt_type", "CE"), signal.get("expiry_date") == datetime.now(IST).strftime("%Y-%m-%d"))
        trade_stress_loss = self.quant.stress_loss(qty, spot, delta, gamma)
        
        # Portfolio VaR Stacking Verification
        if (self.current_portfolio_var + trade_stress_loss) > (self.net_liquidity * self.config.MAX_PORTFOLIO_VAR_PCT):
            await self.redis.xack(f"{self.ns}:signals:pre_risk", "risk_group", msg_id); return

        # SRE PATCH: Atomic Ledger Execution (Checks total margin bounds)
        reserve_result = await self.lua_reserve(keys=[self.reserved_key], args=[capital_required, self.broker_free_margin])
        if reserve_result != -1:
            signal.update({"position_size_shares": qty, "expected_capital": capital_required, "indep_delta": delta, "indep_gamma": gamma})
            await self.redis.xadd(f"{self.ns}:signals:alpha", signal)
            self.trade_timestamps.append(current_time)
            logger.info(f"鉁 ATOMIC APPROVE: {lots} Lots of {family} | VaR Stacked | Ledger: 鈧箋reserve_result:.2f}")

        await self.redis.xack(f"{self.ns}:signals:pre_risk", "risk_group", msg_id)

    async def start(self):
        self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
        self.lua_reserve = self.redis.register_script(LUA_RESERVE)

        try: 
            await self.redis.xgroup_create(f"{self.ns}:signals:pre_risk", "risk_group", id="0", mkstream=True)
            await self.redis.xgroup_create(f"{self.ns}:execution:feedback", "risk_feedback_group", id="0", mkstream=True)
        except Exception: pass

        logger.info(f"馃洝锔 Hydra Prop-Desk Firewall Online for {self.ns}")

        asyncio.create_task(self._heartbeat())
        asyncio.create_task(self.sync_portfolio())
        asyncio.create_task(self._execution_feedback_monitor())
        asyncio.create_task(self._execution_silence_watchdog())
        asyncio.create_task(self._daily_reset_scheduler())
        
        await self.consume_signals()

if __name__ == "__main__":
    engine = HydraRiskFirewall("redis://127.0.0.1:6379/0", "MOTO_G84_MAIN")
    try: asyncio.run(engine.start())
    except KeyboardInterrupt: print("Shutdown.")
EOF
