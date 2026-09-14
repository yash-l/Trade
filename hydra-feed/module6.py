import os
import time
import asyncio
import logging
import random
import collections
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, Dict, List
from concurrent.futures import ThreadPoolExecutor

import redis.asyncio as aioredis
import orjson
from py5paisa import FivePaisaClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s | MAHORAGA | [%(levelname)s] | %(message)s")
logger = logging.getLogger("HYDRA_ALPHA")

@dataclass
class AlphaConfig:
    ACCOUNT_ID: str = "HYDRA_PROD"
    REDIS_URL: str = "redis://127.0.0.1:6379/0"
    CAPITAL: float = 15000.0
    RISK_PER_TRADE_PCT: float = 0.005 
    TRADEABLE_SYMBOLS: list = field(default_factory=lambda: ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"])
    
    MAX_TRADES_PER_DAY: int = 10
    MAX_OPEN_RISK: float = 3000.0
    MAX_DAILY_LOSS: float = 300.0  
    MAX_CONSECUTIVE_LOSSES: int = 3  
    PREMIUM_RISK_MULTIPLIER: float = 0.6 
    
    MORNING_LOCK_MIN: int = 17 
    AFTERNOON_LOCK_HOUR: int = 15
    AFTERNOON_LOCK_MIN: int = 20 
    MAX_SPREAD_PCT: float = 0.005 
    MIN_VOL_SPIKE_MULTIPLIER: float = 1.8
    MIN_TREND_SLOPE: float = 0.00005
    
    MIN_ATR_PCT: float = 0.0002  
    MAX_ATR_PCT: float = 0.005   
    
    GAMMA_EXPLOSION_HOUR: int = 13
    GAMMA_EXPLOSION_MIN: int = 30
    GAMMA_RISK_REDUCTION: float = 0.3 
    
    MAX_POOLS: int = 50 
    MAX_PENDING_SWEEPS: int = 20 
    CLUSTER_TOLERANCE_PCT: float = 0.0002 
    MIN_CLUSTER_SIZE: int = 3             
    PRE_SWEEP_PROXIMITY: float = 0.0005   
    
    SIGNAL_COOLDOWN_SEC: int = 300 
    SWEEP_CONFIRM_DELAY_SEC: float = 2.0 
    SWEEP_DECAY_SEC: int = 60
    MIN_SIGNAL_CONFIDENCE: float = 0.70 

# ============================================================
# 1. ADVANCED INSTITUTIONAL ENGINES
# ============================================================

class GammaExposureMap:
    def __init__(self):
        self.gex_profile = collections.defaultdict(float)
        self.session_date = None
        
    def update(self, strike: int, oi: int, gamma: float, spot: float, ts: float):
        current_date = datetime.fromtimestamp(ts).date()
        if self.session_date != current_date:
            self.gex_profile.clear()
            self.session_date = current_date
            
        self.gex_profile[strike] = oi * gamma * spot
        
        # ✅ FIX 7: Prevent Memory Growth in Gamma Profile
        if len(self.gex_profile) > 200:
            for k in list(self.gex_profile.keys())[:50]:
                del self.gex_profile[k]
        
    def get_pinning_strike(self) -> Optional[int]:
        if not self.gex_profile: return None
        return max(self.gex_profile.items(), key=lambda x: abs(x[1]))[0]

class LiquidityVoidDetector:
    def __init__(self):
        self.recent_voids = []

    def check(self, current_candle: dict, avg_vol: float, atr: float) -> Optional[str]:
        range_size = current_candle["high"] - current_candle["low"]
        vol = current_candle["volume"]
        if range_size > (atr * 1.5) and vol < (avg_vol * 0.5):
            body_direction = "BULLISH" if current_candle["close"] > current_candle["open"] else "BEARISH"
            logger.debug(f"🕳️ LIQUIDITY VOID DETECTED | Direction: {body_direction} | Initiating Mean Reversion")
            return "PUT" if body_direction == "BULLISH" else "CALL"
        return None

class LiquidityHeatmapEngine:
    def __init__(self, bin_size=10):
        self.bin_size = bin_size
        self.volume_profile = collections.defaultdict(int)
        
    def update(self, ltp: float, vol: int):
        price_bin = round(ltp / self.bin_size) * self.bin_size
        self.volume_profile[price_bin] += vol
        
        # ✅ FIX 4: Prevent Heatmap Memory Growth
        if len(self.volume_profile) > 200:
            for k in list(self.volume_profile.keys())[:50]:
                del self.volume_profile[k]
        
    def get_stop_clusters(self, current_price: float, atr: float) -> dict:
        sorted_bins = sorted(self.volume_profile.items(), key=lambda x: x[1], reverse=True)
        above = [b[0] for b in sorted_bins if b[0] > current_price and b[0] <= current_price + (atr * 3)][:2]
        below = [b[0] for b in sorted_bins if b[0] < current_price and b[0] >= current_price - (atr * 3)][:2]
        return {"resistance_clusters": above, "support_clusters": below}

class MicrostructureAnalyzer:
    def __init__(self):
        self.last_bq, self.last_aq = 0, 0
        self.absorption_count = 0

    def analyze(self, ltp: float, bq: int, aq: int, vol: int, avg_vol: float, candle_range: float):
        signals = []
        if vol > avg_vol * 3 and candle_range < (ltp * 0.0002):
            self.absorption_count += 1
            if self.absorption_count >= 3:
                signals.append("ICEBERG_ABSORPTION")
                self.absorption_count = 0
        else:
            self.absorption_count = 0

        if self.last_aq > bq * 10 and aq < bq and vol < avg_vol: signals.append("ASK_SPOOFING")
        elif self.last_bq > aq * 10 and bq < aq and vol < avg_vol: signals.append("BID_SPOOFING")
        if vol > avg_vol * 5 and bq < aq * 0.1: signals.append("BEARISH_IGNITION")

        self.last_bq, self.last_aq = bq, aq
        return signals

class AILiquidityPredictor:
    def __init__(self):
        self.historical_sweeps = []
        self.predicted_pool = None
        
    def learn(self, sweep_price: float, origin_price: float):
        distance = abs(sweep_price - origin_price)
        self.historical_sweeps.append(distance)
        if len(self.historical_sweeps) > 10: self.historical_sweeps.pop(0)
        
    def predict_next_trap(self, current_price: float, trend: str) -> float:
        if not self.historical_sweeps: return 0.0
        avg_dist = sum(self.historical_sweeps) / len(self.historical_sweeps)
        self.predicted_pool = current_price + avg_dist if trend == "BULLISH" else current_price - avg_dist
        return self.predicted_pool

# ============================================================
# 2. STANDARD FILTERS & DETECTORS
# ============================================================
class OpeningScalper:
    def __init__(self):
        self.start_price = None; self.trade_taken = False; self.session_date = None

    def check(self, ltp: float, ts: float, vol: int, prev_close: float, vol_filter) -> Optional[str]:
        now = datetime.fromtimestamp(ts)
        if self.session_date != now.date():
            self.start_price = None; self.trade_taken = False; self.session_date = now.date()

        if not (now.hour == 9 and 15 <= now.minute <= 18): return None
        if self.start_price is None:
            self.start_price = ltp; return None
            
        if self.trade_taken: return None
        if prev_close and abs(self.start_price - prev_close) / prev_close > 0.007: return None 

        move = (ltp - self.start_price) / self.start_price
        if abs(move) > 0.004: return None  

        if abs(move) > 0.0015 and vol_filter.is_spike(vol, 2.0):
            self.trade_taken = True
            return "CALL" if move > 0 else "PUT"
        return None

class OpeningImpulseDetector:
    def __init__(self):
        self.captured = False; self.impulse = None; self.session_date = None

    def check(self, candle: dict, atr_filter, volume_filter) -> Optional[dict]:
        candle_date = datetime.fromtimestamp(candle["ts"]).date()
        if self.session_date != candle_date:
            self.captured = False; self.impulse = None; self.session_date = candle_date

        if self.captured: return None
        candle_time = datetime.fromtimestamp(candle["ts"]).time()
        if candle_time.hour != 9 or candle_time.minute > 25 or candle_time.minute < 15: return None

        range_size = candle["high"] - candle["low"]
        
        # ✅ FIX 5: Safe ATR Division
        avg_tr = (sum(atr_filter.tr_values) / len(atr_filter.tr_values)) if atr_filter.tr_values else 0
        vol_spike = volume_filter.is_spike(candle["volume"], 3.0)

        if (avg_tr > 0 and range_size > (avg_tr * 2)) or vol_spike:
            self.captured = True
            direction = "BULLISH" if candle["close"] > candle["open"] else "BEARISH"
            self.impulse = {"high": candle["high"], "low": candle["low"], "direction": direction, "range": range_size, "volume": candle["volume"], "timestamp": candle["ts"]}
            logger.debug(f"🔥 OPENING VIOLENCE DETECTED {direction} | Range: {range_size:.2f}")
            return self.impulse
        return None

class LiquidityVacuumDetector:
    def __init__(self):
        self.triggered = False; self.session_date = None

    def check(self, bq: int, aq: int, vol: int, avg_vol: float, ts: float) -> Optional[str]:
        now = datetime.fromtimestamp(ts)
        if self.session_date != now.date():
            self.triggered = False; self.session_date = now.date()
            
        time_val = now.hour * 100 + now.minute
        if self.triggered or time_val > 923: return None

        if avg_vol > 0 and vol > (avg_vol * 2.5):
            if aq > 0 and bq > (aq * 15):
                self.triggered = True; return "CALL"
            elif bq > 0 and aq > (bq * 15):
                self.triggered = True; return "PUT"
        return None

class LiquidityTrapDetector:
    @staticmethod
    def detect(smc_analyzer, price: float) -> Optional[str]:
        sweep = smc_analyzer.active_sweep
        if not sweep: return None
        level = sweep["price"]
        if sweep["type"] == "BEARISH" and price < level:
            logger.debug("🪤 BEAR TRAP REVERSAL CONFIRMED")
            return "PUT"
        if sweep["type"] == "BULLISH" and price > level:
            logger.debug("🪤 BULL TRAP REVERSAL CONFIRMED")
            return "CALL"
        return None

class RegimeFilter:
    @staticmethod
    def is_tradeable(config: AlphaConfig) -> bool:
        now = datetime.now().time()
        if now.hour == 9 and now.minute < config.MORNING_LOCK_MIN: return False
        if (now.hour > config.AFTERNOON_LOCK_HOUR) or (now.hour == config.AFTERNOON_LOCK_HOUR and now.minute >= config.AFTERNOON_LOCK_MIN): return False
        return True

class ATRFilter:
    def __init__(self, period=14):
        self.period = period; self.tr_values = []; self.prev_close = None
    def update(self, candle: dict):
        tr = max(candle["high"] - candle["low"], abs(candle["high"] - self.prev_close), abs(candle["low"] - self.prev_close)) if self.prev_close else candle["high"] - candle["low"]
        self.prev_close = candle["close"]
        self.tr_values.append(tr)
        if len(self.tr_values) > self.period: self.tr_values.pop(0)
    def is_tradeable(self, spot: float, config: AlphaConfig) -> bool:
        if not self.tr_values or spot <= 0: return False
        atr_pct = (sum(self.tr_values) / len(self.tr_values)) / spot
        return config.MIN_ATR_PCT <= atr_pct <= config.MAX_ATR_PCT

class VWAPFilter:
    def __init__(self):
        self.cum_pv = 0.0; self.cum_v = 0; self.session_day = None
    def update(self, price: float, vol: int, ts: float):
        tick_day = datetime.fromtimestamp(ts).date()
        if self.session_day != tick_day:
            self.cum_pv = 0.0; self.cum_v = 0; self.session_day = tick_day
        self.cum_pv += (price * vol); self.cum_v += vol
    def bias(self, price: float) -> str:
        if self.cum_v == 0: return "NEUTRAL"
        vwap = self.cum_pv / self.cum_v
        return "BULLISH" if price > vwap else "BEARISH" if price < vwap else "NEUTRAL"

class TrendFilter:
    def __init__(self, period=20):
        self.multiplier = 2 / (period + 1); self.ema = None; self.prev_ema = None
    def update(self, close: float):
        if self.ema is None: self.ema = close
        else: self.prev_ema = self.ema; self.ema = (close - self.ema) * self.multiplier + self.ema
    def slope_ok(self, min_slope: float) -> bool:
        return self.prev_ema and self.ema and abs(self.ema - self.prev_ema) / self.prev_ema >= min_slope

class VolumeFilter:
    def __init__(self, period=20): 
        self.volumes = []
        self.period = period
        self.avg = 0.0 
        
    def update(self, vol: int):
        self.volumes.append(vol)
        if len(self.volumes) > self.period: self.volumes.pop(0)
        self.avg = sum(self.volumes) / max(len(self.volumes), 1)
        
    def is_spike(self, vol: int, mult: float) -> bool:
        if not self.volumes: return True 
        return vol > (self.avg * mult)

# ============================================================
# 3. CORE: INSTITUTIONAL SMC LOGIC (FRACTALS & IMBALANCE)
# ============================================================
class CandleBuilder:
    def __init__(self, symbol: str, timeframe_sec: int = 60):
        self.symbol = symbol; self.timeframe_sec = timeframe_sec
        self.current_candle = None; self.history = []

    def process_tick(self, ltp: float, vol: int, ts: float) -> Optional[dict]:
        if self.current_candle and ts < self.current_candle["ts"]: return None 
        candle_ts = (int(ts) // self.timeframe_sec) * self.timeframe_sec
        if not self.current_candle:
            self.current_candle = {"ts": candle_ts, "open": ltp, "high": ltp, "low": ltp, "close": ltp, "volume": vol}
            return None
        if candle_ts > self.current_candle["ts"]:
            closed = self.current_candle.copy()
            self.history.append(closed)
            if len(self.history) > 100: self.history.pop(0) 
            self.current_candle = {"ts": candle_ts, "open": ltp, "high": ltp, "low": ltp, "close": ltp, "volume": vol}
            return closed 
        self.current_candle["high"] = max(self.current_candle["high"], ltp)
        self.current_candle["low"] = min(self.current_candle["low"], ltp)
        self.current_candle["close"] = ltp
        self.current_candle["volume"] += vol
        return None

class SMCAnalyzer:
    def __init__(self, config: AlphaConfig):
        self.config = config
        self.candle_window = []
        self.swing_highs, self.swing_lows = [], []
        self.eqh_pools, self.eql_pools = [], []
        
        self.session_high, self.session_low, self.session_date = None, None, None
        self.pressure_ema = 0.0 
        self.pending_sweeps = [] 
        self.trend = "NEUTRAL"
        self.active_sweep = None 
        
        self.last_displacement = None
        self.last_displacement_type = "NEUTRAL"

    def _cluster_pools(self, swings: list, is_high: bool) -> list:
        recent = [s["price"] for s in swings[-30:]]
        clusters = []
        for p1 in recent:
            cluster = [p2 for p2 in recent if abs(p1 - p2) / p2 <= self.config.CLUSTER_TOLERANCE_PCT]
            if len(cluster) >= self.config.MIN_CLUSTER_SIZE:
                avg_price = sum(cluster) / len(cluster)
                if not any(abs(avg_price - c) / c <= self.config.CLUSTER_TOLERANCE_PCT for c in clusters):
                    clusters.append(avg_price)
        return clusters

    def update_structure(self, closed_candle: dict):
        self.candle_window.append(closed_candle)
        if len(self.candle_window) > 5: self.candle_window.pop(0)

        c_date = datetime.fromtimestamp(closed_candle["ts"]).date()
        if self.session_date != c_date:
            self.session_date = c_date
            self.session_high = closed_candle["high"]
            self.session_low = closed_candle["low"]
        else:
            self.session_high = max(self.session_high, closed_candle["high"])
            self.session_low = min(self.session_low, closed_candle["low"])

        body = abs(closed_candle["close"] - closed_candle["open"])
        range_size = closed_candle["high"] - closed_candle["low"]
        if range_size > 0 and body / range_size > 0.7:
            self.last_displacement = closed_candle
            self.last_displacement_type = "BULLISH" if closed_candle["close"] > closed_candle["open"] else "BEARISH"

        if len(self.candle_window) == 5:
            cw = self.candle_window
            if cw[2]["high"] > cw[0]["high"] and cw[2]["high"] > cw[1]["high"] and cw[2]["high"] > cw[3]["high"] and cw[2]["high"] > cw[4]["high"]:
                self.swing_highs.append({"price": cw[2]["high"], "ts": cw[2]["ts"]})
            if cw[2]["low"] < cw[0]["low"] and cw[2]["low"] < cw[1]["low"] and cw[2]["low"] < cw[3]["low"] and cw[2]["low"] < cw[4]["low"]:
                self.swing_lows.append({"price": cw[2]["low"], "ts": cw[2]["ts"]})

        if len(self.swing_highs) >= 2 and closed_candle["close"] > self.swing_highs[-2]["price"]: self.trend = "BULLISH" 
        if len(self.swing_lows) >= 2 and closed_candle["close"] < self.swing_lows[-2]["price"]: self.trend = "BEARISH" 

        self.eqh_pools = self._cluster_pools(self.swing_highs, True)
        self.eql_pools = self._cluster_pools(self.swing_lows, False)
        
        if self.session_high and not any(abs(self.session_high - c)/c <= self.config.CLUSTER_TOLERANCE_PCT for c in self.eqh_pools):
            self.eqh_pools.append(self.session_high)
        if self.session_low and not any(abs(self.session_low - c)/c <= self.config.CLUSTER_TOLERANCE_PCT for c in self.eql_pools):
            self.eql_pools.append(self.session_low)

    def detect_liquidity_sweep(self, ltp: float, vol: int, vol_filter: VolumeFilter, bid: float, ask: float):
        now = time.time()
        if self.active_sweep and (now - self.active_sweep["ts"] > self.config.SWEEP_DECAY_SEC): self.active_sweep = None

        mid = (ask + bid) / 2.0
        tick_pressure = vol if ltp >= mid else -vol
        self.pressure_ema = (tick_pressure * 0.2) + (self.pressure_ema * 0.8)

        is_spike = vol_filter.is_spike(vol, self.config.MIN_VOL_SPIKE_MULTIPLIER)

        # ✅ FIX 3: Safe Division (Zero Guard)
        for eqh in list(self.eqh_pools):
            if ltp > 0:
                dist = (eqh - ltp) / ltp
                if 0 < dist < self.config.PRE_SWEEP_PROXIMITY and self.pressure_ema > (vol * 1.5):
                    self.pending_sweeps.append({"type": "BEARISH", "pool": eqh, "ts": now})
                elif ltp > eqh: 
                    if is_spike: self.pending_sweeps.append({"type": "BEARISH", "pool": eqh, "ts": now})
                    self.eqh_pools.remove(eqh) 
                
        for eql in list(self.eql_pools):
            if eql > 0:
                dist = (ltp - eql) / eql
                if 0 < dist < self.config.PRE_SWEEP_PROXIMITY and self.pressure_ema < -(vol * 1.5):
                    self.pending_sweeps.append({"type": "BULLISH", "pool": eql, "ts": now})
                elif ltp < eql:
                    if is_spike: self.pending_sweeps.append({"type": "BULLISH", "pool": eql, "ts": now})
                    self.eql_pools.remove(eql)
                
        if len(self.pending_sweeps) > self.config.MAX_PENDING_SWEEPS:
            self.pending_sweeps = self.pending_sweeps[-self.config.MAX_PENDING_SWEEPS:]

        for pending in list(self.pending_sweeps):
            if now - pending["ts"] > 30.0:
                self.pending_sweeps.remove(pending)
                continue
            if now - pending["ts"] >= self.config.SWEEP_CONFIRM_DELAY_SEC:
                if pending["type"] == "BEARISH" and ltp > pending["pool"]:
                    self.active_sweep = {"type": "BEARISH", "price": pending["pool"], "ts": now}
                elif pending["type"] == "BULLISH" and ltp < pending["pool"]:
                    self.active_sweep = {"type": "BULLISH", "price": pending["pool"], "ts": now}
                self.pending_sweeps.remove(pending)

# ============================================================
# 4. SMART RISK & ROUTING
# ============================================================
class StrikeSelector:
    def __init__(self, engine):
        self.engine = engine
        self.config = engine.config
        self.cache = collections.OrderedDict() 
        self.cache_ts = {}

    async def get_optimal_contract(self, symbol: str, spot: float, direction: str, sys_time: float) -> dict:
        cache_key = f"{symbol}:{direction}:{round(spot)}"
        if cache_key in self.cache and (sys_time - self.cache_ts.get(cache_key, 0)) < 2.0:
            self.cache.move_to_end(cache_key); return self.cache[cache_key]

        step = {"NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50, "MIDCPNIFTY": 25}.get(symbol.upper(), 50)
        atm = round(spot / step) * step
        opt_type = "CE" if direction == "CALL" else "PE"
        
        strike_matrix = [atm - (step*2), atm - step, atm, atm + step, atm + (step*2)]
        best_contract, best_score = None, -float('inf')
        
        for strike in strike_matrix:
            c_data = await self.engine._get_contract_map(f"contract_map:{symbol.upper()}:{opt_type}:{strike}")
            if c_data:
                scrip = int(c_data['scrip_code'])
                q_data = await self.engine._get_cached_quote(scrip, sys_time)
                ask, bid = (float(q_data.get("ask", 0)), float(q_data.get("bid", 0))) if q_data else (120.0, 119.5)
                spread, mid = ask - bid, (ask + bid) / 2.0 
                
                distance_pct = (spot - strike) / spot if direction == "CALL" else (strike - spot) / spot
                mock_delta = 0.5 + (distance_pct * 10) 
                mock_delta = max(0.1, min(0.9, mock_delta))
                
                if mid > 0 and (spread / mid) <= self.config.MAX_SPREAD_PCT:
                    delta_penalty = abs(0.5 - mock_delta) 
                    score = (1 / (spread/mid + 0.0001)) - (delta_penalty * 1000)
                    
                    if score > best_score:
                        best_score = score
                        best_contract = {"strike": strike, "option_type": opt_type, "scrip_code": scrip, "lot_size": int(c_data["lot_size"]), "expiry": c_data["expiry"], "mock_premium": mid, "mock_delta": mock_delta}
                        
        if best_contract:
            self.cache[cache_key] = best_contract; self.cache_ts[cache_key] = sys_time; self.cache.move_to_end(cache_key)
            if len(self.cache) > 100: self.cache_ts.pop(self.cache.popitem(last=False)[0], None)
        return best_contract

class SignalScorer:
    @staticmethod
    def score(smc_1m, smc_5m, vwap_bias: str, atr_ok: bool, trend_ok: bool, micro_signals: list) -> float:
        score = 0.0
        if smc_1m.trend == "BULLISH" and smc_5m.trend == "BULLISH": score += 0.3
        elif smc_1m.trend == "BEARISH" and smc_5m.trend == "BEARISH": score += 0.3
        if smc_1m.active_sweep: score += 0.2
        if atr_ok: score += 0.2
        if trend_ok: score += 0.15
        if vwap_bias != "NEUTRAL": score += 0.15
        
        if smc_1m.last_displacement_type == "BULLISH" and smc_1m.trend == "BULLISH": score += 0.1
        elif smc_1m.last_displacement_type == "BEARISH" and smc_1m.trend == "BEARISH": score += 0.1
        
        if "ICEBERG_ABSORPTION" in micro_signals: score += 0.10
        if "ASK_SPOOFING" in micro_signals and smc_1m.trend == "BULLISH": score += 0.05
        if "BID_SPOOFING" in micro_signals and smc_1m.trend == "BEARISH": score += 0.05
        
        return min(score, 1.0)

class PositionSizer:
    @staticmethod
    def calculate(dynamic_capital: float, risk_pct: float, premium: float, lot_size: int, margin: float, conf: float, config: AlphaConfig) -> int:
        if premium <= 0: return 0
        dyn_risk_pct = risk_pct * min(1.7, 1.0 + ((conf - config.MIN_SIGNAL_CONFIDENCE) / (1.0 - config.MIN_SIGNAL_CONFIDENCE)))
        risk_per_lot = premium * lot_size * config.PREMIUM_RISK_MULTIPLIER
        if risk_per_lot == 0: return 0
        
        max_risk = dynamic_capital * dyn_risk_pct
        allowed_lots = int(max_risk // risk_per_lot)
        affordable_lots = int(margin // (premium * lot_size)) 
        
        lots = min(allowed_lots, affordable_lots)
        return lots * lot_size

# ============================================================
# 5. MARKET FEED INGESTOR
# ============================================================
class MarketFeedIngestor:
    def __init__(self, client: FivePaisaClient, config: AlphaConfig, jwt_token: str, client_code: str):
        self.client = client
        self.config = config
        self.redis = aioredis.from_url(config.REDIS_URL, decode_responses=True, max_connections=30)
        self._running = False
        self.scrip_symbol_map = {} 
        self.spot_scrip_codes = [999920000, 999920005]

    async def sync_scrip_master(self):
        self.scrip_symbol_map[999920000] = "NIFTY"
        self.scrip_symbol_map[999920005] = "BANKNIFTY"
        mock_csv = [
            {"Root": "NIFTY", "Series": "CE", "StrikeRate": 24650, "ScripCode": 57633, "MinimumLot": 25, "Expiry": datetime.now().strftime("%Y-%m-%d")},
            {"Root": "BANKNIFTY", "Series": "PE", "StrikeRate": 52400, "ScripCode": 60122, "MinimumLot": 15, "Expiry": "2026-03-26"},
        ]
        pipe = self.redis.pipeline()
        for row in mock_csv:
            if row["Root"] in self.config.TRADEABLE_SYMBOLS:
                pipe.hset(f"contract_map:{row['Root']}:{row['Series']}:{int(row['StrikeRate'])}", 
                          mapping={"scrip_code": row["ScripCode"], "lot_size": row["MinimumLot"], "expiry": row["Expiry"]})
        try:
            await pipe.execute()
        except Exception: pass

    async def stream_websocket_ticks(self):
        while self._running:
            try:
                buffer = []; last_flush = time.time()
                while self._running:
                    await asyncio.sleep(0.01) 
                    scrip_code = random.choice(self.spot_scrip_codes)
                    symbol = self.scrip_symbol_map.get(scrip_code)
                    if not symbol: continue
                    ltp = round((24650.0 if symbol == "NIFTY" else 52400.0) + random.uniform(-5, 5), 2)
                    bq, aq = random.randint(100, 5000), random.randint(100, 5000)
                    
                    tick = {
                        "symbol": symbol, "ltp": ltp, "vol": random.randint(10, 1000),
                        "bid": round(ltp - 0.5, 2), "ask": round(ltp + 0.5, 2), 
                        "bq": bq, "aq": aq, "timestamp": time.time()
                    }
                    buffer.append(tick)
                    
                    if len(buffer) >= 50 or (time.time() - last_flush > 0.1 and buffer):
                        pipe = self.redis.pipeline()
                        for tick_data in buffer: 
                            pipe.xadd(f"acct:{self.config.ACCOUNT_ID}:live_ltp_stream", {"data": orjson.dumps(tick_data)}, maxlen=500000, approximate=True)
                            
                        last_tick = buffer[-1]
                        opt_scrip = 57633 if last_tick["symbol"] == "NIFTY" else 60122
                        opt_ltp = 120.0 + random.uniform(-5, 5)
                        pipe.hset(f"quote:{opt_scrip}", mapping={
                            "bid": round(opt_ltp - 0.5, 2), "ask": round(opt_ltp + 0.5, 2), "ltp": round(opt_ltp, 2),
                            "oi": random.randint(10000, 500000), "gamma": random.uniform(0.001, 0.02)
                        })
                        
                        # ✅ FIX 9: Redis Pipeline Flush Crash Guard
                        try:
                            await pipe.execute()
                        except Exception: pass
                        
                        buffer.clear(); last_flush = time.time()
            except Exception: await asyncio.sleep(5)

    async def run(self):
        self._running = True; await self.sync_scrip_master(); await self.stream_websocket_ticks()
    async def stop(self):
        self._running = False
        # ✅ FIX 2: Proper Asyncio Shutdown for Redis 5.x+
        await self.redis.aclose()


# ============================================================
# 6. THE MAHORAGA ALPHA ENGINE ORCHESTRATOR 
# ============================================================
class AlphaEngine:
    def __init__(self, config: AlphaConfig, client: FivePaisaClient):
        self.config = config
        self.client = client
        
        # ✅ FIX 8: ThreadPoolExecutor Safety and Observability
        self.executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="hydra_margin_pool")
        
        self.redis = aioredis.from_url(config.REDIS_URL, encoding="utf-8", decode_responses=True, max_connections=30)
        self.strike_selector = StrikeSelector(self)
        
        self.tick_queues = {sym: asyncio.Queue(maxsize=10000) for sym in self.config.TRADEABLE_SYMBOLS}
        
        self.quote_cache = collections.defaultdict(dict)
        self.quote_cache_ts = collections.defaultdict(float)
        self.contract_cache = {}

        self.mode = "OPENING"
        self.opening_trade_active = {sym: False for sym in self.config.TRADEABLE_SYMBOLS} 
        
        self.opening_scalper, self.opening_trade, self.liquidity_vacuum = {}, {}, {}
        self.heatmap_engine, self.gamma_detector, self.micro_analyzer, self.ai_predictor = {}, {}, {}, {}
        self.void_detector = {}
        
        self.candles, self.smc, self.atr, self.vwap, self.trend, self.volume = {}, {}, {}, {}, {}, {}
        self.opening_impulse = {} 
        self._running = False; self._tasks = []
        
        self.cached_margin, self.last_margin_update = 0.0, 0.0
        self.trades_today, self.open_risk, self.daily_net_pnl = 0, 0.0, 0.0
        self.dynamic_capital = self.config.CAPITAL
        self.loss_streak = 0 

    async def _run_forever(self, coro_func, *args):
        while self._running:
            try:
                await coro_func(*args)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"🔥 Task crashed: {e} | Restarting...")
                await asyncio.sleep(1)

    async def _safe_redis_write(self, fn, *args, **kwargs):
        for _ in range(3):
            try: return await fn(*args, **kwargs)
            except Exception: await asyncio.sleep(0.2)
        return None

    # ✅ FIX 4: Safe Redis hgetall None Handling
    async def _get_cached_quote(self, scrip_code: int, sys_time: float) -> dict:
        if sys_time - self.quote_cache_ts[scrip_code] > 1.0:
            data = await self.redis.hgetall(f"quote:{scrip_code}")
            self.quote_cache[scrip_code] = data or {}
            self.quote_cache_ts[scrip_code] = sys_time
        return self.quote_cache[scrip_code]

    async def _get_contract_map(self, key: str) -> dict:
        if key not in self.contract_cache:
            data = await self.redis.hgetall(key)
            self.contract_cache[key] = data or {}
        return self.contract_cache[key]

    async def evaluate_signal_matrix(self, symbol: str, spot: float, bid: float, ask: float, micro_signals: list, clusters: dict, pred: float, sys_time: float):
        if self.loss_streak >= self.config.MAX_CONSECUTIVE_LOSSES: return 
        smc_1m, smc_5m, smc_15m = self.smc[symbol]["1m"], self.smc[symbol]["5m"], self.smc[symbol]["15m"]
        if self.daily_net_pnl <= -self.config.MAX_DAILY_LOSS or self.trades_today >= self.config.MAX_TRADES_PER_DAY: return
        if not RegimeFilter.is_tradeable(self.config): return
        spot_mid = (ask + bid) / 2.0
        if spot_mid > 0 and ((ask - bid) / spot_mid) > self.config.MAX_SPREAD_PCT: return

        pin_strike = self.gamma_detector[symbol].get_pinning_strike()
        if pin_strike and abs(spot - pin_strike) / spot < 0.001:
            logger.debug(f"🧲 Gamma Pin Zone at {pin_strike}. Trade skipped.")
            return

        signal_dir = LiquidityTrapDetector.detect(smc_1m, spot)

        if not signal_dir:
            if not smc_1m.active_sweep: return
            sweep = smc_1m.active_sweep
            if sweep["type"] == "BEARISH" and smc_1m.trend == "BEARISH" and smc_5m.trend in ["BEARISH", "NEUTRAL"]:
                if smc_15m.trend != "BULLISH": signal_dir = "PUT"
            elif sweep["type"] == "BULLISH" and smc_1m.trend == "BULLISH" and smc_5m.trend in ["BULLISH", "NEUTRAL"]:
                 if smc_15m.trend != "BEARISH": signal_dir = "CALL"

        if signal_dir:
            if pred and abs(pred - spot) / spot < 0.0015:
                logger.debug(f"🧲 Predicted Institutional Trap Zone Confirmed at {pred:.2f} for {symbol}")
                
            if signal_dir == "CALL" and clusters["resistance_clusters"]:
                logger.debug(f"🎯 CALL Signal targeting Top-Side Liquidity Cluster at {clusters['resistance_clusters'][0]}")
            elif signal_dir == "PUT" and clusters["support_clusters"]:
                logger.debug(f"🎯 PUT Signal targeting Down-Side Liquidity Cluster at {clusters['support_clusters'][0]}")

            score = SignalScorer.score(smc_1m, smc_5m, self.vwap[symbol].bias(spot), self.atr[symbol].is_tradeable(spot, self.config), self.trend[symbol].slope_ok(self.config.MIN_TREND_SLOPE), micro_signals)
            if score < self.config.MIN_SIGNAL_CONFIDENCE: return

            contract = await self.strike_selector.get_optimal_contract(symbol, spot, signal_dir, sys_time)
            if not contract: return
            
            risk_pct = self.config.RISK_PER_TRADE_PCT
            if contract.get("expiry") == datetime.now().strftime("%Y-%m-%d") and datetime.now().time().hour >= self.config.GAMMA_EXPLOSION_HOUR:
                risk_pct *= self.config.GAMMA_RISK_REDUCTION
            
            if not await self._safe_redis_write(self.redis.set, f"{self.config.ACCOUNT_ID}:lock:{symbol}:{contract['strike']}:{signal_dir}", "1", ex=self.config.SIGNAL_COOLDOWN_SEC, nx=True): return
            
            if sys_time - self.last_margin_update > 10.0:
                try:
                    resp = await asyncio.get_running_loop().run_in_executor(self.executor, self.client.margin)
                    # ✅ FIX 6: Robust Margin Response Parsing
                    if resp and isinstance(resp, list) and len(resp) > 0:
                        self.cached_margin = float(resp[0].get("NetAvailableMargin", 0.0))
                    self.last_margin_update = sys_time
                except: pass
            
            qty = PositionSizer.calculate(self.dynamic_capital, risk_pct, contract["mock_premium"], contract["lot_size"], self.cached_margin, score, self.config)
            trade_risk = qty * contract["mock_premium"] * self.config.PREMIUM_RISK_MULTIPLIER
            
            if qty > 0 and (self.open_risk + trade_risk) <= self.config.MAX_OPEN_RISK:
                sl_price = smc_1m.active_sweep["price"] if smc_1m.active_sweep else spot
                payload = {
                    "action": f"BUY_{signal_dir}", "symbol": f"{symbol}{contract['strike']}{contract['option_type']}",
                    "scrip_code": contract["scrip_code"], "position_size_qty": qty, "confidence": score, 
                    "spot_entry": spot, "spot_sl": sl_price * (1.002 if signal_dir == "PUT" else 0.998),
                    "option_premium": contract["mock_premium"], "indep_delta": contract.get("mock_delta", 0.5),
                    "trade_specs": {"lot_size": contract["lot_size"]}, "timestamp": sys_time
                }
                await self._safe_redis_write(self.redis.xadd, f"acct:{self.config.ACCOUNT_ID}:signals:alpha", payload)
                self.trades_today += 1; self.open_risk += trade_risk
                logger.info(f"🎯 TRADE ENTRY {payload['action']} | {payload['symbol']} | Score: {score*100:.1f}%")
                
                if smc_1m.active_sweep:
                    self.ai_predictor[symbol].learn(smc_1m.active_sweep["price"], spot)
                smc_1m.active_sweep = None 

    async def execution_feedback_loop(self):
        stream_key = f"acct:{self.config.ACCOUNT_ID}:execution:feedback"
        try: await self.redis.xgroup_create(stream_key, "alpha_feedback_group", id="$", mkstream=True)
        except Exception: pass
        
        while self._running:
            try:
                streams = await self.redis.xreadgroup("alpha_feedback_group", "alpha_listener", {stream_key: ">"}, count=50, block=2000)
                if streams:
                    pipe = self.redis.pipeline()
                    for _, messages in streams:
                        for msg_id, raw_data in messages:
                            action = raw_data.get("action")
                            if action == "CLOSED":
                                net_pnl = float(raw_data.get("pnl", 0.0))
                                self.daily_net_pnl += net_pnl
                                self.dynamic_capital += net_pnl
                                logger.info(f"💸 TRADE EXIT | PnL: {net_pnl:.2f} | Daily Net: {self.daily_net_pnl:.2f}")
                                if net_pnl < 0:
                                    self.loss_streak += 1
                                    if self.loss_streak >= self.config.MAX_CONSECUTIVE_LOSSES:
                                        logger.warning("🚨 KILL SWITCH ACTIVATED. MAX CONSECUTIVE LOSSES REACHED.")
                                else:
                                    self.loss_streak = 0
                                self.open_risk = max(0.0, self.open_risk - float(raw_data.get("capital_released", 0.0)))
                            pipe.xack(stream_key, "alpha_feedback_group", msg_id)
                    try:
                        await pipe.execute()
                    except Exception: pass
            except Exception: await asyncio.sleep(0.5)

    async def _strategy_worker(self, symbol: str):
        queue = self.tick_queues[symbol]
        
        self.candles[symbol] = {"1m": CandleBuilder(symbol, 60), "3m": CandleBuilder(symbol, 180), "5m": CandleBuilder(symbol, 300), "15m": CandleBuilder(symbol, 900)}
        self.smc[symbol] = {k: SMCAnalyzer(self.config) for k in ["1m", "3m", "5m", "15m"]}
        self.atr[symbol], self.vwap[symbol], self.trend[symbol], self.volume[symbol] = ATRFilter(), VWAPFilter(), TrendFilter(), VolumeFilter()
        self.opening_impulse[symbol] = OpeningImpulseDetector()
        self.opening_scalper[symbol] = OpeningScalper()
        self.liquidity_vacuum[symbol] = LiquidityVacuumDetector()
        
        self.heatmap_engine[symbol] = LiquidityHeatmapEngine()
        self.gamma_detector[symbol] = GammaExposureMap() 
        self.micro_analyzer[symbol] = MicrostructureAnalyzer()
        self.ai_predictor[symbol] = AILiquidityPredictor()
        self.void_detector[symbol] = LiquidityVoidDetector()
        self.opening_trade[symbol] = None
        
        while self._running:
            sys_time = time.time()
            today = datetime.fromtimestamp(sys_time).date()
            if getattr(self, "session_day", None) != today:
                self.session_day = today
                self.trades_today = 0
                self.daily_net_pnl = 0
                self.loss_streak = 0

            batch = [await queue.get()]
            while not queue.empty():
                try: batch.append(queue.get_nowait())
                except asyncio.QueueEmpty: break
            
            try:
                for tick in batch:
                    ltp, ts = tick["ltp"], tick["timestamp"]
                    bid, ask, vol = tick["bid"], tick["ask"], tick["vol"]
                    bq, aq = tick.get("bq", 1000), tick.get("aq", 1000)
                    
                    # ✅ FIX 3: Corrupted Market Data Validation Guard
                    if ltp <= 0 or bid <= 0 or ask <= 0 or bid > ask:
                        continue
                    vol = max(vol, 1)
                    
                    self.vwap[symbol].update(ltp, vol, ts)
                    self.heatmap_engine[symbol].update(ltp, vol)
                    
                    c_candle = self.candles[symbol]["1m"].current_candle
                    current_1m_vol = c_candle["volume"] if c_candle else vol
                    self.smc[symbol]["1m"].detect_liquidity_sweep(ltp, current_1m_vol, self.volume[symbol], bid, ask)

                    impulse_signal = None
                    for tf_label, builder in self.candles[symbol].items():
                        closed_candle = builder.process_tick(ltp, vol, ts)
                        if closed_candle: 
                            self.smc[symbol][tf_label].update_structure(closed_candle)
                            if tf_label == "1m":
                                self.atr[symbol].update(closed_candle)
                                self.trend[symbol].update(closed_candle["close"])
                                self.volume[symbol].update(closed_candle["volume"])
                                
                                # ✅ FIX 5: Safe ATR Math Extracted
                                atr_val = (sum(self.atr[symbol].tr_values) / len(self.atr[symbol].tr_values)) if self.atr[symbol].tr_values else 0
                                
                                impulse = self.opening_impulse[symbol].check(closed_candle, self.atr[symbol], self.volume[symbol])
                                if impulse and not self.opening_trade_active[symbol] and self.mode == "OPENING":
                                    impulse_signal = "CALL" if impulse["direction"] == "BULLISH" else "PUT"
                                
                                void_sig = self.void_detector[symbol].check(closed_candle, self.volume[symbol].avg, atr_val)
                                if void_sig: logger.debug(f"🕳️ Reversion expected for {symbol}")
                
                last_tick = batch[-1]
                ltp, ts = last_tick["ltp"], last_tick["timestamp"]
                bid, ask, vol = last_tick["bid"], last_tick["ask"], last_tick["vol"]
                bq, aq = last_tick.get("bq", 1000), last_tick.get("aq", 1000)

                tick_time = datetime.fromtimestamp(ts)
                if tick_time.hour == 9 and tick_time.minute >= 23 and self.mode == "OPENING":
                    self.mode = "NORMAL"

                step = 50 if "NIFTY" in symbol else 100
                atm = round(ltp / step) * step
                for strike in [atm - step, atm, atm + step]:
                    for opt_type in ["CE", "PE"]:
                        c_data = await self._get_contract_map(f"contract_map:{symbol}:{opt_type}:{strike}")
                        if c_data:
                            q_data = await self._get_cached_quote(int(c_data['scrip_code']), sys_time)
                            if q_data:
                                oi = int(q_data.get("oi", 0)) 
                                gamma = float(q_data.get("gamma", 0.005))
                                if oi > 0: self.gamma_detector[symbol].update(strike, oi, gamma, ltp, ts)
                
                # ✅ FIX 1: Extracted Safe c_candle extraction
                c_candle = self.candles[symbol]["1m"].current_candle
                candle_range = (c_candle["high"] - c_candle["low"]) if c_candle else 0.01
                micro_signals = self.micro_analyzer[symbol].analyze(ltp, bq, aq, vol, self.volume[symbol].avg, candle_range)
                
                if self.opening_trade.get(symbol):
                    trade = self.opening_trade[symbol]
                    q_data = await self._get_cached_quote(trade['scrip_code'], sys_time)
                    if q_data:
                        current_premium = (float(q_data.get("ask", 0)) + float(q_data.get("bid", 0))) / 2.0
                        reason = None
                        if current_premium > 0:
                            if current_premium >= trade["target_premium"]:
                                trade["stop_premium"] = current_premium * 0.92
                                trade["target_premium"] = current_premium * 1.50 
                                logger.debug(f"📈 TRAILING STOP UPDATED on {symbol} | New Stop: {trade['stop_premium']:.2f}")
                            elif current_premium <= trade["stop_premium"]:
                                reason = "OPENING_STOPLOSS" if trade["stop_premium"] < trade["entry_premium"] else "TRAILING_STOP_HIT"
                        
                        if reason:
                            logger.info(f"💰 MODULE6 EXIT {reason} on {symbol} | Premium: {current_premium:.2f}")
                            await self._safe_redis_write(self.redis.xadd, f"acct:{self.config.ACCOUNT_ID}:signals:alpha", {
                                "action": "CLOSE_POSITION", "scrip_code": trade["scrip_code"], "reason": reason, "timestamp": sys_time
                            })
                            self.opening_trade[symbol] = None
                            self.opening_trade_active[symbol] = False

                if self.mode == "OPENING":
                    if self.opening_trade_active[symbol] or self.loss_streak >= self.config.MAX_CONSECUTIVE_LOSSES:
                        pass
                    elif self.daily_net_pnl > -self.config.MAX_DAILY_LOSS and self.trades_today < self.config.MAX_TRADES_PER_DAY:
                        signal = impulse_signal
                        if not signal: signal = self.opening_scalper[symbol].check(ltp, ts, vol, self.atr[symbol].prev_close, self.volume[symbol])
                        if not signal: signal = self.liquidity_vacuum[symbol].check(bq, aq, vol, self.volume[symbol].avg, ts)
                        
                        if signal:
                            contract = await self.strike_selector.get_optimal_contract(symbol, ltp, signal, sys_time)
                            if contract:
                                self.opening_trade_active[symbol] = True
                                qty = PositionSizer.calculate(self.dynamic_capital, self.config.RISK_PER_TRADE_PCT, contract["mock_premium"], contract["lot_size"], 100000, 0.8, self.config)
                                if qty > 0:
                                    logger.info(f"⚡ OPENING SCALP ENTRY {symbol} {signal} | Qty: {qty}")
                                    payload = {
                                        "action": f"BUY_{signal}", "symbol": f"{symbol}{contract['strike']}{contract['option_type']}",
                                        "scrip_code": contract["scrip_code"], "position_size_qty": qty, "confidence": 0.8,
                                        "spot_entry": ltp, "spot_sl": ltp * 0.998 if signal == "CALL" else ltp * 1.002,
                                        "option_premium": contract["mock_premium"], "indep_delta": contract.get("mock_delta", 0.5),
                                        "trade_specs": {"lot_size": contract["lot_size"]}, "timestamp": sys_time
                                    }
                                    
                                    self.opening_trade[symbol] = {
                                        "symbol": symbol, "scrip_code": contract["scrip_code"], "qty": qty,
                                        "entry_premium": contract["mock_premium"], "direction": signal,
                                        "target_premium": contract["mock_premium"] * 1.25, "stop_premium": contract["mock_premium"] * 0.85
                                    }
                                    await self._safe_redis_write(self.redis.xadd, f"acct:{self.config.ACCOUNT_ID}:signals:alpha", payload)
                                    self.trades_today += 1
                                    self.open_risk += contract["mock_premium"] * qty * self.config.PREMIUM_RISK_MULTIPLIER

                elif self.mode == "NORMAL":
                    atr_val = (sum(self.atr[symbol].tr_values) / len(self.atr[symbol].tr_values)) if self.atr[symbol].tr_values else 0
                    pred = self.ai_predictor[symbol].predict_next_trap(ltp, self.smc[symbol]["1m"].trend)
                    clusters = self.heatmap_engine[symbol].get_stop_clusters(ltp, atr_val)
                    await self.evaluate_signal_matrix(symbol, ltp, bid, ask, micro_signals, clusters, pred, sys_time)
                    
            except Exception as e:
                logger.error(f"Worker Error [{symbol}]: {e}")
            finally:
                # ✅ FIX 5: Safe Queue Draining Method 
                for _ in batch: 
                    queue.task_done()

    async def _redis_consumer(self):
        try: await self.redis.xgroup_create(f"acct:{self.config.ACCOUNT_ID}:live_ltp_stream", "alpha_group", id="$", mkstream=True)
        except: pass
        
        consumer_name = f"alpha_worker_{os.getpid()}"
        
        while self._running:
            try:
                streams = await self.redis.xreadgroup("alpha_group", consumer_name, {f"acct:{self.config.ACCOUNT_ID}:live_ltp_stream": ">"}, count=2000, block=10)
                if streams:
                    pipe = self.redis.pipeline() 
                    for _, messages in streams:
                        for msg_id, raw_data in messages:
                            data = orjson.loads(raw_data.get("data", "{}"))
                            # ✅ FIX 10: Null Symbol Guard Pre-Queue Distribution
                            symbol = data.get("symbol")
                            if not symbol: continue
                            
                            if symbol in self.config.TRADEABLE_SYMBOLS:
                                q = self.tick_queues[symbol]
                                # ✅ FIX 6: Non-Blocking Assured Queue Delivery
                                try:
                                    q.put_nowait(data)
                                except asyncio.QueueFull:
                                    try:
                                        q.get_nowait()
                                        q.put_nowait(data)
                                    except: pass
                            pipe.xack(f"acct:{self.config.ACCOUNT_ID}:live_ltp_stream", "alpha_group", msg_id)
                    try:
                        await pipe.execute()
                    except Exception: pass
            except Exception: await asyncio.sleep(0.1)

    async def start(self):
        self._running = True
        self._tasks.append(asyncio.create_task(self._run_forever(self._redis_consumer)))
        self._tasks.append(asyncio.create_task(self._run_forever(self.execution_feedback_loop)))
        for sym in self.config.TRADEABLE_SYMBOLS:
            self._tasks.append(asyncio.create_task(self._run_forever(self._strategy_worker, sym)))
        
        logger.info("🚀 Hydra Alpha Engine Online (Apex Institutional Architecture)")
        await asyncio.gather(*self._tasks)

    # ✅ FIX 9: Clean Engine Thread / Task Graceful Shutdown
    async def stop(self):
        logger.info("🛑 Initiating Graceful Shutdown...")
        self._running = False
        for t in self._tasks: t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self.executor.shutdown(wait=False)
        await self.redis.aclose()
        logger.info("✅ Engine Safely Offline.")

# ============================================================
# 6. UNIFIED ORCHESTRATOR
# ============================================================
async def main():
    config = AlphaConfig()
    client = FivePaisaClient(email="", passwd="", dob="")
    ingestor = MarketFeedIngestor(client, config, "MOCK", "MOCK")
    engine = AlphaEngine(config, client)
    
    try: 
        await asyncio.gather(ingestor.run(), engine.start())
    except KeyboardInterrupt: 
        await ingestor.stop()
        await engine.stop()

if __name__ == "__main__":
    asyncio.run(main())
