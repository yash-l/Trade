cat << 'EOF' > module2.py
import time
import logging
import asyncio
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from dataclasses import dataclass
from collections import OrderedDict
import redis.asyncio as aioredis
import orjson

logger = logging.getLogger("DynamicRouter")
logging.basicConfig(level=logging.WARNING, format='%(message)s')

IST = ZoneInfo("Asia/Kolkata")

@dataclass
class RouterConfig:
    STRIKE_INTERVALS = {
        "NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50, "MIDCPNIFTY": 25
    }
    LOT_SIZES = {
        "NIFTY": 25, "BANKNIFTY": 15, "FINNIFTY": 25, "MIDCPNIFTY": 50
    }
    FREEZE_LIMITS = {
        "NIFTY": 1800, "BANKNIFTY": 900, "FINNIFTY": 1800, "MIDCPNIFTY": 3000
    }
    STRIKE_BOUNDS = {
        "NIFTY": (15000, 30000), 
        "BANKNIFTY": (30000, 70000), 
        "FINNIFTY": (15000, 30000), 
        "MIDCPNIFTY": (8000, 15000)
    }
    MAX_STRIKE_DISTANCE_POINTS = {
        "NIFTY": 400, "BANKNIFTY": 1000, "FINNIFTY": 400, "MIDCPNIFTY": 200
    }
    
    MAX_ACCEPTABLE_SPREAD_PCT = 1.0  
    MAX_SPREAD_POINTS = 5.0          
    MIN_OI = 10000                   
    MIN_VOL = 100                    
    MIN_DEPTH = 100        
    MAX_IV = 80.0          
    EXPIRY_MIN_DTE = 2  
    
    NORM_VOLUME = 5000.0
    NORM_OI = 500000.0
    TARGET_SPREAD_DECIMAL = 0.02  
    TARGET_SLIPPAGE = 0.5  
    
    REDIS_TIMEOUT_SLA = 0.05         
    MAX_ROUTES_PER_SEC = 200

class RouterRateLimiter:
    def __init__(self, max_per_sec: int):
        self.max_per_sec = max_per_sec
        self.calls = 0
        self.window_start = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            now = time.monotonic()
            if now - self.window_start >= 1.0:
                self.calls = 0
                self.window_start = now
            self.calls += 1
            if self.calls > self.max_per_sec:
                raise RuntimeError(f"Rate Limit Exceeded: >{self.max_per_sec} routes/sec.")

class RouterCircuitBreaker:
    def __init__(self):
        self.failures = 0
        self.window_start = time.monotonic()
        self.is_tripped = False
        self.trip_time = 0.0

    def record_failure(self):
        now = time.monotonic()
        if self.is_tripped: return
        if now - self.window_start > 1.0:
            self.failures = 1
            self.window_start = now
        else:
            self.failures += 1
            if self.failures >= 5:
                self.is_tripped = True
                self.trip_time = now
                logger.critical(orjson.dumps({"event": "CIRCUIT_BREAKER_TRIPPED", "msg": "5s cooldown."}).decode('utf-8'))

    def check(self):
        if self.is_tripped:
            if time.monotonic() - self.trip_time > 5.0: 
                self.is_tripped = False
                self.failures = 0
                self.window_start = time.monotonic()
            else:
                raise RuntimeError("Router is locked by Circuit Breaker.")

class ATMLocator:
    def __init__(self, config: RouterConfig):
        self.config = config

    def get_atm(self, symbol: str, spot_price: float) -> int:
        interval = self.config.STRIKE_INTERVALS.get(symbol)
        if not interval: raise ValueError(f"Unknown symbol: {symbol}")
        return int(round(spot_price / interval) * interval)

class ExpiryHandler:
    def __init__(self, config: RouterConfig):
        self.config = config
        self._last_raw = []
        self._cached_parsed = []

    def choose_expiry(self, available_expiries: list[str], now_ist: datetime) -> str:
        if available_expiries != self._last_raw:
            parsed = [(datetime.strptime(x.split("T")[0], "%Y-%m-%d").date(), x) for x in available_expiries]
            parsed.sort(key=lambda item: item[0])
            self._cached_parsed = parsed
            self._last_raw = list(available_expiries)
            
        today_date = now_ist.date()
        for expiry_date, expiry_str in self._cached_parsed:
            dte = (expiry_date - today_date).days
            if dte < 0: continue
            
            if dte == 0 and now_ist.hour < 11: 
                return expiry_str
            if dte >= self.config.EXPIRY_MIN_DTE: 
                return expiry_str
                
        raise ValueError("No valid expiries found.")

class StrikeWindowGenerator:
    def __init__(self, config: RouterConfig):
        self.config = config

    def generate_window(self, symbol: str, atm: int, opt_type: str, vix: float) -> list[int]:
        interval = self.config.STRIKE_INTERVALS[symbol]
        
        if vix < 12.0: window_size = 2
        elif vix > 20.0: window_size = 4
        else: window_size = 3
        
        # SRE PATCH: Symmetrical Strike Window Fix
        offsets = list(range(-window_size, window_size + 1))
        strikes = [atm + (interval * offset) for offset in offsets]
        
        if opt_type == "PE":
            strikes.reverse()
            
        return strikes

class StrikeScoringEngine:
    def __init__(self, config: RouterConfig):
        self.config = config

    def calculate_score(self, strike: int, atm: int, symbol: str, opt_type: str, metrics: dict, confidence: float) -> float:
        interval = self.config.STRIKE_INTERVALS[symbol]
        
        liq_score = min(metrics["vol"] / self.config.NORM_VOLUME, 1.0)
        spread_decimal = metrics["spread_pct"] / 100.0 
        spread_score = max(0.0, 1.0 - min(spread_decimal / self.config.TARGET_SPREAD_DECIMAL, 1.0))
        oi_score = min(metrics["oi"] / self.config.NORM_OI, 1.0)
        
        strike_distance = abs(strike - atm)
        gamma_proxy = max(0.0, 1.0 - (strike_distance / (interval * 4))**2)
        dist_score = max(0.0, 1.0 - (strike_distance / (interval * 4)))
        
        min_depth = min(metrics["bq"], metrics["aq"])
        slippage_estimate = metrics["spread_pts"] / max(min_depth, 1)
        slippage_score = max(0.0, 1.0 - (slippage_estimate / self.config.TARGET_SLIPPAGE))
        
        is_itm = (strike < atm) if opt_type == "CE" else (strike > atm)
        delta_multiplier = 1.2 if (confidence >= 0.8 and is_itm) else 1.0
        
        total_score = (
            (liq_score * 0.25) + 
            (spread_score * 0.20) + 
            (slippage_score * 0.15) + 
            (oi_score * 0.15) + 
            (gamma_proxy * 0.15) + 
            (dist_score * 0.10)
        ) * delta_multiplier
        return round(total_score, 4)

class ContractResolver:
    def __init__(self, config: RouterConfig):
        self.config = config
        self._master_dict = {}
        self._tick_cache = OrderedDict() 
        self.tick_cache_max_size = 5000
        self.redis = None 

    def set_redis(self, redis_client: aioredis.Redis):
        self.redis = redis_client

    async def load_master(self):
        if not self.redis: return
        try:
            raw_mapping = await asyncio.wait_for(self.redis.hgetall("scripmaster:active_mapping"), timeout=1.0)
            if raw_mapping:
                self._master_dict = {k: int(v) for k, v in raw_mapping.items()}
                logger.info(f"Loaded {len(self._master_dict)} contracts into RAM.")
        except Exception as e:
            logger.warning(f"Failed to load scripmaster: {e}")

    def cleanup_ttl(self):
        now = time.time()
        while self._tick_cache:
            _, oldest_tick = next(iter(self._tick_cache.items()))
            if now - self._parse_redis_val(oldest_tick, 'ts') > 2.0:
                self._tick_cache.popitem(last=False)
            else:
                break

    async def get_scrip_code(self, symbol: str, expiry: str, strike: int, opt_type: str) -> int | None:
        contract_key = f"{symbol}:{expiry}:{strike}:{opt_type}"
        return self._master_dict.get(contract_key)

    def _parse_redis_val(self, data: dict, key_str: str, default=0.0):
        val = data.get(key_str) or data.get(key_str.encode('utf-8'))
        if val in (None, "", b""): return default
        try: return float(val)
        except ValueError: return default

    async def validate_liquidity(self, scrip_code: int, phase_multiplier: float) -> dict:
        if not self.redis: return {"valid": False, "reason": "redis_offline", "msg": "Redis offline."}
        
        try:
            is_blacklisted = await asyncio.wait_for(self.redis.exists(f"blacklist:{scrip_code}"), timeout=0.05)
            if is_blacklisted:
                return {"valid": False, "reason": "blacklisted", "msg": "Strike is quarantined."}
        except Exception:
            pass
        
        cached_tick = self._tick_cache.get(scrip_code)
        now = time.time()
        
        if cached_tick and (now - self._parse_redis_val(cached_tick, 'ts') < 0.1):
            tick_data = cached_tick
        else:
            stream_key = f"ticks:{scrip_code}"
            try:
                latest_ticks = await asyncio.wait_for(self.redis.xrevrange(stream_key, count=1), timeout=self.config.REDIS_TIMEOUT_SLA)
            except asyncio.TimeoutError:
                return {"valid": False, "reason": "timeout", "msg": "Redis SLA timeout"}
            
            if not latest_ticks: 
                if cached_tick and (now - self._parse_redis_val(cached_tick, 'ts') < 2.0): 
                    tick_data = cached_tick
                else: 
                    return {"valid": False, "reason": "no_data", "msg": "No live/fallback tick data."}
            else:
                tick_data = latest_ticks[0][1]
                self._tick_cache[scrip_code] = tick_data 
                self._tick_cache.move_to_end(scrip_code)
                if len(self._tick_cache) > self.tick_cache_max_size:
                    self._tick_cache.popitem(last=False)
            
        ts = self._parse_redis_val(tick_data, 'ts')
        if now - ts > 1.0: return {"valid": False, "reason": "stale_tick", "msg": "Stale tick."}
            
        ltp, bid, ask = self._parse_redis_val(tick_data, 'ltp'), self._parse_redis_val(tick_data, 'bid'), self._parse_redis_val(tick_data, 'ask')
        oi, vol = int(self._parse_redis_val(tick_data, 'oi')), int(self._parse_redis_val(tick_data, 'vol'))
        bq, aq = int(self._parse_redis_val(tick_data, 'bq')), int(self._parse_redis_val(tick_data, 'aq'))
        iv = float(self._parse_redis_val(tick_data, 'iv'))
        
        if ltp <= 0 or bid <= 0 or ask <= 0: return {"valid": False, "reason": "zero_value", "msg": "Zero-value tick."}
        if bid >= ask: 
            try: await asyncio.wait_for(self.redis.setex(f"blacklist:{scrip_code}", 30, "1"), timeout=0.05)
            except: pass
            return {"valid": False, "reason": "corrupt_book", "msg": "Corrupted Book. Blacklisted."}
        if not (bid <= ltp <= ask): return {"valid": False, "reason": "ltp_bounds", "msg": "LTP outside spread."}
            
        if bq < self.config.MIN_DEPTH or aq < self.config.MIN_DEPTH: return {"valid": False, "reason": "low_depth", "msg": "Insufficient depth."}
        if iv > self.config.MAX_IV: return {"valid": False, "reason": "high_iv", "msg": "IV exceeds safety threshold."}

        spread_pts = ask - bid
        max_dynamic_spread = max(self.config.MAX_SPREAD_POINTS, ltp * 0.02) * phase_multiplier
        if spread_pts > max_dynamic_spread: return {"valid": False, "reason": "spread_fail", "msg": f"Spread > dynamic limit."}
            
        spread_pct = (spread_pts / max(ltp, 0.05)) * 100
        if spread_pct > (self.config.MAX_ACCEPTABLE_SPREAD_PCT * phase_multiplier): return {"valid": False, "reason": "spread_fail", "msg": "Spread % > limit."}
        
        if oi < self.config.MIN_OI: return {"valid": False, "reason": "oi_fail", "msg": "Low OI."}
        if vol < self.config.MIN_VOL: return {"valid": False, "reason": "vol_fail", "msg": "Low Vol."}
            
        return {"valid": True, "spread_pct": round(spread_pct, 3), "spread_pts": spread_pts, "oi": oi, "vol": vol, "bq": bq, "aq": aq}

class RouterEngine:
    def __init__(self, primary_redis_url: str, failover_redis_url: str = None):
        self.primary_redis_url = primary_redis_url
        self.failover_redis_url = failover_redis_url or primary_redis_url
        self.active_redis_url = primary_redis_url
        
        self.redis = None
        self.config = RouterConfig()
        self.atm_locator = ATMLocator(self.config)
        self.expiry_handler = ExpiryHandler(self.config)
        self.window_generator = StrikeWindowGenerator(self.config)
        self.scoring_engine = StrikeScoringEngine(self.config)
        self.resolver = ContractResolver(self.config)
        
        self.circuit_breaker = RouterCircuitBreaker()
        self.rate_limiter = RouterRateLimiter(self.config.MAX_ROUTES_PER_SEC)
        self.holidays = self._load_holidays()
        
        self.last_valid_spot = None
        self.last_spot_time = 0.0

    def _load_holidays(self) -> list[str]:
        try:
            if os.path.exists("holidays.json"):
                with open("holidays.json", "rb") as f: return orjson.loads(f.read())
        except: pass
        return ["2026-01-26", "2026-03-30", "2026-04-14", "2026-05-01"]

    async def initialize(self):
        self.redis = await aioredis.from_url(self.active_redis_url, decode_responses=True, max_connections=500)
        self.resolver.set_redis(self.redis)
        await self.resolver.load_master()
        asyncio.create_task(self._redis_watchdog())

    async def warmup_router(self, current_spots: dict):
        for symbol, spot in current_spots.items():
            atm = self.atm_locator.get_atm(symbol, spot)
            for opt_type in ["CE", "PE"]:
                await self.resolver.get_scrip_code(symbol, "ACTIVE", atm, opt_type)
        logger.info("Router Cache Warmed Up.")

    async def _redis_watchdog(self):
        backoff = 1.0
        while True:
            try:
                if self.redis: await self.redis.ping()
                self.resolver.cleanup_ttl()
                await self.resolver.load_master() 
                backoff = 1.0
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(orjson.dumps({"event": "REDIS_FAIL", "error": str(e), "msg": f"Reconnecting in {backoff}s"}).decode('utf-8'))
                try: await self.redis.close()
                except: pass
                
                self.active_redis_url = self.failover_redis_url if self.active_redis_url == self.primary_redis_url else self.primary_redis_url
                try:
                    new_redis = await aioredis.from_url(self.active_redis_url, decode_responses=True, max_connections=500)
                    await asyncio.wait_for(new_redis.ping(), timeout=1.0)
                    self.redis = new_redis
                    self.resolver.set_redis(self.redis)
                    await self.resolver.load_master()
                except Exception as ex:
                    pass
                
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 60.0)

    def _get_session_phase_multiplier(self, now_ist: datetime) -> float:
        time_val = now_ist.hour * 100 + now_ist.minute
        if 915 <= time_val <= 925: return 2.0 
        if 1430 <= time_val <= 1530: return 0.5 
        return 1.0

    def _is_market_open(self, now_ist: datetime) -> bool:
        if now_ist.weekday() >= 5: return False 
        if now_ist.strftime("%Y-%m-%d") in self.holidays: return False
        return 915 <= (now_ist.hour * 100 + now_ist.minute) <= 1530

    async def _log_telemetry(self, status: str, latency: float):
        if not self.redis: return
        try:
            pipe = self.redis.pipeline(transaction=False)
            pipe.incr(f"router:{status}_count")
            if latency > 0: pipe.xadd("router:latency_stream", {"latency_ms": latency}, maxlen=10000, approximate=True)
            await asyncio.wait_for(pipe.execute(), timeout=0.1)
        except Exception: 
            pass

    def calculate_order_slices(self, symbol: str, quantity: int) -> list[int]:
        freeze = self.config.FREEZE_LIMITS.get(symbol.upper(), 1000)
        if quantity <= 0: return []
        if quantity <= freeze: return [quantity]
        
        parts = []
        while quantity > 0:
            chunk = min(quantity, freeze)
            parts.append(chunk)
            quantity -= chunk
        return parts

    async def _build_success_payload(self, symbol, expiry, target_strike, opt_type, scrip_code, metrics, score, start_time):
        execution_time_ms = (time.perf_counter() - start_time) * 1000
        
        # SRE PATCH: Latency Ladder (Warn/Alert instead of Crash)
        if execution_time_ms > 40.0:
            logger.error(f"CRITICAL LATENCY: Router took {execution_time_ms:.2f}ms. Investigate Redis/CPU stall.")
        elif execution_time_ms > 20.0:
            logger.warning(f"HIGH LATENCY: Router took {execution_time_ms:.2f}ms.")
            
        asyncio.create_task(self._log_telemetry("success", execution_time_ms))
        
        payload = {
            "status": "success", 
            "scrip_code": scrip_code,
            "contract_details": f"{symbol} {expiry} {target_strike} {opt_type}",
            "trade_specs": {"lot_size": self.config.LOT_SIZES.get(symbol, 1), "freeze_limit": self.config.FREEZE_LIMITS.get(symbol, 1000)},
            "metrics": metrics, 
            "confidence_score": score,
            "routing_time_ms": round(execution_time_ms, 3)
        }
        return payload

    async def route_signal(
        self, symbol: str, spot_price: float, spot_time: float, 
        opt_type: str, available_expiries: list[str], vix: float = 15.0, confidence: float = 0.5
    ) -> dict:
        if self.redis is None: return {"status": "error", "reason": "Redis offline."}
        start_time = time.perf_counter()
        
        try:
            if not symbol or not isinstance(symbol, str): raise ValueError("Invalid symbol.")
            symbol, opt_type = symbol.upper(), opt_type.upper()
            if opt_type not in ("CE", "PE"): raise ValueError(f"Invalid option type: {opt_type}. Must be CE or PE.")
            
            await self.rate_limiter.acquire()
            self.circuit_breaker.check()
            
            try:
                sys_ready = await asyncio.wait_for(self.redis.get("SYSTEM:READY"), timeout=0.1)
                sys_status = await asyncio.wait_for(self.redis.get("SYSTEM:STATUS"), timeout=0.1)
            except asyncio.TimeoutError:
                raise RuntimeError("Redis state check timeout.")
                
            if sys_ready != "YES": raise ValueError("System not initialized.")
            if sys_status != "OPEN": raise ValueError(f"Exchange status is not OPEN (Current: {sys_status}).")
            
            now_ist = datetime.now(IST)
            if not self._is_market_open(now_ist): raise ValueError("Outside market hours/Holiday.")
            if not available_expiries: raise ValueError("No expiries provided.")
                
            now = time.time()
            if now - spot_time > 2.0: raise ValueError("Spot price stale.")

            if self.last_valid_spot is not None:
                spot_move_pct = abs(spot_price - self.last_valid_spot) / self.last_valid_spot
                time_diff = now - self.last_spot_time
                if spot_move_pct > 0.03 or (spot_move_pct > 0.02 and time_diff < 0.2):
                    raise ValueError(f"Spot velocity spike detected. Move: {spot_move_pct*100:.2f}%.")
                    
            self.last_valid_spot = spot_price
            self.last_spot_time = now

            if symbol not in self.config.STRIKE_INTERVALS: raise ValueError(f"Unsupported symbol: {symbol}")
            if spot_price <= 0 or spot_price > 1000000: raise ValueError(f"Invalid spot: {spot_price}")
            
            atm = self.atm_locator.get_atm(symbol, spot_price)
            expiry = self.expiry_handler.choose_expiry(available_expiries, now_ist)
            vol_modifier = max(1.0, vix / 15.0) if vix else 1.0
            phase_multi = self._get_session_phase_multiplier(now_ist) * vol_modifier
            
            cache_key = f"smart_strike:{symbol}:{expiry}:{opt_type}:{atm}"
            try:
                cached_winner = await asyncio.wait_for(self.redis.get(cache_key), timeout=0.05)
                if cached_winner:
                    asyncio.create_task(self._log_telemetry("cache_hit_success", 0.0))
                    return orjson.loads(cached_winner)
            except: pass

            sticky_key = f"sticky_winner:{symbol}:{opt_type}"
            try:
                sticky_data_str = await asyncio.wait_for(self.redis.get(sticky_key), timeout=0.05)
                if sticky_data_str:
                    sticky_data = orjson.loads(sticky_data_str)
                    if sticky_data.get('expiry') == expiry:
                        val = await self.resolver.validate_liquidity(sticky_data['scrip_code'], phase_multi)
                        if val["valid"]:
                            score = self.scoring_engine.calculate_score(sticky_data['target_strike'], atm, symbol, opt_type, val, confidence)
                            return await self._build_success_payload(
                                symbol, expiry, sticky_data['target_strike'], opt_type, sticky_data['scrip_code'], val, score, start_time
                            )
            except: pass
            
            candidate_strikes = self.window_generator.generate_window(symbol, atm, opt_type, vix)

            async def evaluate_strike(target_strike):
                if target_strike <= 0: return None
                max_dist = self.config.MAX_STRIKE_DISTANCE_POINTS.get(symbol, 500)
                if abs(target_strike - spot_price) > max_dist: return None
                
                bounds = self.config.STRIKE_BOUNDS.get(symbol, (0, 100000))
                if not (bounds[0] <= target_strike <= bounds[1]): return None
                
                scrip_code = await self.resolver.get_scrip_code(symbol, expiry, target_strike, opt_type)
                if not scrip_code: return None
                
                validation = await self.resolver.validate_liquidity(scrip_code, phase_multi)
                if validation["valid"]:
                    score = self.scoring_engine.calculate_score(target_strike, atm, symbol, opt_type, validation, confidence)
                    return {
                        "scrip_code": scrip_code, "target_strike": target_strike,
                        "metrics": validation, "score": score
                    }
                else:
                    asyncio.create_task(self._log_telemetry(validation["reason"], 0.0))
                return None

            results = await asyncio.gather(*(evaluate_strike(strike) for strike in candidate_strikes), return_exceptions=True)
            valid_contracts = [res for res in results if res is not None and not isinstance(res, Exception)]

            if not valid_contracts:
                self.circuit_breaker.record_failure()
                asyncio.create_task(self._log_telemetry("route_fail", 0.0))
                return {"status": "error", "reason": "All calculated strikes failed liquidity validation."}
                
            best_contract = max(valid_contracts, key=lambda x: x["score"])
            
            success_payload = await self._build_success_payload(
                symbol, expiry, best_contract['target_strike'], opt_type, best_contract['scrip_code'], 
                best_contract['metrics'], best_contract['score'], start_time
            )
            
            try:
                await asyncio.wait_for(self.redis.setex(cache_key, 2, orjson.dumps(success_payload)), timeout=0.05)
                sticky_save = {"scrip_code": best_contract["scrip_code"], "target_strike": best_contract["target_strike"], "expiry": expiry}
                await asyncio.wait_for(self.redis.setex(sticky_key, 10, orjson.dumps(sticky_save)), timeout=0.05)
            except: pass
            
            return success_payload
            
        except Exception as e:
            if not isinstance(e, RuntimeError) or ("Rate Limit" not in str(e) and "latency hard kill" not in str(e)): 
                self.circuit_breaker.record_failure()
            asyncio.create_task(self._log_telemetry("exception_fail", 0.0))
            return {"status": "error", "reason": str(e)}
EOF
