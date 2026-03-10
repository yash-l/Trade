cat << 'EOF' > module3.py
import time
import asyncio
import logging
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
import redis.asyncio as aioredis
import orjson

# Import Module 4: Risk & Position Sizing Engine
from position_sizer import PositionSizer, TradeDirection

logger = logging.getLogger("AlphaEngine")
logging.basicConfig(level=logging.INFO, format='%(message)s')
IST = ZoneInfo("Asia/Kolkata")

@dataclass
class AlphaConfig:
    STRATEGY_VERSION: str = "v1.7.0-eod-persistence"
    MAX_INSTRUMENTS: int = 500  # SRE PATCH: Hard limit for Memory Bank
    NIFTY_SPOT_SCRIP: int = 999920000
    BANKNIFTY_SPOT_SCRIP: int = 999920005
    
    BASE_WEIGHT_VWAP: float = 0.4
    BASE_WEIGHT_EMA: float = 0.3
    BASE_WEIGHT_RSI: float = 0.3
    ORDERFLOW_MULTIPLIER: float = 1.2  
    
    CONFIDENCE_THRESHOLD: float = 0.65
    SIGNAL_COOLDOWN_SEC: float = 15.0
    MAX_SIGNALS_PER_MIN: int = 5
    MAX_DAILY_SIGNALS: int = 10        
    
    MAX_SPREAD_PCT: float = 0.5
    MAX_ATR_SPIKE: float = 3.0  
    EXCHANGE_CIRCUIT_PCT: float = 10.0 
    
    MIN_LIQUIDITY_VOL: int = 500  
    IMBALANCE_THRESHOLD: float = 2.5   
    
    ATR_WARMUP_TICKS: int = 20
    RSI_WARMUP_TICKS: int = 14
    MIN_MOMENTUM_ATR_MULTIPLIER: float = 0.5
    FEED_TIMEOUT_SEC: float = 5.0
    MIN_PRICE_MOVE_FOR_DUPLICATE_PCT: float = 0.2
    
    # Capital, Risk, and Time Limits
    MAX_DAILY_DRAWDOWN: float = -2500.0
    SL_ATR_MULTIPLIER: float = 2.0     
    MAX_CONCURRENT_POSITIONS: int = 3  
    SL_SLIPPAGE_BUFFER_PCT: float = 0.0005 # 0.05% SL slippage shock absorber
    EOD_SQUARE_OFF_TIME: int = 1514        # 3:14 PM EOD exit

@dataclass
class GlobalMarketContext:
    nifty_trend: int = 0
    banknifty_trend: int = 0
    nifty_regime: str = "UNKNOWN"

@dataclass
class InstrumentState:
    scrip_code: int
    symbol: str = "UNKNOWN" 
    trading_date: str = ""
    tick_count: int = 0
    
    open_price: float = 0.0          
    last_price: float = 0.0
    last_volume: int = 0
    ema_volume: float = 0.0          
    last_timestamp: float = 0.0
    
    vwap_num: float = 0.0
    vwap_den: float = 0.0
    vwap: float = 0.0
    prev_vwap: float = 0.0
    vwap_slope: float = 0.0          
    
    ema_9: float = 0.0
    ema_21: float = 0.0
    atr: float = 0.0
    
    avg_gain: float = 0.0
    avg_loss: float = 0.0
    rsi_14: float = 50.0
    
    bid_ask_imbalance: float = 1.0   
    
    last_signal_time: float = 0.0
    last_signal_action: str = ""        
    last_signal_price: float = 0.0      
    signal_count_minute: int = 0
    daily_signal_count: int = 0      
    minute_window_start: float = 0.0
    is_halted: bool = False          

    def update_indicators(self, price: float, volume: int, bq: int, aq: int, current_date: str, config: AlphaConfig):
        self.tick_count += 1
        
        if self.trading_date != current_date:
            self.vwap_num, self.vwap_den = 0.0, 0.0
            self.vwap = self.prev_vwap = self.open_price = price
            self.daily_signal_count = 0
            self.is_halted = False
            self.trading_date = current_date

        if self.last_price == 0.0:
            self.last_price = self.open_price = self.ema_9 = self.ema_21 = price
            self.ema_volume = volume
            return

        if abs(price - self.open_price) / self.open_price * 100 >= config.EXCHANGE_CIRCUIT_PCT:
            self.is_halted = True

        self.bid_ask_imbalance = bq / max(aq, 1)

        if volume > 0:
            self.vwap_num += (price * volume)
            self.vwap_den += volume
            self.prev_vwap = self.vwap
            self.vwap = self.vwap_num / self.vwap_den if self.vwap_den > 0 else price
            self.vwap_slope = self.vwap - self.prev_vwap
            self.ema_volume = (volume - self.ema_volume) * (2 / (10 + 1)) + self.ema_volume

        self.ema_9 = (price - self.ema_9) * (2 / (9 + 1)) + self.ema_9
        self.ema_21 = (price - self.ema_21) * (2 / (21 + 1)) + self.ema_21

        tr = abs(price - self.last_price)
        self.atr = ((self.atr * 13) + tr) / 14 if self.atr > 0 else tr

        change = price - self.last_price
        gain = change if change > 0 else 0.0
        loss = abs(change) if change < 0 else 0.0

        self.avg_gain = ((self.avg_gain * 13) + gain) / 14
        self.avg_loss = ((self.avg_loss * 13) + loss) / 14
        
        if self.tick_count >= 14:
            self.rsi_14 = 100.0 if self.avg_loss == 0 else 100 - (100 / (1 + (self.avg_gain / self.avg_loss)))

        self.last_price, self.last_volume, self.last_timestamp = price, volume, time.time()

class PortfolioGovernor:
    def __init__(self, config: AlphaConfig):
        self.config = config
        self.trading_date = datetime.now(IST).strftime("%Y-%m-%d")
        self.daily_pnl = 0.0
        self.global_kill_switch = False
        self.active_positions = {} 

    def check_rollover(self):
        today = datetime.now(IST).strftime("%Y-%m-%d")
        if self.trading_date != today:
            self.daily_pnl = 0.0
            self.global_kill_switch = False
            self.active_positions.clear()
            self.trading_date = today

    def can_trade(self, scrip_code: int) -> bool:
        if self.global_kill_switch: return False
        if scrip_code in self.active_positions: return False 
        if len(self.active_positions) >= self.config.MAX_CONCURRENT_POSITIONS: return False
        return True

    def lock_position(self, scrip: int, action: str, entry_price: float, sl_price: float):
        self.active_positions[scrip] = {
            "action": action, "entry_spot": entry_price, "sl_spot": sl_price,
            "highest_high": entry_price, "lowest_low": entry_price, "entry_time": time.time()
        }
        logger.warning(f"馃敀 Position Locked on {scrip}. Shifting to Trade Management Mode.")

    def unlock_position(self, scrip: int):
        if scrip in self.active_positions:
            del self.active_positions[scrip]
            logger.warning(f"馃敁 Position Unlocked on {scrip}. Awaiting EMS true PnL feedback.")

    def apply_realized_pnl(self, realized_pnl: float) -> bool:
        self.daily_pnl += realized_pnl
        logger.info(f"馃挵 Realized PnL updated: {realized_pnl}. Daily Total: {self.daily_pnl}")
        if self.daily_pnl <= self.config.MAX_DAILY_DRAWDOWN and not self.global_kill_switch:
            self.global_kill_switch = True
            logger.critical(f"馃洃 GLOBAL KILL SWITCH ACTIVATED. Max Drawdown {self.config.MAX_DAILY_DRAWDOWN} hit.")
            return True
        return False

class SecurityFilter:
    def __init__(self, config: AlphaConfig):
        self.config = config

    def is_safe_to_trade(self, state: InstrumentState, tick: dict, now_ist: datetime) -> bool:
        if state.is_halted or state.daily_signal_count >= self.config.MAX_DAILY_SIGNALS: return False
        time_val = now_ist.hour * 100 + now_ist.minute
        if not (920 <= time_val <= 1510): return False
        tick_ts = float(tick.get("ts", 0))
        if tick_ts > 1e12: tick_ts = tick_ts / 1000.0
        if time.time() - tick_ts > 3.0: return False
        if state.last_price <= 0: return False
        ask, bid = float(tick.get("ask", 0)), float(tick.get("bid", 0))
        if bid <= 0 or ask <= 0 or ((ask - bid) / state.last_price) * 100 > self.config.MAX_SPREAD_PCT: return False
        if state.tick_count > self.config.ATR_WARMUP_TICKS and state.atr > 0 and abs(state.last_price - float(tick.get("ltp", 0))) > (state.atr * self.config.MAX_ATR_SPIKE): return False
        if (int(tick.get("bq", 0)) + int(tick.get("aq", 0))) < self.config.MIN_LIQUIDITY_VOL: return False
        now = time.time()
        if now - state.last_signal_time < self.config.SIGNAL_COOLDOWN_SEC: return False
        if now - state.minute_window_start > 60.0:
            state.minute_window_start, state.signal_count_minute = now, 0
        if state.signal_count_minute >= self.config.MAX_SIGNALS_PER_MIN: return False
        return True

class AlphaStrategyEngine:
    def __init__(self, config: AlphaConfig):
        self.config = config

    def _detect_regime(self, state: InstrumentState) -> str:
        if state.last_price == 0 or state.tick_count < self.config.ATR_WARMUP_TICKS: return "UNKNOWN"
        is_high_vol = ((state.atr / state.last_price) * 100) > 0.25
        is_trending = (abs(state.ema_9 - state.ema_21) / state.last_price * 100) > 0.15 
        if is_trending: return "TREND_HIGH_VOL" if is_high_vol else "TREND_LOW_VOL"
        else: return "RANGE_HIGH_VOL" if is_high_vol else "RANGE_LOW_VOL"

    def _get_dynamic_weights(self, regime: str) -> tuple[float, float, float]:
        if "TREND" in regime: return (0.45, 0.40, 0.15)
        elif "RANGE" in regime: return (0.20, 0.20, 0.60)
        return (self.config.BASE_WEIGHT_VWAP, self.config.BASE_WEIGHT_EMA, self.config.BASE_WEIGHT_RSI)

    def evaluate(self, state: InstrumentState, ctx: GlobalMarketContext) -> dict | None:
        if state.tick_count < max(self.config.ATR_WARMUP_TICKS, self.config.RSI_WARMUP_TICKS): return None
        regime = self._detect_regime(state)
        wt_vwap, wt_ema, wt_rsi = self._get_dynamic_weights(regime)
        if abs(state.last_price - state.ema_21) < (state.atr * self.config.MIN_MOMENTUM_ATR_MULTIPLIER): return None  

        long_score, short_score, strategies_hit = 0.0, 0.0, []

        if state.last_price > state.vwap * 1.001 and state.vwap_slope >= 0: 
            long_score += wt_vwap; strategies_hit.append("VWAP_BULL")
        elif state.last_price < state.vwap * 0.999 and state.vwap_slope <= 0: 
            short_score += wt_vwap; strategies_hit.append("VWAP_BEAR")

        if state.ema_9 > state.ema_21: long_score += wt_ema; strategies_hit.append("EMA_BULL")
        elif state.ema_9 < state.ema_21: short_score += wt_ema; strategies_hit.append("EMA_BEAR")

        if 55 < state.rsi_14 < 70: long_score += wt_rsi; strategies_hit.append("RSI_BULL")
        elif 30 < state.rsi_14 < 45: short_score += wt_rsi; strategies_hit.append("RSI_BEAR")

        if state.last_volume > (state.ema_volume * 1.5):
            long_score *= 1.1; short_score *= 1.1; strategies_hit.append("VOL_EXPANSION")

        if state.bid_ask_imbalance >= self.config.IMBALANCE_THRESHOLD:
            long_score *= self.config.ORDERFLOW_MULTIPLIER; strategies_hit.append("ORDERFLOW_BULL")
        elif state.bid_ask_imbalance <= (1.0 / self.config.IMBALANCE_THRESHOLD):
            short_score *= self.config.ORDERFLOW_MULTIPLIER; strategies_hit.append("ORDERFLOW_BEAR")

        if ctx.nifty_trend == 1: short_score *= 0.5  
        elif ctx.nifty_trend == -1: long_score *= 0.5   

        action, confidence = None, 0.0
        if long_score >= self.config.CONFIDENCE_THRESHOLD and long_score > short_score: action, confidence = "BUY_CALL", long_score
        elif short_score >= self.config.CONFIDENCE_THRESHOLD and short_score > long_score: action, confidence = "BUY_PUT", short_score

        if not action: return None
        if action == state.last_signal_action and abs(state.last_price - state.last_signal_price) / state.last_signal_price * 100 < self.config.MIN_PRICE_MOVE_FOR_DUPLICATE_PCT: return None 

        return self._build_smart_payload(state, action, confidence, strategies_hit, regime)

    def _build_smart_payload(self, state, action, confidence, strategies, regime):
        vol_pct = (state.atr / state.last_price) * 100 if state.last_price > 0 else 0
        pref_delta = 0.45 if ("TREND" in regime and vol_pct > 0.3) else 0.55 if "TREND" in regime else 0.35 if vol_pct > 0.3 else 0.65              
        stop_loss_price = state.last_price - (state.atr * self.config.SL_ATR_MULTIPLIER) if action == "BUY_CALL" else state.last_price + (state.atr * self.config.SL_ATR_MULTIPLIER)

        return {
            "symbol": state.symbol, "scrip_code": state.scrip_code, "action": action,
            "confidence": round(confidence, 2), "spot_entry": state.last_price, "spot_sl": round(stop_loss_price, 2),
            "market_regime": regime, "vol_pct": round(vol_pct, 2), "preferred_delta": pref_delta,
            "strategy": "+".join(strategies), "version": self.config.STRATEGY_VERSION, "timestamp": int(time.time())
        }

class AlphaEngineController:
    def __init__(self, redis_url: str):
        self.config = AlphaConfig()
        self.redis_url = redis_url
        self.redis = None
        self.consumer_id = f"alpha_node_{uuid.uuid4().hex[:8]}"
        
        self.memory_bank = OrderedDict() 
        self.instrument_master = {} 
        self.active_streams = {}
        
        self.governor = PortfolioGovernor(self.config)
        self.filter = SecurityFilter(self.config)
        self.strategy = AlphaStrategyEngine(self.config)
        self.sizer = PositionSizer(available_capital=50000.0, max_risk_pct=0.02)
        
        self.metrics = {"ticks_processed": 0, "signals_generated": 0}
        self.market_ctx = GlobalMarketContext()  
        
        self.last_global_tick_time = 0.0
        # SRE PATCH: Queue maxsize restricted to 2000 to prevent strategy stall and feed lag
        self.signal_queue = asyncio.Queue(maxsize=2000) 

    async def _load_and_build_streams(self):
        try:
            mapping = await self.redis.hgetall("instrument:master")
            if mapping:
                self.instrument_master = {int(k): v for k, v in mapping.items()}
                self.active_streams = {f"ticks:{scrip}": ">" for scrip in self.instrument_master.keys()}
                for stream_key in self.active_streams.keys():
                    try: await self.redis.xgroup_create(stream_key, "alpha_group", id="0", mkstream=True)
                    except aioredis.exceptions.ResponseError as e:
                        if "BUSYGROUP" not in str(e): pass
                logger.info(f"Loaded {len(self.instrument_master)} instruments. Node ID: {self.consumer_id}")
        except Exception as e: logger.error(f"Failed to load master: {e}")

    async def _snapshot_state(self):
        while True:
            await asyncio.sleep(60) 
            
            # 1. Snapshot Portfolio Governor
            try:
                gov_payload = orjson.dumps({
                    "daily_pnl": self.governor.daily_pnl,
                    "global_kill_switch": self.governor.global_kill_switch,
                    "active_positions": self.governor.active_positions,
                    "trading_date": self.governor.trading_date
                })
                await self.redis.set(f"alpha:portfolio_backup:{self.consumer_id}", gov_payload)
            except Exception as e:
                logger.error(f"Governor snapshot failed: {e}")

            # 2. Snapshot Indicator Memory
            if not self.memory_bank: continue
            try:
                pipe = self.redis.pipeline(transaction=False)
                for scrip, state in self.memory_bank.items():
                    payload = orjson.dumps({
                        "vwap_num": state.vwap_num, "vwap_den": state.vwap_den, "vwap": state.vwap, "prev_vwap": state.prev_vwap, "vwap_slope": state.vwap_slope,
                        "ema_9": state.ema_9, "ema_21": state.ema_21, "atr": state.atr, "avg_gain": state.avg_gain, "avg_loss": state.avg_loss, "rsi_14": state.rsi_14, 
                        "ema_volume": state.ema_volume, "bid_ask_imbalance": state.bid_ask_imbalance, "last_signal_time": state.last_signal_time, 
                        "last_signal_action": state.last_signal_action, "last_signal_price": state.last_signal_price, "signal_count_minute": state.signal_count_minute,
                        "minute_window_start": state.minute_window_start, "daily_signal_count": state.daily_signal_count, "is_halted": state.is_halted, 
                        "trading_date": state.trading_date, "tick_count": state.tick_count
                    })
                    pipe.hset(f"alpha:state_backup:{self.consumer_id}", str(scrip), payload)
                await asyncio.wait_for(pipe.execute(), timeout=2.0)
            except Exception: pass

    async def _restore_state(self):
        today_str = datetime.now(IST).strftime("%Y-%m-%d")
        
        # 1. Restore Portfolio Governor
        try:
            gov_backup = await self.redis.get(f"alpha:portfolio_backup:{self.consumer_id}")
            if gov_backup:
                gov_data = orjson.loads(gov_backup)
                if gov_data.get("trading_date") == today_str:
                    self.governor.daily_pnl = gov_data.get("daily_pnl", 0.0)
                    self.governor.global_kill_switch = gov_data.get("global_kill_switch", False)
                    # strict JSON dict key recovery (JSON converts ints to strings)
                    self.governor.active_positions = {int(k): v for k, v in gov_data.get("active_positions", {}).items()}
                    self.governor.trading_date = today_str
                    logger.warning(f"Restored Governor: PnL {self.governor.daily_pnl}, Active: {len(self.governor.active_positions)}")
                else:
                    self.governor.check_rollover()
        except Exception as e:
            logger.error(f"Failed to restore governor: {e}")

        # 2. Restore Indicator Memory
        try:
            backups = await self.redis.hgetall(f"alpha:state_backup:{self.consumer_id}")
            restored_count = 0
            for scrip_str, payload_str in backups.items():
                data = orjson.loads(payload_str)
                if data.get("trading_date") == today_str:
                    scrip = int(scrip_str)
                    state = InstrumentState(scrip_code=scrip, symbol=self.instrument_master.get(scrip, "UNK"))
                    state.vwap_num, state.vwap_den, state.vwap = data["vwap_num"], data["vwap_den"], data["vwap"]
                    state.prev_vwap, state.vwap_slope = data["prev_vwap"], data["vwap_slope"]
                    state.ema_9, state.ema_21, state.atr = data["ema_9"], data["ema_21"], data["atr"]
                    state.avg_gain, state.avg_loss, state.rsi_14 = data["avg_gain"], data["avg_loss"], data["rsi_14"]
                    state.ema_volume, state.bid_ask_imbalance = data["ema_volume"], data["bid_ask_imbalance"]
                    state.last_signal_time, state.last_signal_action = data["last_signal_time"], data["last_signal_action"]
                    state.last_signal_price, state.signal_count_minute = data["last_signal_price"], data["signal_count_minute"]
                    state.minute_window_start, state.daily_signal_count = data["minute_window_start"], data["daily_signal_count"]
                    state.is_halted, state.trading_date, state.tick_count = data["is_halted"], data["trading_date"], data["tick_count"]

                    self.memory_bank[scrip] = state
                    restored_count += 1
            if restored_count > 0: logger.warning(f"Restored {restored_count} indicator states for {self.consumer_id}.")
        except Exception: pass

    def _update_cross_asset_context(self, scrip_code: int, state: InstrumentState):
        trend_val = 1 if (state.last_price > state.vwap and state.ema_9 > state.ema_21) else \
                   -1 if (state.last_price < state.vwap and state.ema_9 < state.ema_21) else 0
        if scrip_code == self.config.NIFTY_SPOT_SCRIP:
            self.market_ctx.nifty_trend, self.market_ctx.nifty_regime = trend_val, self.strategy._detect_regime(state)
        elif scrip_code == self.config.BANKNIFTY_SPOT_SCRIP:
            self.market_ctx.banknifty_trend = trend_val

    async def _redis_watchdog(self):
        backoff = 1.0
        while True:
            try:
                if self.redis: await self.redis.ping()
                backoff = 1.0
                await asyncio.sleep(5)
            except Exception:
                try: await self.redis.close()
                except: pass
                try: self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
                except: pass
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

    async def _feed_watchdog(self):
        while True:
            await asyncio.sleep(1)
            if self.last_global_tick_time > 0:
                time_since_last = time.time() - self.last_global_tick_time
                if time_since_last > self.config.FEED_TIMEOUT_SEC:
                    logger.critical(f"馃毃 FEED DEAD: No ticks received in {time_since_last:.1f} seconds! Module 1 may be down.")

    async def _eod_watchdog(self):
        """Monitors EOD Square Off and forces liquidation at 3:14 PM"""
        while True:
            await asyncio.sleep(15)
            self.governor.check_rollover() 
            
            now_ist = datetime.now(IST)
            time_val = now_ist.hour * 100 + now_ist.minute
            
            if time_val >= self.config.EOD_SQUARE_OFF_TIME and not self.governor.global_kill_switch:
                if self.governor.active_positions:
                    logger.warning("馃晵 EOD SQUARE OFF. Liquidating all open positions.")
                    await self._liquidate_all_active_positions(reason="EOD_SQUARE_OFF")
                self.governor.global_kill_switch = True 

    async def _signal_writer_loop(self):
        while True:
            signal = await self.signal_queue.get()
            try:
                await self.redis.xadd("signals:alpha", signal, maxlen=5000)
                logger.warning(orjson.dumps({"event": "SIGNAL_FIRED", "data": signal}).decode('utf-8'))
            except Exception as e: logger.error(f"Routing failed: {e}")
            finally: self.signal_queue.task_done()

    async def _liquidate_all_active_positions(self, reason="GLOBAL_KILL_SWITCH"):
        logger.critical(f"馃毃 FORCED LIQUIDATION [{reason}]: Emitting emergency CLOSE commands.")
        for scrip in list(self.governor.active_positions.keys()):
            exit_payload = {
                "scrip_code": scrip, 
                "action": "CLOSE_POSITION", 
                "reason": reason, 
                "timestamp": int(time.time())
            }
            try: self.signal_queue.put_nowait(exit_payload)
            except asyncio.QueueFull: pass
            self.governor.unlock_position(scrip)

    async def _pnl_feedback_loop(self):
        try: await self.redis.xgroup_create("execution:feedback", "alpha_feedback_group", id="0", mkstream=True)
        except Exception: pass
        
        while True:
            try:
                streams = await self.redis.xreadgroup(
                    groupname="alpha_feedback_group", consumername=self.consumer_id,
                    streams={"execution:feedback": ">"}, count=50, block=2000
                )
                if streams:
                    for stream_name, messages in streams:
                        for message_id, raw_data in messages:
                            if raw_data.get("action") == "CLOSED":
                                pnl = float(raw_data.get("realized_pnl", 0.0))
                                kill_tripped = self.governor.apply_realized_pnl(pnl)
                                if kill_tripped:
                                    await self._liquidate_all_active_positions(reason="MAX_DRAWDOWN_HIT")
                            await self.redis.xack(stream_name, "alpha_feedback_group", message_id)
            except Exception:
                await asyncio.sleep(1)

    async def _zombie_hunter_loop(self):
        while True:
            await asyncio.sleep(15) 
            if not self.active_streams: continue
            now_ist, today_str = datetime.now(IST), datetime.now(IST).strftime("%Y-%m-%d")
            
            for stream_key in self.active_streams.keys():
                try:
                    reply = await self.redis.xautoclaim(
                        name=stream_key, groupname="alpha_group", consumername=self.consumer_id,
                        min_idle_time=10000, start_id="0-0", count=100
                    )
                    if reply and len(reply) >= 2 and reply[1]:
                        stolen = reply[1]
                        logger.warning(f"[{self.consumer_id}] XAUTOCLAIM stole {len(stolen)} dead ticks.")
                        await self._process_stream_batch([(stream_key, stolen)], today_str, now_ist)
                except Exception: pass

    async def _process_stream_batch(self, streams, today_str, now_ist):
        for stream_name, messages in streams:
            for message_id, raw_tick in messages:
                tick_start_time = time.perf_counter()
                self.last_global_tick_time = time.time() 
                self.metrics["ticks_processed"] += 1
                
                stream_str = stream_name.decode('utf-8') if isinstance(stream_name, bytes) else stream_name
                scrip_code = int(stream_str.split(":")[1])
                price = float(raw_tick.get("ltp", 0))
                
                # SRE PATCH: Memory Bank Eviction Policy applied to OrderedDict
                if scrip_code not in self.memory_bank:
                    if len(self.memory_bank) >= self.config.MAX_INSTRUMENTS: 
                        self.memory_bank.popitem(last=False)
                    self.memory_bank[scrip_code] = InstrumentState(scrip_code=scrip_code, symbol=self.instrument_master.get(scrip_code, "UNK"))
                    
                state = self.memory_bank[scrip_code]
                self.memory_bank.move_to_end(scrip_code) 
                
                tick_dt = datetime.fromtimestamp(float(raw_tick.get("ts", time.time())), tz=IST)
                state.update_indicators(price, int(raw_tick.get("vol", 0)), int(raw_tick.get("bq", 0)), int(raw_tick.get("aq", 0)), tick_dt.strftime("%Y-%m-%d"), self.config)
                self._update_cross_asset_context(scrip_code, state)

                # --- 1. TRADE MANAGEMENT MODE (Trailing Exits) ---
                if scrip_code in self.governor.active_positions:
                    trade = self.governor.active_positions[scrip_code]
                    exit_reason = None
                    
                    if trade["action"] == "BUY_CALL":
                        if price > trade["highest_high"]:
                            trade["highest_high"] = price
                            new_sl = price - (state.atr * self.config.SL_ATR_MULTIPLIER)
                            trade["sl_spot"] = max(trade["sl_spot"], new_sl) 
                        if price <= trade["sl_spot"]: exit_reason = "TRAILING_SL_HIT"

                    elif trade["action"] == "BUY_PUT":
                        if price < trade["lowest_low"]:
                            trade["lowest_low"] = price
                            new_sl = price + (state.atr * self.config.SL_ATR_MULTIPLIER)
                            trade["sl_spot"] = min(trade["sl_spot"], new_sl) 
                        if price >= trade["sl_spot"]: exit_reason = "TRAILING_SL_HIT"
                    
                    if exit_reason:
                        self.governor.unlock_position(scrip_code)
                        exit_payload = {"scrip_code": scrip_code, "action": "CLOSE_POSITION", "reason": exit_reason, "exit_spot": price}
                        try: self.signal_queue.put_nowait(exit_payload)
                        except asyncio.QueueFull: pass
                    
                    await self.redis.xack(stream_name, "alpha_group", message_id)
                    continue 

                # --- 2. SIGNAL GENERATION MODE (Risk Adjusted) ---
                if self.governor.can_trade(scrip_code) and self.filter.is_safe_to_trade(state, raw_tick, now_ist):
                    signal = self.strategy.evaluate(state, self.market_ctx)
                    if signal:
                        trade_dir = TradeDirection.LONG if signal["action"] == "BUY_CALL" else TradeDirection.SHORT
                        
                        spread_buffer = signal["spot_entry"] * self.config.SL_SLIPPAGE_BUFFER_PCT
                        effective_sl = signal["spot_sl"] - spread_buffer if trade_dir == TradeDirection.LONG else signal["spot_sl"] + spread_buffer

                        risk_eval = self.sizer.calculate_trade(entry_price=signal["spot_entry"], stop_loss=effective_sl, trade_type=trade_dir)
                        
                        if risk_eval.status == "APPROVED":
                            state.last_signal_time, state.last_signal_action, state.last_signal_price = time.time(), signal["action"], state.last_price
                            state.signal_count_minute += 1; state.daily_signal_count += 1 
                            self.metrics["signals_generated"] += 1
                            
                            signal["position_size_shares"] = risk_eval.shares
                            signal["capital_allocated"] = risk_eval.capital_required
                            signal["expected_risk"] = risk_eval.actual_risk
                            signal["internal_latency_ms"] = round((time.perf_counter() - tick_start_time) * 1000, 3)
                            
                            self.governor.lock_position(scrip_code, signal["action"], signal["spot_entry"], signal["spot_sl"])
                            
                            try: self.signal_queue.put_nowait(signal)
                            except asyncio.QueueFull: pass
                        else:
                            logger.debug(f"Rejected by Risk Engine: {risk_eval.reason}")
                
                await self.redis.xack(stream_name, "alpha_group", message_id)

    async def start(self):
        self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
        await self._load_and_build_streams()
        await self._restore_state()
        
        asyncio.create_task(self._redis_watchdog())
        asyncio.create_task(self._feed_watchdog())
        asyncio.create_task(self._eod_watchdog())
        asyncio.create_task(self._snapshot_state())
        asyncio.create_task(self._signal_writer_loop())
        asyncio.create_task(self._zombie_hunter_loop())
        asyncio.create_task(self._pnl_feedback_loop())
        asyncio.create_task(self._telemetry_loop())
        
        logger.info(f"馃煝 Alpha Engine [{self.consumer_id}] Online.")
        await self._consume_loop()

    async def _consume_loop(self):
        now_ist, today_str = datetime.now(IST), datetime.now(IST).strftime("%Y-%m-%d")
        try:
            pending_streams = await self.redis.xreadgroup(groupname="alpha_group", consumername=self.consumer_id, streams={k: "0-0" for k in self.active_streams.keys()}, count=500)
            if pending_streams: await self._process_stream_batch(pending_streams, today_str, now_ist)
        except Exception: pass

        while True:
            if not self.active_streams:
                await asyncio.sleep(1); await self._load_and_build_streams(); continue
            try:
                streams = await self.redis.xreadgroup(groupname="alpha_group", consumername=self.consumer_id, streams=self.active_streams, count=100, block=100)
                if streams:
                    await self._process_stream_batch(streams, datetime.now(IST).strftime("%Y-%m-%d"), datetime.now(IST))
                    await asyncio.sleep(0) 
            except aioredis.exceptions.ConnectionError: await asyncio.sleep(1)
            except Exception: await asyncio.sleep(0.5)

    async def _telemetry_loop(self):
        while True:
            await asyncio.sleep(60)
            logger.info(orjson.dumps({
                "event": "ALPHA_TELEMETRY", 
                "node_id": self.consumer_id,
                "ticks_processed": self.metrics["ticks_processed"],
                "signals_routed": self.metrics["signals_generated"],
                "instruments_tracked": len(self.memory_bank),
                "writer_queue_size": self.signal_queue.qsize(),
                "active_positions": len(self.governor.active_positions),
                "daily_pnl": self.governor.daily_pnl
            }).decode('utf-8'))
            self.metrics["ticks_processed"] = 0

if __name__ == "__main__":
    engine = AlphaEngineController("redis://127.0.0.1:6379/0")
    try: asyncio.run(engine.start())
    except KeyboardInterrupt: print("Shutdown.")
EOF
