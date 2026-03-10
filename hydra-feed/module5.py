import time
import asyncio
import logging
import uuid
import random
import collections
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor

import redis.asyncio as aioredis
import orjson
from py5paisa import FivePaisaClient
from py5paisa.strategy import *

# Institutional Production Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | [%(name)s] | %(message)s"
)
logger = logging.getLogger("5Paisa_EMS")

class OrderState(Enum):
    PENDING = "PENDING"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"

@dataclass
class EMSConfig:
    MAX_RETRIES: int = 3
    CHASE_TIMEOUT_SEC: float = 2.0
    EXECUTION_TIMEOUT_SEC: float = 12.0 
    FREEZE_LIMITS: dict = field(default_factory=lambda: {
        "NIFTY": 1800, "BANKNIFTY": 900, "FINNIFTY": 1800, "MIDCPNIFTY": 3000
    })
    MAX_OPEN_POSITIONS: int = 5
    MAX_DAILY_LOSS_LIMIT: float = -5000.0
    MAX_CAPITAL_PER_TRADE: float = 200000.0
    
    # Institutional Protections
    MAX_ORDERS_PER_SEC: float = 5.0
    MAX_ORDERS_PER_MIN: float = 100.0
    MAX_PORTFOLIO_DELTA: float = 1000.0
    BROKER_OUTAGE_TIMEOUT_SEC: float = 12.0
    QUEUE_MAX_SIZE: int = 100
    TICK_QUEUE_MAX_SIZE: int = 10000
    MAX_BROKER_FAILURES: int = 10
    
    # Market Threat Guards
    MORNING_LOCK_MINUTES: int = 2
    STALE_SIGNAL_DROP_SEC: float = 2.0
    MAX_SLIPPAGE_FALLBACK_PCT: float = 0.02
    MAX_ACCEPTABLE_SPREAD_PCT: float = 0.05

# ============================================================
# 1. ASYNC GATEWAY, CACHES, & RATE LIMITERS
# ============================================================

class TokenBucketRateLimiter:
    def __init__(self, rate: float, capacity: float):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            while True:
                now = time.monotonic()
                elapsed = now - self.last_update
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.last_update = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                await asyncio.sleep(0.01)

class Async5PaisaGateway:
    def __init__(self, client: FivePaisaClient, config: EMSConfig):
        self.client = client
        self.config = config
        self.executor = ThreadPoolExecutor(max_workers=10)

    async def place_order(self, scrip: int, qty: int, is_buy: bool, order_type: str, price: float = 0.0):
        req = OrderRequest(
            Exchange="N", ExchangeType="D", ScripCode=scrip, Qty=qty, Price=price,
            BuySell="B" if is_buy else "S", OrderType=order_type, IsIntraday=True
        )
        try:
            return await asyncio.get_running_loop().run_in_executor(self.executor, self.client.place_order, req)
        except Exception as e:
            return {"Message": str(e), "Status": -1}

    async def safe_place_order(self, scrip: int, qty: int, is_buy: bool, order_type: str, price: float = 0.0):
        for _ in range(self.config.MAX_RETRIES):
            resp = await self.place_order(scrip, qty, is_buy, order_type, price)
            if isinstance(resp, dict) and resp.get("Status") == 0:
                return resp
            await asyncio.sleep(0.1)
        return resp

    async def cancel_order(self, exch_order_id: str):
        try:
            return await asyncio.get_running_loop().run_in_executor(self.executor, self.client.cancel_order, "N", "D", exch_order_id)
        except Exception as e:
            return {"Message": str(e), "Status": -1}

    async def fetch_order_book(self):
        try:
            return await asyncio.get_running_loop().run_in_executor(self.executor, self.client.order_book)
        except Exception:
            return None

    async def fetch_market_depth(self, scrip: int) -> dict:
        try:
            req = [{"Exchange": "N", "ExchangeType": "D", "ScripCode": str(scrip)}]
            resp = await asyncio.get_running_loop().run_in_executor(self.executor, self.client.fetch_market_depth, req)
            if resp and "Data" in resp and resp["Data"]:
                data = resp["Data"][0]
                return {
                    "ltp": float(data.get("LastTradedPrice", 0.0)),
                    "bid": float(data.get("BestBidPrice", 0.0)),
                    "ask": float(data.get("BestOfferPrice", 0.0)),
                    "bq": int(data.get("BestBidQty", 0)),
                    "aq": int(data.get("BestOfferQty", 0))
                }
        except Exception:
            pass
        return None

    async def refresh_login(self):
        try:
            await asyncio.get_running_loop().run_in_executor(self.executor, self.client.login)
        except Exception:
            pass

class OrderBookCache:
    def __init__(self, gateway: Async5PaisaGateway, ems_ref):
        self.gateway = gateway
        self.ems = ems_ref
        self.book_index = {}
        self._running = False
        self.last_update = time.time()

    async def start_polling(self):
        self._running = True
        while self._running:
            try:
                fetched_book = await self.gateway.fetch_order_book()
                if fetched_book is not None:
                    # Memory filter applied: keep only active orders
                    self.book_index = {
                        str(o.get("BrokerOrderId")): o 
                        for o in fetched_book 
                        if not self.ems.state_machine._is_resolved(o.get("OrderStatus"))
                    }
                    self.last_update = time.time()
            except Exception:
                pass
            
            outage_time = time.time() - self.last_update
            if outage_time > self.ems.config.BROKER_OUTAGE_TIMEOUT_SEC:
                logger.critical(f"🚨 BROKER OUTAGE DETECTED: API dead for {outage_time:.1f}s")
                await self.ems.trigger_global_panic("BROKER_API_TIMEOUT")
                self.last_update = time.time()
            await asyncio.sleep(0.7)

    def stop(self):
        self._running = False

    def get_order(self, order_id: str) -> dict:
        return self.book_index.get(str(order_id))

# ============================================================
# 2. MICROSTRUCTURE ICEBERG SLICER
# ============================================================

class OrderIcebergSlicer:
    @staticmethod
    def slice_order(symbol: str, total_qty: int, lot_size: int, config: EMSConfig) -> List[int]:
        base_symbol = "NIFTY"
        sym_up = symbol.upper()
        for key in ["BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTY"]:
            if key in sym_up:
                base_symbol = key
                break

        freeze_limit = config.FREEZE_LIMITS.get(base_symbol, 1000)
        slices = []
        remaining = (total_qty // lot_size) * lot_size
        while remaining > 0:
            if remaining <= freeze_limit:
                slices.append(remaining)
                break
            else:
                raw_chunk = int(freeze_limit * random.uniform(0.8, 1.0))
                chunk = (raw_chunk // lot_size) * lot_size
                if chunk == 0: chunk = lot_size
                slices.append(chunk)
                remaining -= chunk
        return slices

# ============================================================
# 3. ADVANCED STATE MACHINE & SPREAD CROSSER
# ============================================================

class ExecutionStateMachine:
    def __init__(self, gateway: Async5PaisaGateway, cache: OrderBookCache, config: EMSConfig, ems_ref):
        self.gateway = gateway
        self.cache = cache
        self.config = config
        self.ems = ems_ref
        self.broker_failures = 0

    def _is_resolved(self, status: str) -> bool:
        s = str(status).lower()
        if "partially" in s: return False
        return any(k in s for k in ["fully", "complet", "reject", "fail", "cancel"]) or s in ["executed", "filled"]

    async def execute_smart_order(self, scrip: int, total_qty: int, is_buy: bool, expected_price: float, symbol: str, lot_size: int, aggression: float = 1.0) -> dict:
        t0_submit = time.perf_counter()
        remaining_qty = total_qty
        cumulative_traded = 0
        weighted_cost = 0.0
        slice_attempts = 0

        while remaining_qty > 0 and slice_attempts < 3:
            if time.perf_counter() - t0_submit > self.config.EXECUTION_TIMEOUT_SEC:
                logger.error(f"[{self.ems.node_id}] ⏳ Execution Timeout on {symbol}. Aborting.")
                break
            
            slice_attempts += 1
            chunk_filled = 0
            chunk_price = 0.0  # Missing chunk_price Initialization applied
            m_qty = 0
            quote = self.ems.live_quotes.get(scrip, {})
            limit_price = expected_price
            executable_qty = remaining_qty

            if quote:
                bid, ask = float(quote.get("bid", expected_price)), float(quote.get("ask", expected_price))
                bq, aq = int(quote.get("bq", 1)), int(quote.get("aq", 1))
                
                spread = ask - bid
                if spread > (expected_price * self.config.MAX_ACCEPTABLE_SPREAD_PCT) and bid > 0:
                    logger.warning(f"[{self.ems.node_id}] 🛑 SPREAD REJECT: {symbol} spread (₹{spread:.2f}) too high.")
                    break

                if is_buy:
                    if executable_qty > aq: executable_qty = max(lot_size, (aq // lot_size) * lot_size)
                    limit_price = round(ask + 0.05, 2) if aggression >= 0.8 else ask
                else:
                    if executable_qty > bq: executable_qty = max(lot_size, (bq // lot_size) * lot_size)
                    limit_price = round(bid - 0.05, 2) if aggression >= 0.8 else bid

            resp = await self.gateway.safe_place_order(scrip, executable_qty, is_buy, "LIMIT", price=limit_price)
            if resp.get("Status") != 0:
                self.broker_failures += 1
                if self.broker_failures >= self.config.MAX_BROKER_FAILURES:
                    await self.ems.trigger_global_panic("BROKER_API_UNSTABLE")
                    break
                continue
            
            self.broker_failures = 0
            broker_id = resp.get("BrokerOrderId")
            chase_start = time.time()
            order_resolved = False

            while (time.time() - chase_start) < self.config.CHASE_TIMEOUT_SEC:
                order_info = self.cache.get_order(broker_id)
                if order_info:
                    chunk_filled = int(order_info.get("TradedQty", 0))
                    if self._is_resolved(order_info.get("OrderStatus")) or chunk_filled == executable_qty:
                        chunk_price = float(order_info.get("AveragePrice", 0.0))
                        order_resolved = True
                        break
                await asyncio.sleep(0.02)

            if not order_resolved:
                await self.gateway.cancel_order(broker_id)
                for _ in range(10):
                    info = self.cache.get_order(broker_id)
                    if info and self._is_resolved(info.get("OrderStatus")):
                        chunk_filled = int(info.get("TradedQty", 0))
                        chunk_price = float(info.get("AveragePrice", 0.0))
                        break
                    await asyncio.sleep(0.05)

            unfilled_in_chunk = executable_qty - chunk_filled
            if unfilled_in_chunk > 0:
                fallback_price = limit_price * (1 + self.config.MAX_SLIPPAGE_FALLBACK_PCT if is_buy else 1 - self.config.MAX_SLIPPAGE_FALLBACK_PCT)
                m_resp = await self.gateway.safe_place_order(scrip, unfilled_in_chunk, is_buy, "LIMIT", price=round(fallback_price, 2))
                if m_resp.get("Status") == 0:
                    m_id = m_resp.get("BrokerOrderId")
                    for _ in range(15):
                        m_info = self.cache.get_order(m_id)
                        if m_info and int(m_info.get("TradedQty", 0)) > 0:
                            m_qty = int(m_info.get("TradedQty", 0))
                            weighted_cost += (m_qty * float(m_info.get("AveragePrice", 0.0)))
                            cumulative_traded += m_qty
                            break
                        await asyncio.sleep(0.1)

            if chunk_filled > 0:
                weighted_cost += (chunk_filled * chunk_price)
                cumulative_traded += chunk_filled
            
            remaining_qty -= (chunk_filled + m_qty)
            if remaining_qty < lot_size: remaining_qty = 0
            await asyncio.sleep(0.1)

        final_avg = (weighted_cost / cumulative_traded) if cumulative_traded > 0 else 0.0
        state = OrderState.FILLED if cumulative_traded == total_qty else (OrderState.PARTIAL if cumulative_traded > 0 else OrderState.CANCELLED)
        return {"state": state, "filled_qty": cumulative_traded, "avg_price": final_avg, "latency_ms": (time.perf_counter() - t0_submit)*1000}

# ============================================================
# 4. QUEUE-DRIVEN SERVER-SIDE STOP ENGINE
# ============================================================

class ServerSideStopEngine:
    def __init__(self, ems):
        self.ems = ems
        self.active_stops = {}
        self.active_stops_lock = asyncio.Lock()
        self.stop_locks = collections.defaultdict(asyncio.Lock)
        self.last_tick_time = collections.defaultdict(float)
        self.tick_queue = asyncio.Queue(maxsize=self.ems.config.TICK_QUEUE_MAX_SIZE)

    async def _worker_loop(self):
        while self.ems._running:
            try:
                scrip, ltp = await self.tick_queue.get()
                await self._evaluate_stop(scrip, ltp)
                self.tick_queue.task_done()
            except Exception as e:
                logger.error(f"Stop Worker Error: {e}")

    async def listen_to_feed(self):
        for _ in range(2): asyncio.create_task(self._worker_loop())
        while self.ems._running:
            pubsub = None
            try:
                pubsub = self.ems.redis.pubsub(ignore_subscribe_messages=True)
                await pubsub.subscribe(f"{self.ems.ns}:live_ltp_stream")
                while self.ems._running:
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=5.0)
                    if message and message["type"] == "message":
                        data = orjson.loads(message["data"])
                        scrip, ltp = data["scrip"], data["ltp"]
                        self.ems.live_quotes[scrip] = data
                        self.last_tick_time[scrip] = time.time()
                        async with self.active_stops_lock:
                            if scrip in self.active_stops:
                                try: self.tick_queue.put_nowait((scrip, ltp))
                                except asyncio.QueueFull: pass
            except Exception as e:
                logger.error(f"PubSub Loop Error: {e}")
                await asyncio.sleep(5)
            finally:
                if pubsub: await pubsub.close()

    async def _evaluate_stop(self, scrip: int, ltp: float):
        async with self.stop_locks[scrip]:
            async with self.active_stops_lock:
                stop = self.active_stops.get(scrip)
                if not stop: return

                triggered = (stop["is_long"] and ltp <= stop["sl"]) or (not stop["is_long"] and ltp >= stop["sl"])
                
                if triggered:
                    if stop["confirm_ticks"] == 0: stop["trigger_time"] = time.time()
                    stop["confirm_ticks"] += 1
                    
                    if stop["confirm_ticks"] >= 2 or (time.time() - stop["trigger_time"]) > 0.15:
                        logger.critical(f"🛑 SERVER SL HIT: {stop['symbol']} @ {ltp}")
                        popped_stop = self.active_stops.pop(scrip, None)
                        if popped_stop:
                            await self.ems.trigger_square_off(scrip, popped_stop["qty"], popped_stop["symbol"], "TRAILING_SL_HIT", ltp)
                else:
                    stop["confirm_ticks"] = 0

# ============================================================
# 5. THE APEX EXECUTION MANAGER
# ============================================================

class ExecutionManager:
    def __init__(self, redis_url: str, account_id: str, client: FivePaisaClient):
        self.redis_url = redis_url
        self.ns = f"acct:{account_id}"
        self.node_id = f"EMS_{uuid.uuid4().hex[:6]}"
        self.config = EMSConfig()
        self.panic_mode = False 

        self.redis = aioredis.from_url(
            self.redis_url, encoding="utf-8", decode_responses=True, 
            max_connections=200, health_check_interval=30
        )
        self.gateway = Async5PaisaGateway(client, self.config)
        self.order_cache = OrderBookCache(self.gateway, self)
        self.state_machine = ExecutionStateMachine(self.gateway, self.order_cache, self.config, self)
        self.stop_engine = ServerSideStopEngine(self)
        
        self.positions = {}
        self.live_quotes = {}
        self._running = False
        self._tasks = []
        
        self.sec_rate_limiter = TokenBucketRateLimiter(self.config.MAX_ORDERS_PER_SEC, self.config.MAX_ORDERS_PER_SEC)
        self.min_rate_limiter = TokenBucketRateLimiter(self.config.MAX_ORDERS_PER_MIN / 60.0, self.config.MAX_ORDERS_PER_MIN)
        
        self.ems_daily_pnl = 0.0
        self.net_portfolio_delta = 0.0
        self.metrics = {"orders_sent": 0, "orders_filled": 0, "orders_rejected": 0, "total_slippage_pts": 0.0, "total_latency_ms": 0.0}
        self.signal_queue = asyncio.Queue(maxsize=self.config.QUEUE_MAX_SIZE)

    async def trigger_global_panic(self, reason: str):
        if self.panic_mode: return
        self.panic_mode = True
        
        logger.critical(f"💀 GLOBAL PANIC: {reason}. Flattening all positions.")
        await self.redis.set(f"{self.ns}:SYSTEM:KILLED", "1")
        
        tasks = []
        for scrip, pos in list(self.positions.items()):
            tasks.append(self.trigger_square_off(int(scrip), pos["qty"], pos["symbol"], f"PANIC_{reason}", pos["avg_price"]))
        
        if tasks: await asyncio.gather(*tasks, return_exceptions=True)

    async def trigger_square_off(self, scrip: int, qty: int, symbol: str, reason: str, exit_trigger_price: float = 0.0):
        pos_data = self.positions.get(scrip, {})
        if not pos_data: return
        
        lot_size = pos_data.get("lot_size", 25)
        is_buy = pos_data.get("side") == "SHORT"
        slices = OrderIcebergSlicer.slice_order(symbol, qty, lot_size, self.config)
        
        is_emergency = any(k in reason for k in ["PANIC", "HIT", "TIMEOUT"])
        results = []
        
        for q in slices:
            await self.sec_rate_limiter.acquire()
            task = self.state_machine.execute_smart_order(scrip, q, is_buy, exit_trigger_price, symbol, lot_size, aggression=1.0)
            if is_emergency:
                results.append(asyncio.create_task(task))
            else:
                results.append(await task)
        
        if is_emergency: results = await asyncio.gather(*results)
        
        total_filled = sum(r["filled_qty"] for r in results if r["state"] in [OrderState.FILLED, OrderState.PARTIAL])
        if total_filled > 0:
            avg_exit = sum(r["avg_price"] * r["filled_qty"] for r in results if r["filled_qty"] > 0) / total_filled
            pnl = (avg_exit - pos_data["avg_price"]) * total_filled * (1 if not is_buy else -1)
            self.ems_daily_pnl += pnl
            self.net_portfolio_delta -= (pos_data.get("delta", 0.0) * total_filled)
            
            if total_filled >= qty:
                self.positions.pop(scrip, None)
            else:
                self.positions[scrip]["qty"] -= total_filled
            
            await self._save_position(scrip)
            await self.safe_xadd(f"{self.ns}:execution:feedback", {"action": "CLOSED", "symbol": symbol, "pnl": pnl, "reason": reason})

    async def _handle_execution(self, msg_id: str, signal: dict):
        if self.panic_mode:
            await self.redis.xack(f"{self.ns}:signals:alpha", "ems_group", msg_id)
            return
        
        scrip = int(signal.get("scrip_code", 0))
        qty = int(signal.get("position_size_shares", 0))
        symbol = signal.get("symbol", "UNK")
        lot_size = int(signal.get("trade_specs", {}).get("lot_size", 25))
        
        if len(self.positions) >= self.config.MAX_OPEN_POSITIONS:
            await self.redis.xack(f"{self.ns}:signals:alpha", "ems_group", msg_id)
            return

        if self.ems_daily_pnl < self.config.MAX_DAILY_LOSS_LIMIT:
            await self.trigger_global_panic("MAX_LOSS_LIMIT")
            await self.redis.xack(f"{self.ns}:signals:alpha", "ems_group", msg_id)
            return

        slices = OrderIcebergSlicer.slice_order(symbol, qty, lot_size, self.config)
        results = []
        for q in slices:
            await self.sec_rate_limiter.acquire()
            res = await self.state_machine.execute_smart_order(scrip, q, True, float(signal.get("option_premium", 0)), symbol, lot_size, aggression=float(signal.get("confidence", 0.5)))
            results.append(res)

        total_filled = sum(r["filled_qty"] for r in results if r["state"] in [OrderState.FILLED, OrderState.PARTIAL])
        if total_filled > 0:
            avg_price = sum(r["avg_price"] * r["filled_qty"] for r in results if r["filled_qty"] > 0) / total_filled
            
            self.positions[scrip] = {
                "qty": total_filled, "avg_price": round(avg_price, 2), "side": "LONG", 
                "symbol": symbol, "lot_size": lot_size, "delta": float(signal.get("indep_delta", 0))
            }
            await self._save_position(scrip)
            
            async with self.stop_engine.active_stops_lock:
                self.stop_engine.active_stops[scrip] = {
                    "sl": float(signal.get("spot_sl", 0)), "qty": total_filled, 
                    "symbol": symbol, "is_long": True, "confirm_ticks": 0, "trigger_time": 0
                }
        
        await self.redis.xack(f"{self.ns}:signals:alpha", "ems_group", msg_id)

    async def _save_position(self, scrip: int):
        if scrip in self.positions:
            await self.redis.hset(f"{self.ns}:positions", str(scrip), orjson.dumps(self.positions[scrip]))
        else:
            await self.redis.hdel(f"{self.ns}:positions", str(scrip))

    async def _load_positions(self):
        data = await self.redis.hgetall(f"{self.ns}:positions")
        if data: self.positions = {int(k): orjson.loads(v) for k, v in data.items()}

    async def _process_alpha_signals(self):
        while self._running:
            try:
                streams = await self.redis.xreadgroup("ems_group", self.node_id, {f"{self.ns}:signals:alpha": ">"}, count=10, block=1000)
                if streams:
                    for _, messages in streams:
                        for mid, sig in messages: await self.signal_queue.put((mid, sig))
            except Exception: await asyncio.sleep(1)

    async def _signal_worker(self):
        while self._running:
            mid, sig = await self.signal_queue.get()
            try: await self._handle_execution(mid, sig)
            finally: self.signal_queue.task_done()

    async def safe_xadd(self, stream, payload):
        try: await self.redis.xadd(stream, payload, maxlen=1000)
        except: pass

    async def start(self):
        self._running = True
        try: await self.redis.xgroup_create(f"{self.ns}:signals:alpha", "ems_group", id="0", mkstream=True)
        except: pass
        await self._load_positions()
        
        self._tasks = [
            asyncio.create_task(self._signal_worker()),
            asyncio.create_task(self.order_cache.start_polling()),
            asyncio.create_task(self.stop_engine.listen_to_feed()),
            asyncio.create_task(self._process_alpha_signals())
        ]
        logger.info(f"🚀 Hydra EMS Online | Node: {self.node_id}")
        await asyncio.gather(*self._tasks)

    async def shutdown(self):
        self._running = False
        for t in self._tasks: t.cancel()
        await self.redis.close()
        logger.info("Shutdown Complete.")

if __name__ == "__main__":
    client = FivePaisaClient(email="", passwd="", dob="")
    ems = ExecutionManager("redis://127.0.0.1:6379/0", "HYDRA_PROD", client)
    try:
        asyncio.run(ems.start())
    except KeyboardInterrupt:
        asyncio.run(ems.shutdown())
