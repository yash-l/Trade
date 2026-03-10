import time
import asyncio
import logging
from enum import Enum
from dataclasses import dataclass
from collections import deque, OrderedDict
from datetime import datetime
from zoneinfo import ZoneInfo
import redis.asyncio as aioredis
import orjson
import websockets

# --- CONFIGURATION & LOGGING ---
logger = logging.getLogger("FeedGuardian")
logging.basicConfig(level=logging.INFO, format='%(message)s')
IST = ZoneInfo("Asia/Kolkata")

class WSState(Enum):
    DISCONNECTED = 1
    CONNECTING = 2
    AUTHENTICATED = 3
    SUBSCRIBED = 4
    STREAMING = 5
    STREAM_ERROR = 6

@dataclass
class FeedConfig:
    WS_URL: str = "wss://openfeed.5paisa.com/Feeds/api/chat"
    MAX_RECONNECT_ATTEMPTS: int = 10
    BASE_RECONNECT_DELAY_SEC: float = 0.5
    MAX_RECONNECT_DELAY_SEC: float = 60.0
    WATCHDOG_TIMEOUT_SEC: float = 5.0
    STREAM_MAXLEN: int = 5000
    BUFFER_MAX_SIZE: int = 10000
    MAX_TPS: int = 20000  # Tuned for high throughput VPS

# --- DATA CLEANING & PARSING ---
class TickSanitizer:
    def __init__(self):
        self._last_ticks = OrderedDict()
        self.max_cache_size = 20000

    def _parse_5paisa_time(self, time_str: str) -> float:
        """Parses 5paisa's /Date(1680000000000)/ format lightning fast"""
        try:
            if time_str and time_str.startswith("/Date("):
                return int(time_str[6:-2]) / 1000.0
            return time.time()
        except Exception:
            return time.time()

    def sanitize(self, raw_tick: dict) -> dict | None:
        try:
            scrip = raw_tick.get("Token")
            if not scrip: return None
            
            scrip = int(scrip)
            ltp = float(raw_tick.get("LastRate", 0))
            bid = float(raw_tick.get("BestBidRate", 0))
            ask = float(raw_tick.get("BestOfferRate", 0))
            vol = int(raw_tick.get("TotalQty", 0))
            ts = self._parse_5paisa_time(raw_tick.get("Time", ""))

            # Physics Sanity Checks
            if ltp <= 0 or bid <= 0 or ask <= 0: return None
            if bid >= ask: return None
            
            now = time.time()
            if ts > now + 1.5 or ts < now - 5.0: return None # Latency protection

            clean_tick = {
                "ltp": ltp, "bid": bid, "ask": ask, 
                "vol": vol, "ts": ts
            }

            # Deduplication Engine (Drops tick if identical to last known state)
            last_tick = self._last_ticks.get(scrip)
            if last_tick and (
                last_tick["ltp"] == clean_tick["ltp"] and 
                last_tick["bid"] == clean_tick["bid"] and 
                last_tick["ask"] == clean_tick["ask"] and
                last_tick["vol"] == clean_tick["vol"]
            ):
                return None

            self._last_ticks[scrip] = clean_tick
            self._last_ticks.move_to_end(scrip)
            if len(self._last_ticks) > self.max_cache_size:
                self._last_ticks.popitem(last=False)

            return {"scrip_code": scrip, "data": clean_tick}

        except Exception:
            return None

# --- CORE ENGINE ---
class MarketFeedGuardian:
    def __init__(self, redis_url: str, client_code: str, auth_token: str, req_data: list[dict]):
        self.config = FeedConfig()
        self.state = WSState.DISCONNECTED
        self.redis_url = redis_url
        self.redis = None
        
        # Auth & Subscriptions
        self.client_code = client_code
        self.auth_token = auth_token
        self.req_data = req_data 
        
        self.sanitizer = TickSanitizer()
        self.ws = None
        self.last_tick_time = time.time()
        self.reconnect_attempts = 0
        
        self.local_buffer = deque(maxlen=self.config.BUFFER_MAX_SIZE)
        self.metrics = {"processed_ticks": 0, "dropped_ticks": 0, "reconnects": 0}
        
        self._running = False
        self._tasks = []
        
        self.tick_window = time.time()
        self.tick_count_window = 0

    def _log(self, level: int, event: str, **context):
        payload = {"ts": datetime.now(IST).isoformat(), "module": "FeedGuardian", "event": event, **context}
        logger.log(level, orjson.dumps(payload).decode('utf-8'))

    def _is_market_open(self) -> bool:
        """Market Hours Guard: 9:00 AM to 3:30 PM IST"""
        now = datetime.now(IST)
        if now.weekday() >= 5: # Saturday/Sunday Check
            return False 
        time_val = now.hour * 100 + now.minute
        return 900 <= time_val <= 1530

    async def start(self):
        self._running = True
        self.redis = await aioredis.from_url(self.redis_url, decode_responses=True, max_connections=200)
        
        # Start Background Workers
        self._tasks.append(asyncio.create_task(self._watchdog()))
        self._tasks.append(asyncio.create_task(self._heartbeat())) 
        self._tasks.append(asyncio.create_task(self._telemetry_logger()))
        self._tasks.append(asyncio.create_task(self._flush_local_buffer()))
        
        await self._connect()

    async def stop(self):
        self._log(logging.WARNING, "SHUTDOWN_INITIATED", msg="Initiating graceful shutdown...")
        self._running = False
        if self.ws:
            try: await asyncio.wait_for(self.ws.close(), timeout=2.0)
            except: pass
        for task in self._tasks: task.cancel()
        await self._flush_local_buffer(force=True)
        if self.redis: await self.redis.close()
        self._log(logging.WARNING, "SHUTDOWN_COMPLETE")

    async def _connect(self):
        while self.reconnect_attempts < self.config.MAX_RECONNECT_ATTEMPTS and self._running:
            
            # The Market Guard
            if not self._is_market_open():
                self._log(logging.INFO, "MARKET_CLOSED", msg="Outside trading hours. Sleeping for 60s.")
                await asyncio.sleep(60)
                continue

            self.state = WSState.CONNECTING
            try:
                # Compression Deflate for bandwidth efficiency
                self.ws = await websockets.connect(self.config.WS_URL, ping_interval=None, compression="deflate")
                
                await self._authenticate_and_subscribe()
                
                self.reconnect_attempts = 0
                self.state = WSState.STREAMING
                self.last_tick_time = time.time()
                self._log(logging.INFO, "WS_STREAMING", msg="Live data connection secured.")
                
                await self._receive_loop()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.state = WSState.STREAM_ERROR
                self.reconnect_attempts += 1
                self.metrics["reconnects"] += 1
                
                delay = min(self.config.BASE_RECONNECT_DELAY_SEC * (2 ** (self.reconnect_attempts - 1)), self.config.MAX_RECONNECT_DELAY_SEC)
                self._log(logging.ERROR, "WS_ERROR", error=str(e), attempt=self.reconnect_attempts, next_retry_sec=delay)
                await asyncio.sleep(delay)

    async def _authenticate_and_subscribe(self):
        # --- LOGIN PHASE ---
        auth_payload = {
            "Method": "Login", "Operation": "MarketData", 
            "ClientCode": self.client_code, "AuthToken": self.auth_token
        }
        await self.ws.send(orjson.dumps(auth_payload).decode('utf-8'))
        self._log(logging.INFO, "AUTH_SENT")

        try:
            auth_resp = await asyncio.wait_for(self.ws.recv(), timeout=5.0)
            resp_data = orjson.loads(auth_resp)
            
            if isinstance(resp_data, dict) and resp_data.get("Status") == 0:
                self.state = WSState.AUTHENTICATED
                self._log(logging.INFO, "AUTH_SUCCESS", msg=resp_data.get("Message", "OK"))
            else:
                self._log(logging.ERROR, "AUTH_REJECTED", response=resp_data)
                raise ConnectionError("Broker rejected AuthToken. Might be expired.")
        except asyncio.TimeoutError:
            raise ConnectionError("Timeout waiting for 5paisa Auth confirmation.")

        # --- SUBSCRIPTION PHASE ---
        sub_payload = {
            "Method": "Subscribe", "Operation": "MarketData", 
            "ReqData": self.req_data
        }
        await self.ws.send(orjson.dumps(sub_payload).decode('utf-8'))
        
        try:
            sub_resp = await asyncio.wait_for(self.ws.recv(), timeout=5.0)
            resp_data = orjson.loads(sub_resp)
            
            if isinstance(resp_data, dict) and resp_data.get("Status") == 0:
                self.state = WSState.SUBSCRIBED
                self._log(logging.INFO, "SUB_ACK_RECEIVED", msg=resp_data.get("Message", "Subscribed"))
            else:
                self._log(logging.WARNING, "SUB_ACK_FAILED", response=resp_data)
        except asyncio.TimeoutError:
            self._log(logging.WARNING, "SUB_ACK_TIMEOUT", msg="Proceeding without explicit sub ACK.")
            self.state = WSState.SUBSCRIBED
            
        self._log(logging.INFO, "SUBSCRIPTION_SENT", count=len(self.req_data))

    async def _heartbeat(self):
        """Heartbeat Ping: Keeps 5paisa from silently killing the socket."""
        while self._running:
            await asyncio.sleep(10)
            if self.state == WSState.STREAMING and self.ws:
                try:
                    await self.ws.ping()
                except Exception as e:
                    self._log(logging.DEBUG, "HEARTBEAT_FAIL", error=str(e))

    async def _receive_loop(self):
        async for message in self.ws:
            if not self._running: break
            
            now = time.time()
            if now - self.tick_window >= 1.0:
                self.tick_window = now
                self.tick_count_window = 0
                
            if self.tick_count_window > self.config.MAX_TPS:
                self.metrics["dropped_ticks"] += 1
                continue 

            self.last_tick_time = now
            
            try:
                raw_data = orjson.loads(message)
                
                # Filter pure ACKs
                if isinstance(raw_data, dict) and "Status" in raw_data:
                    if raw_data.get("Status") == 0:
                        self._log(logging.INFO, "BROKER_ACK_RECEIVED")
                    continue

                ticks = raw_data if isinstance(raw_data, list) else [raw_data]
                
                # Intra-batch deduplication
                if len(ticks) > 500:
                    ticks = list({t.get("Token"): t for t in ticks if t.get("Token")}.values())
                
                valid_ticks = []
                for raw_tick in ticks:
                    self.tick_count_window += 1
                    clean_tick = self.sanitizer.sanitize(raw_tick)
                    if not clean_tick:
                        continue
                    valid_ticks.append(clean_tick)
                    self.metrics["processed_ticks"] += 1
                    
                if valid_ticks:
                    await self._route_batch_to_redis(valid_ticks)
                    
            except Exception as e:
                self._log(logging.DEBUG, "PARSE_ERROR", error=str(e))

    async def _route_batch_to_redis(self, clean_ticks: list):
        if not self.redis:
            for ct in clean_ticks: self.local_buffer.append((f"ticks:{ct['scrip_code']}", ct["data"]))
            return
            
        try:
            pipe = self.redis.pipeline(transaction=False)
            for ct in clean_ticks:
                stream_key = f"ticks:{ct['scrip_code']}"
                pipe.xadd(stream_key, ct["data"], maxlen=self.config.STREAM_MAXLEN, approximate=True)
            await asyncio.wait_for(pipe.execute(), timeout=1.0)
        except Exception:
            for ct in clean_ticks:
                self.local_buffer.append((f"ticks:{ct['scrip_code']}", ct["data"]))

    async def _flush_local_buffer(self, force=False):
        """Rescues buffered data if Redis disconnected briefly."""
        while self._running or force:
            if self.local_buffer and self.redis:
                try:
                    pipe = self.redis.pipeline(transaction=False)
                    count = 0
                    while self.local_buffer and count < 500:
                        stream_key, tick_data = self.local_buffer.popleft()
                        pipe.xadd(stream_key, tick_data, maxlen=self.config.STREAM_MAXLEN, approximate=True)
                        count += 1
                    if count > 0:
                        await asyncio.wait_for(pipe.execute(), timeout=1.0)
                except Exception:
                    pass 
                    
            if force and not self.local_buffer: break
            elif force and not self.redis: break
                
            if not force: await asyncio.sleep(0.1)

    async def _watchdog(self):
        while self._running:
            await asyncio.sleep(1)
            
            # --- Redis Health Check ---
            if self.redis:
                try: 
                    await asyncio.wait_for(self.redis.ping(), timeout=1.0)
                except Exception as e: 
                    self._log(logging.ERROR, "REDIS_DEAD", error=str(e), msg="Attempting aggressive Redis reconnect")
                    try: await self.redis.close()
                    except: pass
                    try: self.redis = await aioredis.from_url(self.redis_url, decode_responses=True, max_connections=200)
                    except: pass

            # --- WSS Connection Health Check ---
            if self.state == WSState.STREAMING:
                time_since_last = time.time() - self.last_tick_time
                if time_since_last > self.config.WATCHDOG_TIMEOUT_SEC:
                    self._log(logging.WARNING, "WATCHDOG_TRIP", msg=f"No ticks for {time_since_last:.1f}s. Reconnecting.")
                    if self.ws:
                        try: await asyncio.wait_for(self.ws.close(), timeout=2.0)
                        except: pass

    async def _telemetry_logger(self):
        while self._running:
            await asyncio.sleep(10)
            rate = self.metrics["processed_ticks"] / 10.0
            self._log(
                logging.INFO, "SENSOR_TELEMETRY",
                tick_rate_sec=round(rate, 1),
                reconnects=self.metrics["reconnects"],
                buffer_size=len(self.local_buffer)
            )
            self.metrics["processed_ticks"] = 0

# --- RUN BLOCK ---
if __name__ == "__main__":
    # Required 5paisa Subscription Format
    subscriptions = [
        {"Exch": "N", "ExchType": "C", "ScripCode": 2885}, # Reliance Example
        {"Exch": "N", "ExchType": "C", "ScripCode": 11536} # TCS Example
    ]
    
    guardian = MarketFeedGuardian(
        redis_url="redis://127.0.0.1:6379/0", 
        client_code="YOUR_CLIENT_CODE", 
        auth_token="YOUR_DECRYPTED_POSTGRES_TOKEN",
        req_data=subscriptions
    )
    
    try:
        asyncio.run(guardian.start())
    except KeyboardInterrupt:
        asyncio.run(guardian.stop())
