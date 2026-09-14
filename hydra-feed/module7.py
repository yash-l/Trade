import os
import time
import logging
import platform
import concurrent.futures
import atexit
import gc 
from datetime import datetime
from flask import Flask, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import redis

# --- ⚙️ INITIALIZATION & CONFIGURATION ---

ist = pytz.timezone('Asia/Kolkata')
app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('HydraMaster')

IS_TERMUX = "com.termux" in os.environ.get("PREFIX", "")
SCHEDULER_RUNNING = False 
SCHEDULER_STARTED = False
SCHEDULER = None # Global Reference to prevent GC

logger.info("=========================================")
logger.info(" HYDRA MASTER CONTROL ONLINE ")
logger.info("=========================================")

BROKER_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=2)

@atexit.register
def shutdown_executor():
    try:
        BROKER_EXECUTOR.shutdown(wait=False)
        logger.info("Broker Executor gracefully shut down.")
    except Exception:
        pass

@atexit.register
def shutdown_scheduler():
    global SCHEDULER
    try:
        if SCHEDULER:
            SCHEDULER.shutdown(wait=False)
            logger.info("Scheduler gracefully shut down.")
    except Exception:
        pass

class MockBroker:
    def positions(self): return []
    def margin(self): return [{"AvailableMargin": 15000}]
    def place_order(self, order): return {"status": "success"}

# In production, replace MockBroker with your initialized FivePaisaClient
broker = MockBroker()

redis_url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
redis_client = redis.from_url(
    redis_url,
    decode_responses=True,
    socket_timeout=3,
    socket_connect_timeout=3,
    retry_on_timeout=True
)

NUKE_PASSWORD = os.environ.get("NUKE_PASSWORD", "changeme_immediately")

HYDRA_STATE = {
    "status": "BOOTING",
    "trading_active": False,
    "safe_mode": False
}

try:
    if redis_client.get("safe_mode") == "True":
        HYDRA_STATE["safe_mode"] = True
        logger.critical("BOOT SEQUENCE: Persistent Safe Mode Active.")
except Exception:
    logger.warning("Redis unavailable during boot.")

# --- 🔥 PROTECTED GATEKEEPER ---

def trading_allowed():
    current_time = datetime.now(ist).time()
    if current_time.hour >= 15 and current_time.minute >= 29:
        return False

    try: 
        halted = redis_client.get("trading_halted") == "True" 
    except Exception: 
        halted = False 
        
    if HYDRA_STATE["safe_mode"] or halted: 
        return False 
        
    return True 

# --- THE HYDRA CHASSIS ---

class HydraChassis:
    def __init__(self, broker_client, redis_db):
        self.broker = broker_client
        self.redis = redis_db
        self.api_failure_count = 0
        self.logger = logging.getLogger('HydraChassis')

    def safe_broker_call(self, func, *args, **kwargs): 
        try: 
            future = BROKER_EXECUTOR.submit(func, *args, **kwargs) 
            result = future.result(timeout=5) 
            self.reset_api_failures() 
            return result 
        except Exception as e: 
            self.logger.error(f"Broker API Failure: {e}") 
            self.register_api_failure() 
            return None 

    def startup_reconciliation(self): 
        self.logger.info("INITIATING STARTUP RECONCILIATION...") 
        margin_data = self.safe_broker_call(self.broker.margin) 
        live_positions = self.safe_broker_call(self.broker.positions) 
        
        if margin_data is None or live_positions is None: 
            self.trigger_circuit_breaker() 
            return 
            
        try: 
            if isinstance(margin_data, list) and len(margin_data) > 0:
                live_margin = margin_data[0].get('AvailableMargin', 15000)
            else:
                live_margin = 15000
                
            self.redis.set('dynamic_capital', live_margin) 
            self.redis.delete('active_positions') 
            
            if isinstance(live_positions, list): 
                for pos in live_positions: 
                    if pos.get('Quantity', 0) != 0: 
                        self.redis.hset('active_positions', pos['ScripCode'], pos['Quantity']) 
        except Exception as e: 
            self.logger.error(f"Reconciliation error: {e}") 
            self.trigger_circuit_breaker() 

    def register_api_failure(self): 
        self.api_failure_count += 1 
        if self.api_failure_count >= 5: 
            self.trigger_circuit_breaker() 

    def reset_api_failures(self): 
        self.api_failure_count = 0 

    def trigger_circuit_breaker(self): 
        if HYDRA_STATE["safe_mode"]: return 
        self.logger.critical("CIRCUIT BREAKER TRIPPED.") 
        HYDRA_STATE["safe_mode"] = True 
        HYDRA_STATE["trading_active"] = False 
        try: 
            self.redis.set('trading_halted', 'True') 
            self.redis.set("safe_mode", "True") 
        except Exception: 
            pass 

chassis = HydraChassis(broker, redis_client)

# --- CORE LOGIC & SCHEDULED TASKS ---

def start_trading_day():
    try:
        if redis_client.get("trading_halted") == "True" or HYDRA_STATE["safe_mode"]: return
    except Exception: pass

    HYDRA_STATE["status"] = "RECONCILING" 
    chassis.startup_reconciliation() 
    if not HYDRA_STATE["safe_mode"]: 
        HYDRA_STATE["status"] = "RUNNING" 
        HYDRA_STATE["trading_active"] = True 

def execute_hard_flatten():
    if not trading_allowed() and not HYDRA_STATE["safe_mode"]: pass

    try: 
        if not redis_client.set("flatten_lock", "1", nx=True, ex=120): return 
    except Exception: pass 
    
    HYDRA_STATE["trading_active"] = False 
    HYDRA_STATE["status"] = "FLATTENING" 
    positions = chassis.safe_broker_call(broker.positions) or [] 
    
    if isinstance(positions, list): 
        for pos in positions: 
            qty = int(pos.get("Quantity", 0)) 
            scrip = pos.get("ScripCode") 
            
            if qty != 0 and scrip is not None: 
                exit_order = {
                    "order_type": "S" if qty > 0 else "B", 
                    "scrip_code": int(scrip), 
                    "quantity": abs(qty)
                } 
                chassis.safe_broker_call(broker.place_order, exit_order) 
                
    logger.warning("Hard flatten completed.") 
    HYDRA_STATE["status"] = "SLEEPING" 
    try: 
        redis_client.delete("flatten_lock") 
    except: pass 

# --- MONITORING & MAINTENANCE ---

def system_heartbeat():
    try: redis_client.set("last_heartbeat", time.time())
    except: pass

def watchdog():
    try:
        last = redis_client.get("last_heartbeat")
        if last:
            # 🛡️ FINAL FIX: Strict Type Casting for Watchdog
            last_time = float(last)
            if time.time() - last_time > 180:
                logger.critical("WATCHDOG TRIGGERED — SYSTEM STALLED")
                chassis.trigger_circuit_breaker()
    except ValueError:
        logger.error("Corrupted heartbeat data in Redis. Ignoring tick.")
    except Exception: 
        pass

def memory_cleanup():
    gc.collect()
    logger.info("Memory GC Cleanup Performed.")

def self_ping():
    try:
        import requests
        requests.get(f"http://127.0.0.1:{os.environ.get('PORT', 10000)}/health", timeout=3)
    except Exception: pass

def redis_health_check():
    global redis_client
    try: 
        redis_client.ping()
    except Exception:
        try:
            redis_client.close()
            redis_client = redis.from_url(
                redis_url, 
                decode_responses=True, 
                socket_timeout=3,
                socket_connect_timeout=3,
                retry_on_timeout=True
            )
            chassis.redis = redis_client
        except Exception: 
            chassis.trigger_circuit_breaker()

# --- WEB & TELEMETRY ROUTES ---

@app.route('/health')
def health_check(): return jsonify({"status": "alive"}), 200

@app.route('/status')
def status():
    return jsonify({
        "engine": "HYDRA",
        "state": HYDRA_STATE["status"],
        "safe_mode": HYDRA_STATE["safe_mode"],
        "trading": HYDRA_STATE["trading_active"],
        "timestamp": time.time()
    })

@app.route('/metrics')
def system_metrics():
    try: capital = float(redis_client.get("dynamic_capital") or 15000)
    except Exception: capital = 15000
    return jsonify({"capital": capital, "safe_mode": HYDRA_STATE["safe_mode"], "trading_active": HYDRA_STATE["trading_active"]}), 200

@app.route('/nuke', methods=['POST'])
def nuke_system():
    data = request.json
    if not data or data.get("password") != NUKE_PASSWORD: return jsonify({"error": "Unauthorized"}), 403
    execute_hard_flatten()
    chassis.trigger_circuit_breaker()
    return jsonify({"status": "NUKED"}), 200

# --- MASTER SCHEDULER ---

def setup_scheduler():
    global SCHEDULER_STARTED, SCHEDULER_RUNNING, SCHEDULER
    if SCHEDULER_STARTED or SCHEDULER_RUNNING: return
    SCHEDULER_STARTED = True
    SCHEDULER_RUNNING = True

    SCHEDULER = BackgroundScheduler(timezone=ist, daemon=True) 
    scheduler = SCHEDULER
    
    scheduler.add_job(start_trading_day, 'cron', id="start_day", replace_existing=True, day_of_week='mon-fri', hour=8, minute=58) 
    scheduler.add_job(execute_hard_flatten, 'cron', id="flatten1", replace_existing=True, day_of_week='mon-fri', hour=15, minute=25) 
    scheduler.add_job(execute_hard_flatten, 'cron', id="flatten2", replace_existing=True, day_of_week='mon-fri', hour=15, minute=29) 
    scheduler.add_job(system_heartbeat, 'interval', id="heartbeat", replace_existing=True, minutes=1) 
    scheduler.add_job(watchdog, 'interval', id="watchdog", replace_existing=True, minutes=3) 
    scheduler.add_job(redis_health_check, 'interval', id="redis_health", replace_existing=True, minutes=3) 
    scheduler.add_job(memory_cleanup, 'interval', id="gc_clean", replace_existing=True, minutes=30) 
    scheduler.add_job(self_ping, 'interval', id="self_ping", replace_existing=True, minutes=10) 
    scheduler.start() 
    logger.info("Scheduler Locked and Initialized.") 

if not IS_TERMUX:
    try:
        if redis_client.set("scheduler_lock", "1", nx=True, ex=10):
            setup_scheduler()
            if 9 <= datetime.now(ist).hour < 15: start_trading_day()
    except Exception: 
        setup_scheduler()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), debug=False, use_reloader=False)
