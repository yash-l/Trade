        try:
            depth = self.client.market_depth('N', 'D', order["ScripCode"])
            if not depth: depth = self.client.market_depth('N', 'C', order["ScripCode"])
            
            if depth and len(depth) > 0:
                live_price = float(depth[0].get('LastRate', 0))
                if live_price == 0: live_price = float(depth[0].get('BuyRate', 0))
                
                if live_price > 0:
                    # REALISM: Add Slippage (Penalty)
                    # Buy at Price + 2, Sell at Price - 2
                    slippage = 2.0 if order['Type'] == 'B' else -2.0
                    fill_price = live_price + slippage
                    
                    order["Status"] = "Filled"
                    order["FillPrice"] = fill_price
                    return fill_price
        except:
            pass
            
        return 0

    def square_all_positions(self):
        print("   🧹 [PAPER] POSITIONS CLEARED.")
        self.orders = {}
EOF

cat << 'EOF' > paper_titan/core/cortex.py
import datetime
import time

class Cortex:
    """
    CORTEX V9.8 (TITAN FINAL - PAPER EDITION)
    """
    def __init__(self, client):
        self.client = client
        self.NIFTY_SPOT_CODE = 99992000
        self.prev_close = 0
        self.is_expiry = False

    def find_scrip_code(self, symbol_name):
        try:
            response = self.client.search_scrip('N', symbol_name)
            if response and 'body' in response and len(response['body']) > 0:
                data = response['body'][0]
                return data['ScripCode'], data['Name']
            return None, None
        except:
            return None, None

    def get_verified_expiry(self):
        today = datetime.date.today()
        days_ahead = 3 - today.weekday()
        if days_ahead < 0: days_ahead += 7
        
        candidates = []
        d1 = today + datetime.timedelta(days=days_ahead)
        candidates.append(d1)
        candidates.append(d1 - datetime.timedelta(days=1)) 
        
        for date_obj in candidates:
            date_str = date_obj.strftime("%d %b %Y").upper()
            test_name = f"NIFTY {date_str} 25000 CE"
            code, _ = self.find_scrip_code(test_name)
            if code:
                return date_str, (date_obj == today)
        return None, False

    def find_futures_chain(self, expiry_str):
        formats = [
            f"NIFTY {expiry_str} FUT",
            f"NIFTY {expiry_str.split()[1]} FUT"
        ]
        for name in formats:
            code, real_name = self.find_scrip_code(name)
            if code:
                return code
        return None

    def get_market_data(self, scrip_code):
        try:
            depth = self.client.market_depth('N', 'D', scrip_code)
            if not depth: depth = self.client.market_depth('N', 'C', scrip_code)
            
            if depth and isinstance(depth, list) and len(depth) > 0:
                d = depth[0]
                ltp = d.get('LastRate', 0) or d.get('BuyRate', 0)
                pc = (d.get('PreviousClose') or d.get('PrevClose') or d.get('Close') or 0)
                return float(ltp), float(pc)
        except:
            pass
        return 0, 0

    def auto_configure(self):
        print("\n🧠 CORTEX V9.8: Initializing Paper Session...")
        
        expiry_date, self.is_expiry = self.get_verified_expiry()
        if not expiry_date: return None

        if self.is_expiry: 
            print(f"   ⚠️ EXPIRY DETECTED: {expiry_date}")
        else: 
            print(f"   📅 Expiry: {expiry_date}")

        fut_code = self.find_futures_chain(expiry_date)
        ltp = 0
        if fut_code:
            ltp, self.prev_close = self.get_market_data(fut_code)
        
        if ltp == 0:
            print("   ⚠️ FALLBACK TO SPOT.")
            ltp, self.prev_close = self.get_market_data(self.NIFTY_SPOT_CODE)
            if not fut_code: fut_code = self.NIFTY_SPOT_CODE 
        
        if ltp == 0:
            print("❌ FATAL: NO PRICE DATA.")
            return None 

        gap = ltp - self.prev_close if self.prev_close > 0 else 0
        print(f"   🎯 Ref Price: {ltp} (Gap: {gap:+.0f})")
        
        atm = round(ltp / 50) * 50
        hedge_dist = 200 if self.is_expiry else 300
        
        ce_name = f"NIFTY {expiry_date} {atm} CE"
        pe_name = f"NIFTY {expiry_date} {atm} PE"
        h_ce_name = f"NIFTY {expiry_date} {atm + hedge_dist} CE"
        h_pe_name = f"NIFTY {expiry_date} {atm - hedge_dist} PE"

        codes = {
            "fut": fut_code,
            "ce": self.find_scrip_code(ce_name)[0],
            "pe": self.find_scrip_code(pe_name)[0],
            "hedge_ce": self.find_scrip_code(h_ce_name)[0],
            "hedge_pe": self.find_scrip_code(h_pe_name)[0],
            "gap": gap,
            "atm_price": atm
        }

        required = ["fut", "ce", "pe", "hedge_ce", "hedge_pe"]
        if not all(codes.get(k) for k in required):
            print(f"❌ FATAL: MISSING STRIKES.")
            return None

        print(f"   ✅ ARMED: {codes['fut']}")
        return codes
EOF

cat << 'EOF' > paper_titan/core/strategy.py
import datetime
import time
import os

class MahoragaEngine:
    """
    CFC-9.8 TITAN PAPER (Logic Twin)
    - Exact replica of the Live Strategy.
    - Saves data to 'paper_results.csv'
    """

    def __init__(self, broker):
        self.broker = broker
        self.alive = True
        
        # SAVE TO PAPER FILE
        self.memory_file = "paper_results.csv"
        self.init_memory()
        self.codes = {}
        
        # RISK
        self.running_pnl = 0.0       
        self.max_daily_loss = -500   
        self.base_qty = 50           
        self.current_qty = 50        
        self.realized_main = 0.0     
        self.atm_price = 25000       
        
        self.reset_full_state()
        self.last_tick_time = time.time()
        
        # TIME
        self.orb_end = datetime.time(9, 18)    
        self.entry_cutoff = datetime.time(10, 0) 
        self.trap_window = 120 

    def init_memory(self):
        if not os.path.exists(self.memory_file):
            with open(self.memory_file, 'w') as f:
                f.write("Date,Time,Type,Signal,PnL,Reason\n")

    def reset_full_state(self):
        self.trade_taken = False
        self.trade_type = None
        self.entry_time = None
        self.tp1_booked = False
        self.reversal_taken = False
        
        self.main_entry = 0
        self.hedge_entry = 0
        self.best_ltp = 0
        self.current_sl = 0
        self.current_qty = self.base_qty
        
        self.vwap_sum_pv = 0
        self.vwap_sum_v = 0
        self.vwap_value = 0
        self.vwap_active = True
        
        self.fut_high = 0
        self.fut_low = float('inf')
        self.orb_buffer = 2.0

    def log_trade(self, type, signal, pnl, reason):
        with open(self.memory_file, 'a') as f:
            now = datetime.datetime.now()
            f.write(f"{now.date()},{now.time()},{type},{signal},{pnl},{reason}\n")

    def configure(self, config_dict):
        if not config_dict or not isinstance(config_dict, dict): return False
        self.codes = config_dict
        self.atm_price = config_dict.get('atm_price', 25000)
        return True

    def now(self): return datetime.datetime.now()

    def process_tick(self, tick):
        if not self.alive: return
        self.last_tick_time = time.time()
        if not self.codes: return 
        try:
            data = tick[0]
            scrip = data.get("ScripCode")
            ltp = data.get("LastRate")
            qty = data.get("LastQty", 0) 
            if not ltp: return

            if scrip == self.codes['fut']:
                if qty > 0:
                    self.vwap_sum_pv += (ltp * qty)
                    self.vwap_sum_v += qty
                    if self.vwap_sum_v > 0: self.vwap_value = self.vwap_sum_pv / self.vwap_sum_v
                else:
                    if self.vwap_active: self.vwap_active = False
                self.process_signal(ltp)
                return

            if self.trade_taken:
                target = self.codes['ce'] if self.trade_type == "CE" else self.codes['pe']
                if scrip == target: self.manage_trade(ltp)
        except: pass

    def process_signal(self, ltp):
        if self.trade_taken: return
        now_t = self.now().time()
        
        if self.running_pnl <= self.max_daily_loss:
            print(f"\r🛑 MAX LOSS. HARD KILL.")
            self.shutdown_system(hard_kill=True)
            return

        if now_t < self.orb_end:
            self.fut_high = max(self.fut_high, ltp)
            self.fut_low = min(self.fut_low, ltp)
            print(f"\r🕯️ CALIB: {self.fut_high:.0f}/{self.fut_low:.0f} | VWAP: {self.vwap_value:.0f}   ", end="")
            return
        
        if now_t > self.entry_cutoff:
            self.shutdown_system(hard_kill=False)
            return

        rng = self.fut_high - self.fut_low
        self.orb_buffer = max(2.0, rng * 0.1)
        vwap_ok = True
        if self.vwap_active and self.vwap_sum_v > (self.base_qty * 100):
            if ltp > (self.fut_high + self.orb_buffer): vwap_ok = (ltp > self.vwap_value)
            elif ltp < (self.fut_low - self.orb_buffer): vwap_ok = (ltp < self.vwap_value)
        
        if ltp > (self.fut_high + self.orb_buffer):
            if vwap_ok: self.enter_sequence("CE")
        elif ltp < (self.fut_low - self.orb_buffer):
            if vwap_ok: self.enter_sequence("PE")

    def enter_sequence(self, type):
        print(f"\n🚀 SIGNAL: {type}")
        self.hedge_scrip = self.codes['hedge_ce'] if type == "CE" else self.codes['hedge_pe']
        h_id = self.broker.place_order_safe('B', 'N', 'D', self.hedge_scrip, self.base_qty, 0, True)
        self.hedge_entry = self.confirm_fill(h_id)
        
        if self.hedge_entry <= 0: return

        iv_limit = self.atm_price * 0.12
        if self.hedge_entry > iv_limit: 
            print(f"❌ HIGH IV ABORT.")
            self.broker.place_order_safe('S', 'N', 'D', self.hedge_scrip, self.base_qty, 0, True)
            return

        main_scrip = self.codes['ce'] if type == "CE" else self.codes['pe']
        m_id = self.broker.place_order_safe('B', 'N', 'D', main_scrip, self.base_qty, 0, True)
        self.main_entry = self.confirm_fill(m_id, fallback_ltp=0)
        
        if self.main_entry <= 0:
             print("❌ MAIN FILL FAILED. UNWIND.")
             self.broker.place_order_safe('S', 'N', 'D', self.hedge_scrip, self.base_qty, 0, True)
             return

        self.trade_taken = True
        self.trade_type = type
        self.current_qty = self.base_qty
        self.entry_time = time.time()
        self.best_ltp = self.main_entry
        self.current_sl = self.main_entry - 10.0
        self.realized_main = 0.0 
        self.tp1_booked = False
        print(f"⚡ ENTRY: {self.main_entry} | SL: {self.current_sl}")

    def confirm_fill(self, order_id, fallback_ltp=0):
        for _ in range(6): 
            p = self.broker.get_real_fill_price(order_id)
            if p > 0: return p
            time.sleep(0.5)
        return fallback_ltp 

    def manage_trade(self, ltp):
        pnl_pts = ltp - self.main_entry
        if not self.tp1_booked and pnl_pts >= 20.0:
            qty_out = 25
            print(f"\n💰 TP1 (+20). BOOKING {qty_out}.")
            scrip = self.codes['ce'] if self.trade_type == "CE" else self.codes['pe']
            s_id = self.broker.place_order_safe('S', 'N', 'D', scrip, qty_out, 0, True)
            fill_p = self.confirm_fill(s_id, fallback_ltp=ltp)
            self.realized_main += (fill_p - self.main_entry) * qty_out
            self.current_qty -= qty_out
            self.tp1_booked = True
            self.current_sl = self.main_entry + 1.0 
            print(f"🛡️ SL TO COST.")

        if ltp > self.best_ltp:
            self.best_ltp = ltp
            if pnl_pts > 30.0:
                new_sl = ltp - 10.0
                if new_sl > self.current_sl:
                    self.current_sl = new_sl
                    print(f"\r⚡ TRAIL: {self.current_sl:.1f}", end="")

        if ltp <= self.current_sl:
            print(f"\n🛑 SL HIT.")
            self.close_all("SL")
        elif pnl_pts >= 60.0:
            print(f"\n💰 RUNNER HIT.")
            self.close_all("TARGET")

    def close_all(self, reason):
        scrip = self.codes['ce'] if self.trade_type == "CE" else self.codes['pe']
        m_id = self.broker.place_order_safe('S', 'N', 'D', scrip, self.current_qty, 0, True)
        main_exit = self.confirm_fill(m_id, fallback_ltp=self.best_ltp)
        h_id = self.broker.place_order_safe('S', 'N', 'D', self.hedge_scrip, self.base_qty, 0, True)
        hedge_exit = self.confirm_fill(h_id, fallback_ltp=self.hedge_entry) 
        
        net = (self.realized_main + (main_exit - self.main_entry) * self.current_qty) + ((hedge_exit - self.hedge_entry) * self.base_qty)
        self.running_pnl += net
        self.log_trade(self.trade_type, "EXIT", net, reason)
        print(f"🏁 NET PnL: {net:.0f}")

        elapsed = time.time() - self.entry_time
        if (reason == "SL" and not self.reversal_taken and not self.tp1_booked and elapsed < self.trap_window and self.running_pnl > self.max_daily_loss):
                print(f"\n🦅 REVERSING...")
                new_type = "PE" if self.trade_type == "CE" else "CE"
                self.entry_cutoff = self.now().time()
                self.trade_taken = False
                self.tp1_booked = False
                self.reversal_taken = True
                self.current_qty = self.base_qty 
                self.main_entry = 0
                self.hedge_entry = 0
                self.enter_sequence(new_type)
                return 
        self.shutdown_system(hard_kill=False)

    def shutdown_system(self, hard_kill=False):
        print("🛑 SYSTEM HALTED.")
        self.alive = False
        if hard_kill: self.broker.square_all_positions()
        self.reset_full_state()
EOF

cat << 'EOF' > paper_titan/main.py
from core.silence import silence_all_output, restore_output
silence_all_output()

from core.session import Session
# IMPORT PAPER BROKER INSTEAD OF REAL BROKER
from core.paper_broker import PaperBroker 
from core.strategy import MahoragaEngine
from core.websocket import MahoragaStream
from core.cortex import Cortex
import time
import sys
import threading

def run_bot():
    client = Session.client()
    
    # USE PAPER BROKER
    broker = PaperBroker(client) 
    cortex = Cortex(client)
    
    restore_output()
    print("---------------------------------------")
    print("📝 TITAN PAPER TRADING (SIMULATION MODE)")
    print("   Capital: ₹15,000 (Virtual)")
    print("   Data:    LIVE MARKET FEED")
    print("---------------------------------------")
    
    broker.square_all_positions()

    config = cortex.auto_configure()
    if not config: 
        print("❌ CONFIG FAILED.")
        return False

    engine = MahoragaEngine(broker)
    if not engine.configure(config):
        return False
        
    stream = MahoragaStream(client, engine)
    
    def start_stream():
        req = [
            {"Exch":"N","ExchType":"D","ScripCode":int(config['fut'])},
            {"Exch":"N","ExchType":"D","ScripCode":int(config['ce'])},
            {"Exch":"N","ExchType":"D","ScripCode":int(config['pe'])},
            {"Exch":"N","ExchType":"D","ScripCode":int(config['hedge_ce'])},
            {"Exch":"N","ExchType":"D","ScripCode":int(config['hedge_pe'])}
        ]
        client.Request_Feed('mf', 's', req)
        client.connect(stream.on_message)

    t = threading.Thread(target=start_stream, daemon=True)
    t.start()
    
    print("\n✅ PAPER BOT ARMED.")

    try:
        while engine.alive:
            time.sleep(1)
            # PHOENIX WATCHDOG
            if (time.time() - engine.last_tick_time) > 10.0:
                print("⚠️ WATCHDOG TIMEOUT.")
                return False 
    except:
        return False
        
    return True 

def main():
    while True:
        if run_bot(): 
            print("🏁 PAPER SESSION END.")
            break
        print("🔄 RETRYING...")
        time.sleep(5)

if __name__ == "__main__":
    main()
EOF

cd paper_titan
python main.py
# 1. Create the silence.py file directly (Repair)
cat << 'EOF' > core/silence.py
import sys
import os

def silence_all_output():
    try:
        sys.stdout = open(os.devnull, 'w')
        sys.stderr = open(os.devnull, 'w')
    except:
        pass

def restore_output():
    try:
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
    except:
        pass
EOF

# 2. Copy the missing Login and Websocket files from your main bot
# We assume your main bot is in the folder above (..)
cp ../core/session.py core/
cp ../core/websocket.py core/
# 3. Create the empty __init__ file just in case
touch core/__init__.py
echo "✅ REPAIR COMPLETE. Try running main.py again."
python main.py
python3 main.py
ls
exit
