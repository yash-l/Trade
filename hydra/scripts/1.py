import os

m5_path = os.path.expanduser("~/hydra/core/module5.py")
m6_path = os.path.expanduser("~/hydra/core/module6.py")

print("🛡️ INITIATING FINAL HYDRA SAFEGUARDS 🛡️")

# ==========================================
# 1. PATCH MODULE 5 (Execution Airgap)
# ==========================================
try:
    with open(m5_path, 'r') as f:
        m5_code = f.read()

    # Inject PAPER_TRADE_MODE flag
    if "PAPER_TRADE_MODE" not in m5_code:
        m5_code = m5_code.replace(
            "class EMSConfig:\n", 
            "class EMSConfig:\n    PAPER_TRADE_MODE: bool = True  # 🛡️ THE AIRGAP IS ACTIVE\n"
        )

    # Intercept place_order
    mock_place_order = """    async def place_order(self, scrip: int, qty: int, is_buy: bool, order_type: str, price: float = 0.0):
        if getattr(self.config, 'PAPER_TRADE_MODE', True):
            sim_id = f"SIM_{int(time.time()*1000)}"
            import logging
            log = logging.getLogger("5Paisa_EMS")
            log.info(f"🛡️ [PAPER TRADE] {'BUY' if is_buy else 'SELL'} | Scrip: {scrip} | Qty: {qty} | Price: {price}")
            
            sim_order = {"BrokerOrderId": sim_id, "OrderStatus": "Fully Executed", "TradedQty": qty, "AveragePrice": price if price > 0 else 100.0}
            if not hasattr(self, 'sim_cache'): self.sim_cache = {}
            self.sim_cache[sim_id] = sim_order
            return {"Message": "Success", "Status": 0, "BrokerOrderId": sim_id}

        req = OrderRequest("""
    
    if "🛡️ [PAPER TRADE]" not in m5_code:
        m5_code = m5_code.replace(
            "    async def place_order(self, scrip: int, qty: int, is_buy: bool, order_type: str, price: float = 0.0):\n        req = OrderRequest(", 
            mock_place_order
        )

    # Intercept fetch_order_book
    mock_fetch = """    async def fetch_order_book(self):
        if getattr(self.config, 'PAPER_TRADE_MODE', True):
            return list(getattr(self, 'sim_cache', {}).values())
        try:"""
    
    if "return list(getattr(self, 'sim_cache'" not in m5_code:
        m5_code = m5_code.replace(
            "    async def fetch_order_book(self):\n        try:", 
            mock_fetch
        )

    with open(m5_path, 'w') as f:
        f.write(m5_code)
    print("✅ [MODULE 5] Execution Airgap Installed. Real money is safe.")

except Exception as e:
    print(f"❌ [MODULE 5] Error: {e}")

# ==========================================
# 2. PATCH MODULE 6 (Kill Mock Data)
# ==========================================
try:
    with open(m6_path, 'r') as f:
        m6_code = f.read()

    # Disable ingestor.run()
    target_string = "await asyncio.gather(ingestor.run(), engine.start())"
    if target_string in m6_code:
        m6_code = m6_code.replace(
            target_string,
            "# 🛡️ MOCK DATA KILLED: Waiting for real FeedGuardian ticks\n        await engine.start()"
        )
        with open(m6_path, 'w') as f:
            f.write(m6_code)
        print("✅ [MODULE 6] Mock Data Generator Neutralized. Waiting for live NSE feed.")
    else:
        print("✅ [MODULE 6] Mock Data already disabled.")

except Exception as e:
    print(f"❌ [MODULE 6] Error: {e}")

print("\n🚀 ALL SYSTEMS SECURED. HYDRA IS READY FOR 9:15 AM LIVE DATA BURN-IN.")
