import os

script_dir = os.path.dirname(os.path.abspath(__file__))
core_dir = os.path.abspath(os.path.join(script_dir, "..", "core"))
mod5 = os.path.join(core_dir, "module5.py")
mod6 = os.path.join(core_dir, "module6.py")

print("🛠️ Applying Indentation & Auth Patch...")

# --- Patch Module 5 ---
if os.path.exists(mod5):
    with open(mod5, "r") as f: content5 = f.read()
    idx5 = content5.find('if __name__ == "__main__":')
    if idx5 != -1:
        new_main5 = """if __name__ == "__main__":
    import asyncio
    class MockClient:
        def margin(self): return [{"NetAvailableMargin": 15000.0}]
        def positions(self): return []
        def place_order(self, **kwargs): return {"status": 0, "Message": "Success"}
        
    client = MockClient()
    ems = ExecutionManager("redis://127.0.0.1:6379/0", "HYDRA_PROD", client)
    
    try:
        asyncio.run(ems.start())
    except KeyboardInterrupt:
        pass
"""
        with open(mod5, "w") as f: f.write(content5[:idx5] + new_main5)
        print("✅ Module 5 Indentation Fixed & Auth Mocked.")

# --- Patch Module 6 ---
if os.path.exists(mod6):
    with open(mod6, "r") as f: content6 = f.read()
    idx6 = content6.find('async def main():')
    if idx6 != -1:
        new_main6 = """class MockClient:
    def margin(self): return [{"NetAvailableMargin": 15000.0}]
    def positions(self): return []
    
async def main():
    config = AlphaConfig()
    client = MockClient()
    ingestor = MarketFeedIngestor(client, config, "MOCK_JWT", "MOCK_CLIENT")
    engine = AlphaEngine(config, client)
    
    logger.info("🚀 Booting Mahoraga MTF Engine...")
    try:
        await asyncio.gather(ingestor.run(), engine.start())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    import asyncio
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
"""
        with open(mod6, "w") as f: f.write(content6[:idx6] + new_main6)
        print("✅ Module 6 NoneType Fixed & Auth Mocked.")

print("🚀 System fully patched. Ready to run.")
