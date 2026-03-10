import os
import shutil

BASE = "hydra"

folders = [
    "core",
    "config",
    "data/market/candles",
    "data/market/ticks",
    "data/trades/live",
    "data/trades/backtest",
    "data/pnl/daily",
    "data/pnl/monthly",
    "data/cache",
    "logs",
    "redis_streams",
    "scripts",
    "monitoring",
    "research/notebooks",
    "research/experiments",
    "tests"
]

modules = [
    "module1_market_feed.py",
    "module2_candle_builder.py",
    "module3_liquidity_engine.py",
    "module4_strike_selector.py",
    "module5_execution_router.py",
    "module6_position_manager.py",
    "module7_master_control.py"
]

requirements = """flask
redis
apscheduler
pytz
requests
"""

runner = """import subprocess
import sys

modules = [
    "../core/module1_market_feed.py",
    "../core/module2_candle_builder.py",
    "../core/module3_liquidity_engine.py",
    "../core/module4_strike_selector.py",
    "../core/module5_execution_router.py",
    "../core/module6_position_manager.py",
    "../core/module7_master_control.py"
]

processes = []

try:
    for module in modules:
        print(f"Starting {module}")
        p = subprocess.Popen([sys.executable, module])
        processes.append(p)

    for p in processes:
        p.wait()

except KeyboardInterrupt:
    print("Stopping Hydra...")
    for p in processes:
        p.terminate()
"""

def create_structure():
    print("Creating Hydra folder structure...")

    os.makedirs(BASE, exist_ok=True)

    for f in folders:
        path = os.path.join(BASE, f)
        os.makedirs(path, exist_ok=True)

    print("Folders created.")

def move_modules():
    print("Moving modules into core/")

    for m in modules:
        if os.path.exists(m):
            shutil.move(m, os.path.join(BASE, "core", m))
            print(f"Moved {m}")
        else:
            print(f"WARNING: {m} not found")

def create_files():
    print("Creating base files...")

    with open(os.path.join(BASE, "requirements.txt"), "w") as f:
        f.write(requirements)

    with open(os.path.join(BASE, "scripts/run_hydra.py"), "w") as f:
        f.write(runner)

    with open(os.path.join(BASE, "README.md"), "w") as f:
        f.write("# Hydra Trading System")

def main():
    create_structure()
    move_modules()
    create_files()

    print("\nHydra project ready.")
    print("Folder created:", BASE)

if __name__ == "__main__":
    main()
