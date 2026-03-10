import subprocess
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
