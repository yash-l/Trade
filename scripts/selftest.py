import asyncio, os, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("HYDRA_DATA_MODE","demo")
os.environ.setdefault("HYDRA_MODE","paper")
SELFTEST_DB = ROOT / ".runtime" / "hydra-selftest.sqlite3"
SELFTEST_DB.parent.mkdir(parents=True, exist_ok=True)
os.environ["HYDRA_DB_PATH"] = str(SELFTEST_DB)
for suffix in ["", "-wal", "-shm"]:
    try: Path(str(SELFTEST_DB)+suffix).unlink()
    except FileNotFoundError: pass
from hydra.config import Settings
from hydra.engine import HydraEngine
async def main():
    cfg = Settings()
    assert cfg.db_path == SELFTEST_DB.resolve(), (cfg.db_path, SELFTEST_DB)
    e=HydraEngine(cfg)
    assert not e.errors,e.errors
    for _ in range(90): await e.process_candle(e.demo.next_candle())
    s=e.snapshot(); print("STATUS",s["status"],"EVENTS",len(s["events"]),"TRADES",s["trades"],"REGIME",s["regime"]["side"],"DB",cfg.db_path)
    assert s["events"]
    await e.five.close()
asyncio.run(main())
