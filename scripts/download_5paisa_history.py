import argparse, asyncio, json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pathlib import Path
from hydra.config import Settings
from hydra.broker.fivepaisa import FivePaisaAdapter
async def run(a):
    f=FivePaisaAdapter(Settings()); data=await f.historical(a.exch,a.exch_type,a.scrip,a.interval,a.start,a.end); await f.close()
    p=Path(a.out); p.parent.mkdir(parents=True,exist_ok=True); p.write_text(json.dumps(data,indent=2)); print(p)
p=argparse.ArgumentParser(); p.add_argument('--scrip',type=int,required=True); p.add_argument('--start',required=True); p.add_argument('--end',required=True); p.add_argument('--interval',default='1m'); p.add_argument('--exch',default='N'); p.add_argument('--exch-type',default='C'); p.add_argument('--out',default='data/history.json'); a=p.parse_args(); asyncio.run(run(a))
