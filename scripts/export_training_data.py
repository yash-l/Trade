#!/usr/bin/env python3
from __future__ import annotations
import argparse,csv,json,sqlite3
from pathlib import Path

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--db",default="data/hydra.sqlite3"); ap.add_argument("--out",default="data/training_data.csv"); ap.add_argument("--json",dest="json_out",default=""); args=ap.parse_args()
    con=sqlite3.connect(args.db); con.row_factory=sqlite3.Row
    trades={r["id"]:dict(r) for r in con.execute("select * from trades")}
    features={}; outcomes=[]
    for e in con.execute("select id,ts,kind,payload from events order by id"):
        payload=json.loads(e["payload"]); payload["event_ts"]=e["ts"]
        if e["kind"]=="SIGNAL_FEATURES": features[payload.get("signal_id")]=payload
        elif e["kind"]=="TRADE_OUTCOME": outcomes.append(payload)
    out=[]
    for o in outcomes:
        f=features.get(o.get("signal_id"),{})
        trade=trades.get(o.get("position_id"),{})
        row={**{k:v for k,v in f.items() if k!="event_ts"}, **{f"outcome_{k}":v for k,v in o.items() if k!="event_ts"}, **{f"trade_{k}":v for k,v in trade.items() if k not in {"id"}}}
        out.append(row)
    Path(args.out).parent.mkdir(parents=True,exist_ok=True)
    keys=sorted({k for r in out for k in r})
    with open(args.out,"w",newline="",encoding="utf-8") as fh:
        w=csv.DictWriter(fh,fieldnames=keys); w.writeheader(); w.writerows(out)
    if args.json_out:
        Path(args.json_out).parent.mkdir(parents=True,exist_ok=True); Path(args.json_out).write_text(json.dumps(out,indent=2,default=str),encoding="utf-8")
    print(f"exported={len(out)} path={args.out}")
if __name__=="__main__": main()
