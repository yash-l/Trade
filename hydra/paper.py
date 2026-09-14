from __future__ import annotations
from datetime import datetime
import uuid
from .models import PaperPosition,Quote,Signal
from .costs import EffectiveCostEngine
from .config import Settings

class PaperBroker:
    def __init__(self,cfg:Settings,costs:EffectiveCostEngine): self.cfg=cfg; self.costs=costs; self.position:PaperPosition|None=None
    def open(self,sig:Signal,q:Quote,adaptive=None)->PaperPosition:
        if self.position: raise RuntimeError("single-position guard")
        entry=q.ask
        # entry-side share of fees is booked immediately using same effective schedule.
        c=self.costs.estimate(entry,entry,self.cfg.lot_size,1,sig.ts.date())
        initial_r=max(abs(sig.underlying-sig.stop_underlying),1e-9)
        p=PaperPosition(
            id="P-"+uuid.uuid4().hex[:10], opened_at=sig.ts, side=sig.side, option_symbol=q.symbol,
            scrip_code=q.scrip_code, qty=self.cfg.lot_size, entry_price=entry, entry_costs=c.total/2,
            stop_underlying=sig.stop_underlying, target_underlying=sig.target_underlying,
            underlying_entry=sig.underlying, estimated_delta=abs(q.delta or .60),
            initial_stop_underlying=sig.stop_underlying, initial_target_underlying=sig.target_underlying,
            initial_r_points=initial_r,
            adaptive_bucket=sig.adaptive_bucket, signal_id=f"{sig.ts.isoformat()}|{sig.side}|{sig.score}",
            adaptive_breakeven_trigger_r=float((adaptive or {}).get("breakeven_trigger_r", self.cfg.breakeven_trigger_r)),
            adaptive_trailing_stop_buffer_r=float((adaptive or {}).get("buffer_r", self.cfg.trailing_stop_buffer_r)),
        )
        self.position=p; return p
    def close(self,q:Quote,underlying:float,reason:str,ts:datetime):
        p=self.position
        if not p:return None
        exit_price=q.bid
        cb=self.costs.estimate(p.entry_price,exit_price,p.qty,2,ts.date())
        gross=(exit_price-p.entry_price)*p.qty
        net=gross-cb.total
        captured_r = ((underlying-p.underlying_entry)/p.initial_r_points) if p.side=="LONG" else ((p.underlying_entry-underlying)/p.initial_r_points)
        row={"id":p.id,"opened_at":p.opened_at.isoformat(),"closed_at":ts.isoformat(),"symbol":p.option_symbol,"qty":p.qty,"entry":p.entry_price,"exit":exit_price,"gross":gross,"costs":cb.total,"net":net,"side":p.side,"reason":reason,"highest_r":p.highest_r,"captured_r":captured_r,"adaptive_bucket":p.adaptive_bucket}
        self.position=None; return row
