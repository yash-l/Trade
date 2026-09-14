from __future__ import annotations
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
import json

@dataclass
class CostBreakdown:
    brokerage:float; nse_transaction:float; ipft:float; sebi:float; stamp:float; stt:float; gst:float; other:float=0.0; slippage:float=0.0
    @property
    def total(self): return sum((self.brokerage,self.nse_transaction,self.ipft,self.sebi,self.stamp,self.stt,self.gst,self.other,self.slippage))
    def dict(self): return {**self.__dict__,"total":self.total}

class EffectiveCostEngine:
    def __init__(self,path:Path|str="config/cost_rates.json"):
        self.path=Path(path); self.cfg=json.loads(self.path.read_text())
    def _schedule(self,dt:date):
        for x in self.cfg["schedules"]:
            start=date.fromisoformat(x["effective_from"]); end=date.max if not x.get("effective_to") else date.fromisoformat(x["effective_to"])
            if start<=dt<=end:return x
        raise ValueError(f"COST_RATE_MISSING for {dt}; fail-closed instead of guessing historical charges")
    def estimate(self,buy:float,sell:float,qty:int,executed_orders:int=2,when:date|None=None,other:float=0,slippage:float=0)->CostBreakdown:
        when=when or date.today(); r=self._schedule(when)
        bt=buy*qty; st=sell*qty; total=bt+st
        brokerage=r["brokerage_per_executed_order"]*executed_orders
        nse=total*r["nse_transaction_rate"]; ipft=total*r["nse_ipft_rate"]; sebi=total*r["sebi_rate"]
        stamp=bt*r["stamp_buy_rate"]; stt=st*r["stt_sell_premium_rate"]
        taxable=brokerage+nse+ipft+sebi; gst=taxable*r["gst_rate"]
        return CostBreakdown(brokerage,nse,ipft,sebi,stamp,stt,gst,other,slippage)
