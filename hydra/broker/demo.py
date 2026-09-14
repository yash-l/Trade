from __future__ import annotations
from datetime import datetime,timedelta,time
from zoneinfo import ZoneInfo
import math, random
from ..models import Candle, Quote
IST=ZoneInfo("Asia/Kolkata")

class DemoMarket:
    """Deterministic accelerated PAPER demonstration. It is not historical evidence."""
    def __init__(self): self.rng=random.Random(42); self.i=0; self.start=datetime.now(IST).replace(hour=9,minute=15,second=0,microsecond=0); self.price=24000.; self.last=None
    def next_candle(self)->Candle:
        ts=self.start+timedelta(minutes=self.i); i=self.i; self.i+=1
        # deterministic trend -> breakout -> retest -> continuation, then mixed session
        if i<15: drift=2.4
        elif i<18: drift=6.0
        elif i<22: drift=-4.5
        elif i==22: drift=2.2
        elif i<35: drift=4.0
        elif i<70: drift=1.2*math.sin(i/4)
        else: drift=-0.7+1.7*math.sin(i/5)
        noise=self.rng.uniform(-1.0,1.0); o=self.price; c=o+drift+noise; h=max(o,c)+self.rng.uniform(1,3); l=min(o,c)-self.rng.uniform(1,3); v=1000+(i%12)*85+(500 if 15<=i<30 else 0)
        self.price=c
        return Candle(ts,o,h,l,c,v)
    def option_quote(self,side:str,underlying:float)->Quote:
        strike=round(underlying/50)*50
        intrinsic=max(0, underlying-strike) if side=="LONG" else max(0,strike-underlying)
        premium=72+intrinsic*0.55+0.06*abs(underlying-24000)
        spread=max(0.25,premium*0.0035); bid=premium-spread/2; ask=premium+spread/2
        typ="CE" if side=="LONG" else "PE"
        return Quote(datetime.now(IST),f"NIFTY DEMO {strike} {typ}",900000+(1 if typ=='CE' else 2),round(bid,2),round(ask,2),round(premium,2),260,300,0.60,strike,typ,"DEMO")
