from __future__ import annotations
from collections import deque
import math
from .models import Candle

class EMA:
    def __init__(self, period:int): self.period=period; self.alpha=2/(period+1); self.value=None; self.prev=None
    def update(self, x:float)->float:
        self.prev=self.value
        self.value=x if self.value is None else self.alpha*x+(1-self.alpha)*self.value
        return self.value
    @property
    def slope(self): return 0.0 if self.prev is None or self.value is None else self.value-self.prev

class ATR:
    def __init__(self, period:int=14): self.period=period; self.buf=deque(maxlen=period); self.prev_close=None; self.value=None
    def update(self, c:Candle)->float:
        tr=c.high-c.low if self.prev_close is None else max(c.high-c.low, abs(c.high-self.prev_close), abs(c.low-self.prev_close))
        self.buf.append(tr); self.prev_close=c.close; self.value=sum(self.buf)/len(self.buf); return self.value

class SessionVWAP:
    def __init__(self): self.pv=0.; self.vol=0.; self.value=None; self.day=None
    def update(self, c:Candle)->float:
        d=c.ts.date()
        if self.day != d: self.day=d; self.pv=0.; self.vol=0.; self.value=None
        typical=(c.high+c.low+c.close)/3
        vol=max(c.volume,1.0)
        self.pv += typical*vol; self.vol += vol; self.value=self.pv/self.vol
        return self.value

class RelativeVolume:
    def __init__(self, period=20): self.buf=deque(maxlen=period)
    def update(self, volume:float)->float:
        base=sum(self.buf)/len(self.buf) if self.buf else max(volume,1.0)
        rv=volume/base if base>0 else 1.0
        self.buf.append(max(volume,0.0)); return rv
