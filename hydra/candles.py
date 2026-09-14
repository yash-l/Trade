from __future__ import annotations
from collections import deque
from datetime import datetime
from .models import Candle

class CandleStore:
    def __init__(self):
        self.one=deque(maxlen=1000); self.three=deque(maxlen=500); self.five=deque(maxlen=500)

    @staticmethod
    def aggregate(candles:list[Candle]) -> Candle:
        return Candle(candles[0].ts, candles[0].open, max(x.high for x in candles), min(x.low for x in candles), candles[-1].close, sum(x.volume for x in candles))

    def add_1m(self, c:Candle):
        self.one.append(c)
        new3=new5=None
        minute=c.ts.minute
        if len(self.one)>=3 and (minute+1)%3==0:
            seq=list(self.one)[-3:]
            if all((seq[i].ts-seq[i-1].ts).total_seconds() in range(50,71) for i in range(1,3)):
                new3=self.aggregate(seq); self.three.append(new3)
        if len(self.one)>=5 and (minute+1)%5==0:
            seq=list(self.one)[-5:]
            if all((seq[i].ts-seq[i-1].ts).total_seconds() in range(50,71) for i in range(1,5)):
                new5=self.aggregate(seq); self.five.append(new5)
        return new3,new5
