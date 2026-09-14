from __future__ import annotations
from dataclasses import dataclass
from datetime import time
from collections import deque
from ..models import Candle, Signal
from ..indicators import EMA, ATR, SessionVWAP, RelativeVolume

@dataclass
class BreakoutState:
    side: str
    level: float
    started_index: int
    retest_lowhigh: float|None=None
    trigger_ref: float|None=None
    bars_waited: int=0

class ApexCore:
    """APEX CORE v1: 5m regime -> 3m breakout/retest -> 1m trigger.
    Score is a filter, never a probability of profit.
    """
    name="APEX_CORE_V1"
    def __init__(self, threshold:int=85):
        self.threshold=threshold
        self.ema20=EMA(20); self.ema50=EMA(50); self.atr=ATR(14); self.vwap=SessionVWAP(); self.rvol=RelativeVolume(20)
        self.or_high=None; self.or_low=None; self.day=None
        self.last5=deque(maxlen=6); self.last3=deque(maxlen=8); self.index1=0
        self.state:BreakoutState|None=None
        self.latest_regime={"side":"CHOP","score":0,"factors":{}}

    def reset_day(self, c:Candle):
        if self.day != c.ts.date():
            self.day=c.ts.date(); self.or_high=None; self.or_low=None; self.state=None

    def on_1m(self,c:Candle)->Signal|None:
        self.reset_day(c); self.index1+=1
        self.vwap.update(c); self.atr.update(c); rv=self.rvol.update(c.volume)
        t=c.ts.time()
        if time(9,15) <= t < time(9,30):
            self.or_high=c.high if self.or_high is None else max(self.or_high,c.high)
            self.or_low=c.low if self.or_low is None else min(self.or_low,c.low)
        if self.state:
            self.state.bars_waited += 1
            if self.state.bars_waited > 8:
                self.state=None; return None
            level=self.state.level
            atr=max(self.atr.value or 1,1)
            tol=max(atr*0.08, 2.0)
            if self.state.trigger_ref is None:
                if self.state.side=="LONG" and c.low <= level+tol and c.close>level and c.close>c.open:
                    self.state.trigger_ref=c.high; self.state.retest_lowhigh=c.low; self.state.bars_waited=0
                elif self.state.side=="SHORT" and c.high >= level-tol and c.close<level and c.close<c.open:
                    self.state.trigger_ref=c.low; self.state.retest_lowhigh=c.high; self.state.bars_waited=0
            else:
                if self.state.side=="LONG" and c.high>self.state.trigger_ref and c.close>level and c.close>(self.vwap.value or c.close):
                    return self._make_signal(c,"LONG",level,rv)
                if self.state.side=="SHORT" and c.low<self.state.trigger_ref and c.close<level and c.close<(self.vwap.value or c.close):
                    return self._make_signal(c,"SHORT",level,rv)
        return None

    def on_3m(self,c:Candle):
        self.last3.append(c)
        if self.or_high is None or self.or_low is None or self.state is not None: return
        if not (time(9,30) <= c.ts.time() <= time(14,50)): return
        side=self.latest_regime["side"]
        if side=="LONG" and c.close>self.or_high and c.open<=self.or_high:
            self.state=BreakoutState("LONG",self.or_high,self.index1)
        elif side=="SHORT" and c.close<self.or_low and c.open>=self.or_low:
            self.state=BreakoutState("SHORT",self.or_low,self.index1)

    def on_5m(self,c:Candle):
        self.last5.append(c)
        e20=self.ema20.update(c.close); e50=self.ema50.update(c.close)
        v=self.vwap.value or c.close
        recent=list(self.last5)
        hhhl=False; lhll=False
        if len(recent)>=3:
            hhhl=recent[-1].high>=recent[-2].high and recent[-1].low>=recent[-2].low
            lhll=recent[-1].high<=recent[-2].high and recent[-1].low<=recent[-2].low
        rv=self.rvol.update(c.volume)
        impulse=abs(c.close-c.open) >= 0.45*max(c.high-c.low,0.01) and rv>=1.05
        bullish={"vwap":c.close>v,"ema":e20>e50,"slope":self.ema20.slope>0,"structure":hhhl,"impulse":impulse}
        bearish={"vwap":c.close<v,"ema":e20<e50,"slope":self.ema20.slope<0,"structure":lhll,"impulse":impulse}
        b=sum(bullish.values()); s=sum(bearish.values())
        if b>=4: side="LONG"; factors=bullish
        elif s>=4: side="SHORT"; factors=bearish
        else: side="CHOP"; factors= bullish if b>=s else bearish
        self.latest_regime={"side":side,"score":max(b,s)*20,"factors":factors,"vwap":v,"ema20":e20,"ema50":e50,"rvol":rv}

    @staticmethod
    def _time_bucket(ts):
        h,m=ts.hour,ts.minute
        if h<10 or (h==10 and m<30): return "OPEN"
        if h<12: return "MID_MORNING"
        if h<13: return "MIDDAY"
        if h<14: return "AFTERNOON"
        return "CLOSE"

    def _make_signal(self,c:Candle,side:str,level:float,rv:float)->Signal|None:
        regime=self.latest_regime
        atr=max(self.atr.value or 1.0,1.0)
        breakout_quality=15
        retest_quality=15
        trend=20 if regime["side"]==side else 0
        vw=15 if ((side=="LONG" and c.close>(self.vwap.value or c.close)) or (side=="SHORT" and c.close<(self.vwap.value or c.close))) else 0
        ema=10 if regime["factors"].get("ema") else 0
        impulse=10 if rv>=1.0 else 5
        netrr=10
        liquidity=5  # final execution gate may remove this score.
        score=trend+vw+ema+breakout_quality+retest_quality+impulse+netrr+liquidity
        if score < self.threshold:
            self.state=None; return None
        if side=="LONG":
            stop=min(self.state.retest_lowhigh or level, level-0.10*atr); target=c.close+2.0*max(c.close-stop,1)
        else:
            stop=max(self.state.retest_lowhigh or level, level+0.10*atr); target=c.close-2.0*max(stop-c.close,1)
        bucket = f"{side}|{regime['side']}|{self._time_bucket(c.ts)}|{score//5*5}-{score//5*5+4}"
        features={
            "regime_factors": dict(regime.get("factors",{})),
            "rvol": rv,
            "atr": atr,
            "time_bucket": self._time_bucket(c.ts),
            "spread_pct": None,
            "r_distance": abs(c.close-stop)/atr if atr else None,
            "regime_side": regime["side"],
            "bucket_key": bucket,
        }
        sig=Signal(c.ts,self.name,side,score,c.close,level,stop,target,
                   f"{side} 5m regime + 3m breakout/retest + 1m rejection trigger",
                   adaptive_bucket=bucket, features=features)
        self.state=None
        return sig
