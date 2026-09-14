from __future__ import annotations
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Literal, Optional

Side = Literal["LONG", "SHORT"]

@dataclass
class Candle:
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    def dict(self):
        d=asdict(self); d["ts"]=self.ts.isoformat(); return d

@dataclass
class Quote:
    ts: datetime
    symbol: str
    scrip_code: int
    bid: float
    ask: float
    ltp: float
    bid_qty: int = 0
    ask_qty: int = 0
    delta: Optional[float] = None
    strike: Optional[float] = None
    option_type: Optional[str] = None
    expiry: Optional[str] = None
    def spread_pct(self) -> float:
        mid=(self.bid+self.ask)/2 if self.bid>0 and self.ask>0 else self.ltp
        return ((self.ask-self.bid)/mid*100) if mid and self.bid>0 and self.ask>0 else 999.0
    def dict(self):
        d=asdict(self); d["ts"]=self.ts.isoformat(); d["spread_pct"]=self.spread_pct(); return d

@dataclass
class Signal:
    ts: datetime
    strategy: str
    side: Side
    score: int
    underlying: float
    breakout_level: float
    stop_underlying: float
    target_underlying: float
    reason: str
    net_rr_candidate: float = 0.0
    adaptive_bucket: str = ""
    signal_id: str = ""
    features: dict | None = None
    def dict(self):
        d=asdict(self); d["ts"]=self.ts.isoformat(); return d

@dataclass
class PaperPosition:
    id: str
    opened_at: datetime
    side: Side
    option_symbol: str
    scrip_code: int
    qty: int
    entry_price: float
    entry_costs: float
    stop_underlying: float
    target_underlying: float
    underlying_entry: float
    estimated_delta: float
    initial_stop_underlying: float = 0.0
    initial_target_underlying: float = 0.0
    initial_r_points: float = 0.0
    highest_r: float = 0.0
    breakeven_active: bool = False
    trailing_stop_active: bool = False
    trailing_target_active: bool = False
    trail_updates: int = 0
    adaptive_bucket: str = ""
    signal_id: str = ""
    adaptive_breakeven_trigger_r: float = 1.0
    adaptive_trailing_stop_buffer_r: float = 0.10
    def dict(self):
        d=asdict(self); d["opened_at"]=self.opened_at.isoformat(); return d
