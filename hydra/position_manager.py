from __future__ import annotations
from dataclasses import dataclass, asdict
import math
from .config import Settings
from .models import Candle, PaperPosition

@dataclass
class TrailUpdate:
    highest_r: float
    stop_before: float
    stop_after: float
    target_before: float
    target_after: float
    breakeven_activated: bool = False
    trailing_stop_activated: bool = False
    trailing_target_activated: bool = False
    stop_moved: bool = False
    target_moved: bool = False
    def dict(self): return asdict(self)

class PositionManager:
    """Ratchet-only position management for one-lot APEX positions.

    Rules:
    - R is frozen at entry from underlying entry to initial structure stop.
    - At +1R (configurable), stop is protected around breakeven + a small lock.
    - At +1.4R, stop trails completed 3m structure with an R buffer.
    - At +1.6R, the target becomes a runner target and can extend in steps,
      always ahead of favorable excursion, capped by max_target_r.
    - A LONG stop/target may only move upward; a SHORT stop/target only downward.
    """
    def __init__(self, cfg: Settings): self.cfg=cfg

    @staticmethod
    def _r_points(p: PaperPosition) -> float:
        r=p.initial_r_points or abs(p.underlying_entry-p.initial_stop_underlying) or abs(p.underlying_entry-p.stop_underlying)
        return max(float(r), 1e-9)

    def favorable_r(self, p: PaperPosition, c: Candle) -> float:
        r=self._r_points(p)
        # Candle-mode PAPER uses the completed 1m close so high/low ordering inside
        # the bar cannot create an artificial trail activation. A future tick-mode
        # live manager can feed mark/tick events directly.
        move=(c.close-p.underlying_entry) if p.side=="LONG" else (p.underlying_entry-c.close)
        return max(0.0, move/r)

    def update(self, p: PaperPosition, c: Candle, last3: Candle|None=None) -> TrailUpdate:
        r=self._r_points(p)
        before_stop=p.stop_underlying; before_target=p.target_underlying
        current_r=self.favorable_r(p,c)
        p.highest_r=max(p.highest_r,current_r)
        be_activated=ts_activated=tt_activated=False

        # Stage 1: protect capital after +1R. Ratchet only.
        if p.highest_r >= p.adaptive_breakeven_trigger_r:
            lock=self.cfg.breakeven_lock_r*r
            be_stop=p.underlying_entry+lock if p.side=="LONG" else p.underlying_entry-lock
            if p.side=="LONG" and be_stop>p.stop_underlying:
                p.stop_underlying=be_stop
                if not p.breakeven_active: be_activated=True
            elif p.side=="SHORT" and be_stop<p.stop_underlying:
                p.stop_underlying=be_stop
                if not p.breakeven_active: be_activated=True
            p.breakeven_active=True

        # Stage 2: trail completed 3m structure from +1.4R.
        if p.highest_r >= self.cfg.trailing_stop_trigger_r:
            if not p.trailing_stop_active: ts_activated=True
            p.trailing_stop_active=True
            if last3 is not None:
                buf=p.adaptive_trailing_stop_buffer_r*r
                structure_stop=(last3.low-buf) if p.side=="LONG" else (last3.high+buf)
                # Never move a stop through/above current market; leave some room.
                if p.side=="LONG":
                    structure_stop=min(structure_stop, c.close-0.02*r)
                    p.stop_underlying=max(p.stop_underlying, structure_stop)
                else:
                    structure_stop=max(structure_stop, c.close+0.02*r)
                    p.stop_underlying=min(p.stop_underlying, structure_stop)

        # Stage 3: trail/extend target. Quantize to avoid moving it every tick.
        if p.highest_r >= self.cfg.trailing_target_trigger_r:
            if not p.trailing_target_active: tt_activated=True
            p.trailing_target_active=True
            desired_r=min(self.cfg.max_target_r, p.highest_r+self.cfg.trailing_target_gap_r)
            step=self.cfg.trailing_target_step_r
            desired_r=math.ceil(desired_r/step-1e-12)*step
            desired_r=min(desired_r,self.cfg.max_target_r)
            desired=p.underlying_entry+desired_r*r if p.side=="LONG" else p.underlying_entry-desired_r*r
            if p.side=="LONG": p.target_underlying=max(p.target_underlying,desired)
            else: p.target_underlying=min(p.target_underlying,desired)

        stop_moved=abs(p.stop_underlying-before_stop)>1e-9
        target_moved=abs(p.target_underlying-before_target)>1e-9
        if stop_moved or target_moved: p.trail_updates += 1
        return TrailUpdate(p.highest_r,before_stop,p.stop_underlying,before_target,p.target_underlying,be_activated,ts_activated,tt_activated,stop_moved,target_moved)
