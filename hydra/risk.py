from __future__ import annotations
from dataclasses import dataclass
from .config import Settings
from .models import Quote, Signal
from .costs import EffectiveCostEngine

@dataclass
class RiskDecision:
    approved:bool; reason:str; estimated_loss:float=0; estimated_costs:float=0; estimated_delta:float=0.6
    def dict(self): return self.__dict__

class RiskEngine:
    def __init__(self,cfg:Settings,costs:EffectiveCostEngine): self.cfg=cfg; self.costs=costs; self.daily_net=0.; self.trades=0; self.losses=0
    def can_trade(self,signal:Signal,q:Quote)->RiskDecision:
        if self.trades>=self.cfg.max_trades:return RiskDecision(False,"MAX_TRADES")
        if self.daily_net<=-self.cfg.daily_loss:return RiskDecision(False,"DAILY_LOSS_GUARD")
        if self.losses>=2:return RiskDecision(False,"TWO_CONSECUTIVE_LOSSES")
        if q.bid<=0 or q.ask<=0:return RiskDecision(False,"NO_EXECUTABLE_BID_ASK")
        if q.spread_pct()>self.cfg.max_spread_pct:return RiskDecision(False,f"SPREAD_TOO_WIDE:{q.spread_pct():.2f}%")
        debit=q.ask*self.cfg.lot_size
        if debit>self.cfg.max_capital_per_trade:return RiskDecision(False,f"PREMIUM_DEBIT_TOO_HIGH:{debit:.0f}")
        delta=abs(q.delta or 0.60)
        underlying_risk=abs(signal.underlying-signal.stop_underlying)
        option_price_risk=underlying_risk*delta*self.cfg.lot_size
        # Conservative round-trip cost estimate assumes no price change for fee reserve.
        cb=self.costs.estimate(q.ask,max(q.bid,0.01),self.cfg.lot_size,2,signal.ts.date())
        # Price-drift limit is an execution rejection threshold, not an expected loss.
        # Risk reserve uses expected executable friction: at least one observed spread or 0.15% of premium.
        expected_slippage_per_unit=max(q.ask-q.bid, q.ask*0.0015)
        friction_reserve=expected_slippage_per_unit*self.cfg.lot_size
        all_in=option_price_risk+cb.total+friction_reserve
        if all_in>self.cfg.hard_risk:return RiskDecision(False,f"ALL_IN_RISK>{self.cfg.hard_risk:.0f}",all_in,cb.total,delta)
        if all_in>self.cfg.normal_risk:return RiskDecision(True,"APPROVED_HARD_BAND",all_in,cb.total,delta)
        return RiskDecision(True,"APPROVED",all_in,cb.total,delta)
    def record_close(self,net:float):
        self.trades+=1; self.daily_net+=net; self.losses=self.losses+1 if net<0 else 0
