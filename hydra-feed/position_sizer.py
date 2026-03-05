cat << 'EOF' > position_sizer.py
import math
from enum import Enum
from dataclasses import dataclass
from typing import Optional

class TradeDirection(Enum):
    LONG = "LONG"
    SHORT = "SHORT"

@dataclass
class TradeResult:
    status: str
    reason: str = ""
    trade_type: str = ""
    shares: int = 0
    entry: float = 0.0
    stop_loss: float = 0.0
    target: float = 0.0
    capital_required: float = 0.0
    actual_risk: float = 0.0
    risk_per_share: float = 0.0
    r_multiple_target: float = 0.0
    net_reward_potential: float = 0.0
    effective_rr: float = 0.0

class PositionSizer:
    def __init__(
        self, 
        available_capital: float, 
        max_risk_pct: float = 0.01, 
        brokerage_fees: float = 55.0, 
        rr_ratio: float = 2.0, 
        max_stop_pct: float = 0.20,
        min_effective_rr: float = 1.2,
        max_price_limit: float = 1_000_000.0
    ):
        self.available_capital = available_capital
        self.max_risk_pct = max_risk_pct
        self.brokerage_fees = brokerage_fees
        self.rr_ratio = rr_ratio
        self.max_stop_pct = max_stop_pct
        self.min_effective_rr = min_effective_rr
        self.max_price_limit = max_price_limit

    def calculate_trade(
        self, 
        entry_price: float, 
        stop_loss: float, 
        trade_type: TradeDirection = TradeDirection.LONG,
        brokerage_override: Optional[float] = None,
        rr_override: Optional[float] = None,
        slippage_pct: float = 0.001
    ) -> TradeResult:
        
        # Resolve overrides first
        current_brokerage = brokerage_override if brokerage_override is not None else self.brokerage_fees
        current_rr = rr_override if rr_override is not None else self.rr_ratio

        # --- 1. SYSTEM SURVIVAL GUARDS ---
        if (self.available_capital * self.max_risk_pct) <= current_brokerage:
            return TradeResult(status="REJECTED", reason=f"Capital exhaustion: Allowed 1% risk cannot cover ₹{current_brokerage} fees.")

        # --- 2. INPUT SANITY CHECKS ---
        if current_brokerage < 0 or slippage_pct < 0:
            return TradeResult(status="REJECTED", reason="Invalid parameters (negative fees or slippage).")
        if entry_price <= 0 or stop_loss <= 0:
            return TradeResult(status="REJECTED", reason="Prices must be > 0.")
        if entry_price > self.max_price_limit:
            return TradeResult(status="REJECTED", reason=f"Entry price exceeds system limit ({self.max_price_limit}).")

        # --- 3. RISK ALLOWANCE ---
        max_total_risk = self.available_capital * self.max_risk_pct 
        chart_risk_allowed = max_total_risk - current_brokerage
        
        if chart_risk_allowed <= 0:
            return TradeResult(status="REJECTED", reason=f"Brokerage (₹{current_brokerage}) exceeds allowed risk.")
            
        # --- 4. STRUCTURAL CHECKS ---
        base_stop_distance = abs(entry_price - stop_loss)
        if base_stop_distance == 0:
            return TradeResult(status="REJECTED", reason="Stop loss cannot equal entry price.")
        if (base_stop_distance / entry_price) > self.max_stop_pct:
            return TradeResult(status="REJECTED", reason=f"Stop distance > {self.max_stop_pct * 100}% of price. Setup is too wide.")

        if trade_type == TradeDirection.LONG and stop_loss >= entry_price:
            return TradeResult(status="REJECTED", reason="Invalid LONG: Stop loss must be below entry.")
        if trade_type == TradeDirection.SHORT and stop_loss <= entry_price:
            return TradeResult(status="REJECTED", reason="Invalid SHORT: Stop loss must be above entry.")

        # --- 5. POSITION SIZING (With Conservative Slippage Bias) ---
        effective_stop_distance = base_stop_distance * (1 + slippage_pct)

        risk_based_shares = math.floor(chart_risk_allowed / effective_stop_distance)
        capital_based_shares = math.floor(self.available_capital / entry_price)
        final_shares = min(risk_based_shares, capital_based_shares)
        
        if final_shares <= 0:
            return TradeResult(status="REJECTED", reason="Risk too tight or stock too expensive after slippage.")
            
        capital_required = final_shares * entry_price
        if capital_required > self.available_capital:
            return TradeResult(status="REJECTED", reason="Insufficient available capital (Zero leverage rule).")
            
        actual_risk_taken = round((final_shares * effective_stop_distance) + current_brokerage, 2)
        
        if actual_risk_taken > round(max_total_risk, 2):
            return TradeResult(status="REJECTED", reason="Risk exceeded after float rounding.")
            
        # --- 6. TARGET & EXPECTANCY CHECKS ---
        target_distance = base_stop_distance * current_rr
        target_price = entry_price + target_distance if trade_type == TradeDirection.LONG else entry_price - target_distance
        
        net_reward_potential = round((final_shares * target_distance) - current_brokerage, 2)
        
        # Float noise immunity via 4-decimal rounding
        effective_rr = round(net_reward_potential / actual_risk_taken, 4)
        if effective_rr < self.min_effective_rr:  
            return TradeResult(status="REJECTED", reason=f"Effective RR ({effective_rr}) below policy minimum ({self.min_effective_rr}).")
        
        return TradeResult(
            status="APPROVED",
            trade_type=trade_type.value,
            shares=final_shares,
            entry=entry_price,
            stop_loss=stop_loss,
            target=round(target_price, 2),
            capital_required=round(capital_required, 2),
            actual_risk=actual_risk_taken,
            risk_per_share=round(effective_stop_distance, 2),
            r_multiple_target=current_rr,
            net_reward_potential=net_reward_potential,
            effective_rr=effective_rr
        )

# --- TEST EXECUTION ---
if __name__ == "__main__":
    sizer = PositionSizer(available_capital=15000.0)
    print("--- HYDRA FNF POSITION SIZER FINALIZED ---")
    
    print("\n[+] Testing Valid Setup (ITC):")
    res1 = sizer.calculate_trade(entry_price=800, stop_loss=790)
    print(f"Status: {res1.status} | Shares: {res1.shares} | Effective RR: {res1.effective_rr}")
EOF
