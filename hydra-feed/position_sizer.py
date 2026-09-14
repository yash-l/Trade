import math
from enum import Enum
from dataclasses import dataclass


class TradeDirection(Enum):
    LONG = "LONG"
    SHORT = "SHORT"


@dataclass
class TradeResult:
    status: str
    reason: str = ""
    trade_type: str = ""
    lots: int = 0
    qty: int = 0
    entry_premium: float = 0.0
    stop_loss_premium: float = 0.0
    target_premium: float = 0.0
    capital_required: float = 0.0
    actual_risk: float = 0.0
    effective_rr: float = 0.0


class PositionSizer:
    """
    HYDRA FINAL FORM
    Deterministic Kelly-Optimized Compounding Engine
    Fully hardened for 15K → 50K geometric growth
    """

    def __init__(
        self,
        current_capital: float = 15000.0,
        brokerage_fees: float = 55.0,
        win_rate_estimate: float = 0.52,
        daily_drawdown_pct: float = 0.0,
        consecutive_losses: int = 0,
        volatility_regime: float = 1.0,
        gamma_risk_mode: bool = False,
        session_hour: int = 10
    ):
        self.capital = max(current_capital, 0.0)
        self.fees = max(brokerage_fees, 0.0)

        self.win_rate = min(max(win_rate_estimate, 0.30), 0.75)
        self.daily_dd = daily_drawdown_pct
        self.loss_streak = max(consecutive_losses, 0)
        self.vol_regime = max(volatility_regime, 0.5)
        self.gamma_risk_mode = gamma_risk_mode
        self.session_hour = session_hour

        self.base_rr = 2.5
        self.min_rr = 1.30

    # -----------------------------------------------------

    def _kelly_fraction(self) -> float:
        """
        Fractional Kelly with hard cap.
        Prevents blow-up from edge overestimation.
        """
        b = self.base_rr
        p = self.win_rate
        q = 1 - p

        raw_kelly = ((b * p) - q) / b

        if raw_kelly <= 0:
            return 0.02  # minimum survival risk

        # 50% Kelly for stability
        fractional = raw_kelly * 0.5

        return min(max(fractional, 0.02), 0.18)

    # -----------------------------------------------------

    def _dynamic_risk_pct(self, ai_conf: float) -> float:
        kelly = self._kelly_fraction()

        # Confidence scaling
        ai_factor = min(1.4, max(0.6, ai_conf))
        risk = kelly * ai_factor

        # Volatility compression
        risk /= self.vol_regime

        # Gamma compression
        if self.gamma_risk_mode:
            risk *= 0.6

        # Daily drawdown guard
        if self.daily_dd <= -0.05:
            risk *= 0.5

        # Loss streak compression
        if self.loss_streak == 2:
            risk *= 0.6
        elif self.loss_streak >= 3:
            risk *= 0.4

        # Liquidity window boost
        if 9 <= self.session_hour <= 11:
            risk *= 1.10

        # Hard risk boundary
        return min(max(risk, 0.01), 0.10)

    # -----------------------------------------------------

    def calculate_trade(
        self,
        premium: float,
        stop_loss: float,
        lot_size: int,
        trade_type: TradeDirection = TradeDirection.LONG,
        ai_confidence: float = 1.0,
        slippage_pct: float = 0.02
    ) -> TradeResult:

        # --- Input Validation ---
        if premium <= 0 or stop_loss <= 0 or lot_size <= 0:
            return TradeResult(status="REJECTED", reason="Invalid inputs")

        if self.capital <= 0:
            return TradeResult(status="REJECTED", reason="No capital")

        stop_distance = abs(premium - stop_loss)
        if stop_distance <= 0:
            return TradeResult(status="REJECTED", reason="Zero stop")

        # --- Risk Budget ---
        risk_pct = self._dynamic_risk_pct(ai_confidence)
        max_risk_inr = self.capital * risk_pct

        round_trip_friction = self.fees * 2
        usable_risk = max_risk_inr - round_trip_friction

        if usable_risk <= 0:
            return TradeResult(status="REJECTED", reason="Friction block")

        # --- Risk Per Lot ---
        effective_stop = stop_distance * (1 + slippage_pct)
        risk_per_lot = effective_stop * lot_size

        if risk_per_lot <= 0:
            return TradeResult(status="REJECTED", reason="Invalid lot risk")

        lots_by_risk = math.floor(usable_risk / risk_per_lot)

        # --- Hard Capital Exposure Cap ---
        exposure_cap = self.capital * 0.55
        lots_by_margin = math.floor(exposure_cap / (premium * lot_size))

        final_lots = min(lots_by_risk, lots_by_margin)

        if final_lots < 1:
            return TradeResult(status="REJECTED", reason="Capital insufficient")

        # --- Final Calculations ---
        qty = final_lots * lot_size
        capital_required = qty * premium

        actual_risk = round((final_lots * risk_per_lot) + round_trip_friction, 2)

        target_distance = stop_distance * self.base_rr

        target_premium = (
            premium + target_distance
            if trade_type == TradeDirection.LONG
            else premium - target_distance
        )

        gross_reward = final_lots * target_distance * lot_size
        net_reward = gross_reward - round_trip_friction

        if actual_risk <= 0:
            return TradeResult(status="REJECTED", reason="Risk invalid")

        effective_rr = round(net_reward / actual_risk, 4)

        if effective_rr < self.min_rr:
            return TradeResult(status="REJECTED", reason="RR too low")

        return TradeResult(
            status="APPROVED",
            trade_type=trade_type.value,
            lots=final_lots,
            qty=qty,
            entry_premium=premium,
            stop_loss_premium=stop_loss,
            target_premium=round(target_premium, 2),
            capital_required=round(capital_required, 2),
            actual_risk=actual_risk,
            effective_rr=effective_rr
        )
