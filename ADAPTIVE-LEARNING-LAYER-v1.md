# HYDRA Adaptive Learning Layer v1

Entry + trailing adaptive layer implemented as bounded, auditable controls.

- Phase 0: SIGNAL_FEATURES, TRADE_OUTCOME, training-data export.
- Phase 1: loss-bucket entry tightening; bounded trailing buffer / early-BE tuning.
- Phase 2: shadow-only gate markers and research-ready labeled dataset.
- Phase 3: not enabled; live promotion remains gated and requires statistical validation.

Hard caps are untouched: max_target_r, hard_risk, daily_loss, max_trades.
Every adaptive parameter adjustment is journaled through the existing hash-chain event path.
