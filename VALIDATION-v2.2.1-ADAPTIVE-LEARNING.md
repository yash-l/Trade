# HYDRA Ω-APEX v2.2.2 — Proper Verification Report

## Clean-package verification
- Fresh ZIP extraction: required
- Editable package install with `--no-deps`: required
- `pytest`: 23 passed / 0 failed
- `python -m compileall -q hydra scripts`: PASS
- `scripts/selftest.py`: PASS
- Adaptive journal events: SIGNAL_FEATURES + TRADE_OUTCOME PASS
- Hash-chain verification + tamper detection: PASS
- Adaptive entry loss threshold: PASS
- Adaptive trailing bounds: PASS
- Shadow classifier normalization: PASS
- Purged/embargoed shadow cross-validation: PASS
- Exact signal/outcome/trade training-data join: PASS

## Safety verification
- Entry adaptive model remains shadow-only.
- Adaptive trailing tuner is shadow-only by default (`HYDRA_ADAPTIVE_TRAIL_SHADOW_ONLY=true`); when explicitly disabled, bounded trailing parameters may be applied to PAPER positions.
- Hard risk, daily loss, max trades and max target caps are outside adaptive control.
- Adaptive trailing parameters are bounded to configured ranges.
- Entry adaptation only raises the threshold; it never lowers it.
- LIVE remains disabled by default.

## External validation NOT claimed
- Real 5paisa credentials/session.
- Real NSE market-hours WebSocket execution.
- Static-IP/account-specific algo compliance approval.
- 50–100+ real labeled market trades.
- Statistical proof of live edge improvement.
- Live adaptive promotion.
