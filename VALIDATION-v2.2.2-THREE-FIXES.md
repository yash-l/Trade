# HYDRA Ω-APEX v2.2.3 — Three Fixes Verification

## Implemented
1. Adaptive trailing tuner is shadow-gated by default with `HYDRA_ADAPTIVE_TRAIL_SHADOW_ONLY=true`.
   - Shadow mode journals `ADAPTIVE_PARAM_ADJUSTED_SHADOW`.
   - Shadow mode opens PAPER positions with default APEX trailing parameters.
   - Only when the flag is explicitly false are bounded adaptive trail parameters applied.
2. Demo reset no longer orphans an open PAPER position.
   - At reset, a valid quote force-closes the position with reason `DEMO_RESET` and journals the outcome.
   - If no valid quote exists, reset is deferred rather than orphaning the position.
3. Added TIME_EXIT coverage at 15:10+ IST and corrected the time comparison to use minutes since midnight.

## Verification
- Fresh-package pytest: 27 passed / 0 failed
- Python compileall: PASS
- Self-test: PASS
- Server startup: PASS
- `/health`: PASS
- UI marker: PASS (`2.2.3-adaptive-paper`)
- Server stopped cleanly after verification: PASS

## Safety
- Entry adaptive model remains shadow-only.
- Adaptive trailing remains shadow-only by default.
- Hard risk, daily loss, max trades, and max target caps remain outside adaptive control.
- LIVE remains disabled by default.


## v2.2.3 UI/UX
- Added Adaptive navigation and mobile bottom-nav entry.
- Added Adaptive Learning page backed by `/api/status.adaptive`.
- Added shadow/live status pills, 0–50 sample progress, bucket intelligence, empty state, and adaptive-only event filtering.
- Uses existing responsive grid/card/gauge patterns; no new media-query breakpoints.
