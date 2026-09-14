# HYDRA Ω-APEX v2.0.6 — TRAILING SL + TRAILING TARGET VALIDATION

Status: PAPER-first engineering build. No live-profitability claim.

## Verified on this exact source tree

- Python compilation: PASS
- Unit/integration suite: 8/8 PASS
- LONG trailing ratchet: PASS
- SHORT trailing ratchet: PASS
- Breakeven protection at configured R trigger: PASS
- 3-minute structure trailing stop activation: PASS
- Dynamic target extension: PASS
- Maximum target-R cap: PASS
- Stop cannot loosen after ratchet: PASS
- Target cannot retract after extension: PASS
- Demo PAPER lifecycle produces TRAIL_UPDATE events: PASS
- Demo PAPER trade closes through trailing target/stop logic: PASS
- HTTP API health: PASS
- Dashboard root: PASS
- API reports version 2.0.6 and ui_build 2.0.6-trailing: PASS
- Dashboard exposes trailing SL/target state: PASS

## Default trailing policy

- +1.00R: breakeven protection, +0.05R lock.
- +1.40R: trailing stop enabled using latest completed 3-minute structure with 0.10R buffer.
- +1.60R: trailing target enabled.
- Target remains about +0.75R ahead of the best favorable excursion, quantized in 0.25R steps.
- Hard runner ceiling: 4.00R by default.
- LONG stop/target only ratchet upward. SHORT stop/target only ratchet downward.

## Important limitation

The verified execution path here is DEMO/PAPER. Real 5paisa option-quote PAPER execution and real-money LIVE execution still require account/session/broker validation and all HYDRA compliance gates.
