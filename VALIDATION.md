# HYDRA Ω-APEX v2.0.7 Validation

## Scope

Validated the packaged code without user 5paisa credentials. No real order was sent.

## Passed locally

- Python syntax compilation: PASS.
- Unit/integration tests: 11/11 PASS.
- Existing APEX demo lifecycle: PASS.
- TRUE-cost/risk tests: PASS.
- Trailing LONG ratchet: PASS.
- Trailing SHORT ratchet: PASS.
- Trailing target maximum cap: PASS.
- Live-order fail-closed gate: PASS.
- 5paisa option resolver fixture:
  - skips 0–1 DTE under default `min_dte=2`: PASS.
  - resolves ATM + one-ITM candidate: PASS.
- 5paisa level-5 depth parser:
  - selects highest bid and lowest ask: PASS.
- Live-data PAPER signal helper:
  - selects option fixture: PASS.
  - enters PAPER at ask: PASS.
- Self-test: PASS.

## Not verified in packaging environment

- User-specific 5paisa OAuth authentication.
- Actual NIFTY Scrip Master schema returned to the user's account/session.
- Actual openfeed WebSocket connection during NSE market hours.
- Actual live NIFTY option bid/ask messages.
- Actual 5paisa market-depth response for a selected NIFTY option.
- Real-money order placement, modification, cancellation or fills.
- User-specific static-IP/algo-ID compliance.

Those require the user's own 5paisa session and market-hours test. The release remains PAPER-first.
