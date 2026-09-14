# HYDRA Ω-APEX v2.0.5 DAY UI ACTIVE FIX — VALIDATION

Purpose: correct the deployment bug where an older HYDRA process on port 8181 could make a newer build appear started while the old dark dashboard remained active.

Validated locally on the packaged source before ZIP:
- Python compile: PASS
- APEX self-test: PASS
- Core tests: 5/5 PASS
- JavaScript syntax: PASS
- Light/day UI static assertions: PASS
- API health identifies version 2.0.5: PASS
- API health identifies ui_build 2.0.5-day-active: PASS
- Root response Cache-Control no-store: PASS
- Root HTML contains data-ui-build=2.0.5-day-active: PASS
- CSS/JS asset cache-busting: PASS

Important: no real-money 5paisa order was sent. PAPER remains default.
