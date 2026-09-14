# HYDRA Ω-APEX v2.0.4 Validation

Validated on the packaged source before release:

- Python compile: PASS
- Self-test with project-local SQLite DB: PASS
- Core tests: 5/5 PASS using `python -m pytest -q`
- Dashboard boot: PASS
- `/api/health`: PASS
- Default mode: PAPER
- Real 5paisa account/auth/live execution: NOT tested in this environment.

Termux-specific fix:
- no `/tmp` dependency in self-test
- relative DB path anchored to application root
- SQLite write probe during install
- startup remains health-checked
