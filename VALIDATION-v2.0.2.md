# HYDRA Ω-APEX v2.0.2 Validation

Validated on the packaged source build:

- Python compileall: PASS
- HYDRA self-test: PASS
- Core tests: 5/5 PASS
- IST offset check: PASS (`UTC+05:30`)
- Dashboard/API boot smoke test: PASS
- `/api/health`: PASS
- `/api/status`: PASS
- Startup false-positive PID behavior: fixed; READY now requires a live process + health response

Termux-specific fix:
- `tzdata` is installed as a Python dependency.
- `hydra.timeutil` falls back to fixed IST (UTC+05:30) if IANA zoneinfo is unavailable.

Boundary:
- This environment is Linux, not Android/Termux. The exact Termux shebang path cannot execute here, so shell-body smoke testing was run via `bash start.sh`. Python/runtime behavior is verified; the user's Termux device remains the final platform validation.
