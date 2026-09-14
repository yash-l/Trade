# HYDRA Ω-APEX v2.0.2 — Termux Timezone + Startup Verification Fix

Fixes observed on Termux Python 3.13:

- `ZoneInfoNotFoundError: No time zone found with key Asia/Kolkata`.
- Adds Python `tzdata` to the lightweight runtime requirements.
- Adds a fail-safe fixed `UTC+05:30` IST fallback because India has no DST.
- Installer verifies IST offset before the HYDRA self-test.
- `start.sh` no longer prints a false READY/PID message if Python crashes.
- Startup now requires both a live process and a successful `/api/health` response.
- No `pip install --upgrade pip`; Termux continues to own `python-pip`.
