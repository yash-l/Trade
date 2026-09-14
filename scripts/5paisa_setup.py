from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from hydra.broker.fivepaisa import FivePaisaAdapter
from hydra.config import APP_ROOT, Settings

ENV = APP_ROOT / ".env"


def upsert(values: dict[str, str]):
    lines = ENV.read_text().splitlines() if ENV.exists() else []
    out = []
    done = set()
    for line in lines:
        if "=" in line and not line.lstrip().startswith("#"):
            k = line.split("=", 1)[0].strip()
            if k in values:
                out.append(f"{k}={values[k]}")
                done.add(k)
                continue
        out.append(line)
    for k, v in values.items():
        if k not in done:
            out.append(f"{k}={v}")
    ENV.write_text("\n".join(out).rstrip() + "\n")
    try:
        os.chmod(ENV, 0o600)
    except Exception:
        pass


async def main():
    cfg = Settings()
    f = FivePaisaAdapter(cfg)
    try:
        cmd = sys.argv[1] if len(sys.argv) > 1 else "help"
        if cmd == "oauth-url":
            response_url = sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:8181/"
            print(f.oauth_url(response_url))
            print("\nLogin in the browser, then copy the RequestToken from the redirect URL.")
            return
        if cmd == "exchange":
            if len(sys.argv) < 3:
                raise SystemExit("Usage: python scripts/5paisa_setup.py exchange 'REQUEST_TOKEN'")
            result = await f.exchange_request_token(sys.argv[2].strip())
            token = result.pop("access_token")
            upsert(
                {
                    "HYDRA_5PAISA_ACCESS_TOKEN": token,
                    "HYDRA_5PAISA_CLIENT_CODE": str(result.get("client_code", "")),
                    "HYDRA_MODE": "paper",
                    "HYDRA_DATA_MODE": "5paisa",
                }
            )
            print("[PASS] Access token saved to .env (not printed).")
            print("[PASS] Client code:", result.get("client_code"))
            print("[PASS] NSE derivatives permission:", result.get("allow_nse_deriv"))
            print("Restart HYDRA: ./stop.sh && ./start.sh")
            return
        print("Usage:")
        print("  python scripts/5paisa_setup.py oauth-url")
        print("  python scripts/5paisa_setup.py exchange 'REQUEST_TOKEN'")
    finally:
        await f.close()


if __name__ == "__main__":
    asyncio.run(main())
