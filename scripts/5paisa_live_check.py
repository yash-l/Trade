from __future__ import annotations

import asyncio
import json

from hydra.broker.fivepaisa import FivePaisaAdapter
from hydra.config import Settings


async def main():
    cfg = Settings()
    f = FivePaisaAdapter(cfg)
    try:
        if not f.access_token or not f.client_code:
            raise RuntimeError("5paisa access token/client code missing in .env")
        code = await f.resolve_nifty_underlying()
        feed = await f.market_feed_snapshot(
            [{"Exch": "N", "ExchType": cfg.nifty_exch_type, "ScripCode": code, "ScripData": ""}]
        )
        margin = await f.margin()
        print("[PASS] AUTH + READ-ONLY LIVE DATA")
        print("NIFTY ScripCode:", code)
        print("Orders sent: 0")
        print("Market feed response:")
        print(json.dumps(feed, indent=2)[:4000])
        print("Margin response received:", bool(margin))
    finally:
        await f.close()


if __name__ == "__main__":
    asyncio.run(main())
