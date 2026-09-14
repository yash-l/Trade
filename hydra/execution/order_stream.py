from __future__ import annotations
import asyncio, json, logging
from typing import Awaitable, Callable
import websockets
from ..broker.fivepaisa import FivePaisaAdapter, FivePaisaError

log=logging.getLogger(__name__)
Callback=Callable[[dict],Awaitable[None]]

class FivePaisaOrderStream:
    """Fail-closed order/trade confirmation stream with bounded reconnects."""
    def __init__(self, broker:FivePaisaAdapter, on_message:Callback, stop_event:asyncio.Event|None=None):
        self.broker=broker; self.on_message=on_message; self.stop_event=stop_event or asyncio.Event(); self._ws=None
    async def run(self, max_retries:int|None=None):
        delay=1.0; retries=0
        while not self.stop_event.is_set():
            try:
                await self._session(); retries=0; delay=1.0
            except asyncio.CancelledError: raise
            except Exception as exc:
                retries+=1; log.warning('order websocket disconnected: %s',exc)
                if max_retries is not None and retries>=max_retries: raise
                await asyncio.sleep(min(delay,30.0)); delay=min(delay*2,30.0)
    async def _session(self):
        if not self.broker.access_token or not self.broker.client_code: raise FivePaisaError('ORDER_WS_AUTH_MISSING')
        async with websockets.connect(self.broker.websocket_url(), ping_interval=20, ping_timeout=10, close_timeout=5, max_size=2**20, open_timeout=12) as ws:
            self._ws=ws
            await ws.send(json.dumps({'Method':'OrderTradeConfirmations','Operation':'Subscribe','ClientCode':self.broker.client_code}))
            async for raw in ws:
                if self.stop_event.is_set(): break
                try: data=json.loads(raw)
                except Exception: continue
                await self.on_message(data)
        self._ws=None
