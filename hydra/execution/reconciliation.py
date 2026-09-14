from __future__ import annotations
from dataclasses import dataclass
from typing import Any
from ..broker.fivepaisa import FivePaisaAdapter
from .order_fsm import OrderRecord, state_from_confirmation

@dataclass
class ReconciliationReport:
    orders_seen:int; trades_seen:int; positions_seen:int; mismatches:list[dict[str,Any]]

class BrokerReconciler:
    def __init__(self, broker:FivePaisaAdapter): self.broker=broker
    async def snapshot(self)->dict[str,Any]:
        return {'orders':await self.broker.order_book(),'trades':await self.broker.trade_book(),'positions':await self.broker.positions()}
    async def reconcile(self, local:dict[str,OrderRecord])->ReconciliationReport:
        snap=await self.snapshot(); mismatches=[]
        orders=((snap['orders'].get('body') or {}).get('OrderBookDetail') or (snap['orders'].get('body') or {}).get('OrderBook') or [])
        trades=((snap['trades'].get('body') or {}).get('TradeBookDetail') or (snap['trades'].get('body') or {}).get('TradeBook') or [])
        positions=((snap['positions'].get('body') or {}).get('NetPositionDetail') or (snap['positions'].get('body') or {}).get('NetPosition') or [])
        by_remote={str(x.get('RemoteOrderID')):x for x in orders if x.get('RemoteOrderID')}
        for rid,rec in local.items():
            row=by_remote.get(rid)
            if not row: mismatches.append({'remote_order_id':rid,'type':'MISSING_BROKER_ORDER'}); continue
            broker_state=state_from_confirmation(row)
            if rec.state!=broker_state and not (rec.state==broker_state==rec.state.UNKNOWN):
                mismatches.append({'remote_order_id':rid,'type':'STATE_MISMATCH','local':rec.state.value,'broker':broker_state.value})
        return ReconciliationReport(len(orders),len(trades),len(positions),mismatches)
