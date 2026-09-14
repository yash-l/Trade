from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

class OrderState(str, Enum):
    NEW='NEW'; SUBMITTED='SUBMITTED'; PLACED='PLACED'; PARTIAL='PARTIAL'; FILLED='FILLED'; MODIFIED='MODIFIED'; CANCEL_REQUESTED='CANCEL_REQUESTED'; CANCELLED='CANCELLED'; REJECTED='REJECTED'; FAILED='FAILED'; UNKNOWN='UNKNOWN'

_ALLOWED={
 OrderState.NEW:{OrderState.SUBMITTED,OrderState.FAILED},
 OrderState.SUBMITTED:{OrderState.PLACED,OrderState.REJECTED,OrderState.FAILED,OrderState.UNKNOWN},
 OrderState.PLACED:{OrderState.PARTIAL,OrderState.FILLED,OrderState.MODIFIED,OrderState.CANCEL_REQUESTED,OrderState.REJECTED,OrderState.UNKNOWN},
 OrderState.PARTIAL:{OrderState.PARTIAL,OrderState.FILLED,OrderState.MODIFIED,OrderState.CANCEL_REQUESTED,OrderState.REJECTED,OrderState.UNKNOWN},
 OrderState.MODIFIED:{OrderState.PARTIAL,OrderState.FILLED,OrderState.CANCEL_REQUESTED,OrderState.REJECTED,OrderState.UNKNOWN},
 OrderState.CANCEL_REQUESTED:{OrderState.CANCELLED,OrderState.PARTIAL,OrderState.FILLED,OrderState.UNKNOWN},
 OrderState.UNKNOWN:{OrderState.PLACED,OrderState.PARTIAL,OrderState.FILLED,OrderState.CANCELLED,OrderState.REJECTED,OrderState.UNKNOWN},
 OrderState.FILLED:set(), OrderState.CANCELLED:set(), OrderState.REJECTED:set(), OrderState.FAILED:set(),
}
@dataclass
class OrderRecord:
    remote_order_id:str
    side:str
    qty:int
    price:float
    state:OrderState=OrderState.NEW
    broker_order_id:str|None=None
    exchange_order_id:str|None=None
    traded_qty:int=0
    avg_price:float=0.0
    history:list[dict[str,Any]]=field(default_factory=list)
    def transition(self,new:OrderState,event:dict[str,Any]|None=None):
        if new!=self.state and new not in _ALLOWED[self.state]:
            raise ValueError(f'INVALID_ORDER_TRANSITION:{self.state}->{new}')
        old=self.state; self.state=new
        e={'old':old.value,'new':new.value,'event':event or {}}
        self.history.append(e); return e


def state_from_confirmation(data:dict[str,Any]) -> OrderState:
    status=str(data.get('Status') or data.get('OrderStatus') or '').lower()
    req=str(data.get('ReqType') or '').upper()
    if req=='T' or 'execut' in status or 'fully' in status:
        return OrderState.FILLED if int(float(data.get('PendingQty') or 0))==0 else OrderState.PARTIAL
    if req=='C' or 'cancel' in status: return OrderState.CANCELLED
    if 'reject' in status or int(float(data.get('ReqStatus') or 0))==1: return OrderState.REJECTED
    if 'modify' in status: return OrderState.MODIFIED
    if 'partial' in status: return OrderState.PARTIAL
    if 'place' in status or 'open' in status: return OrderState.PLACED
    return OrderState.UNKNOWN
