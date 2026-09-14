from __future__ import annotations
from datetime import timedelta

class CandleGapRecovery:
    """Recovery coordinator: detects gaps and asks the broker for historical 1m data.
    Trading should remain suppressed until the caller validates continuity/warmup."""
    def __init__(self, broker): self.broker=broker
    async def recover(self, *, exch, exch_type, scrip_code, start, end):
        return await self.broker.historical(exch,exch_type,scrip_code,'1m',start,end)
    @staticmethod
    def gap_seconds(prev,current): return max(0,(current-prev).total_seconds())
    @staticmethod
    def requires_recovery(prev,current,max_gap=90): return CandleGapRecovery.gap_seconds(prev,current)>max_gap
