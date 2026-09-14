from __future__ import annotations
from collections import defaultdict
from datetime import datetime, timedelta

class AdaptiveLayer:
    """Bounded, journal-derived adaptive layer. Default is shadow-only."""
    def __init__(self, cfg, journal): self.cfg=cfg; self.journal=journal

    @staticmethod
    def time_bucket(ts):
        h,m=ts.hour,ts.minute
        if h<10 or (h==10 and m<30): return "OPEN"
        if h<12: return "MID_MORNING"
        if h<13: return "MIDDAY"
        if h<14: return "AFTERNOON"
        return "CLOSE"

    @staticmethod
    def score_band(score):
        return f"{int(score)//5*5}-{int(score)//5*5+4}"

    def bucket(self, side, regime_side, ts, score):
        return f"{side}|{regime_side}|{self.time_bucket(ts)}|{self.score_band(score)}"

    def _outcomes(self, days=None):
        rows=self.journal.events(5000); out=[]
        cutoff=datetime.now().astimezone()-timedelta(days=days) if days else None
        for e in rows:
            if e['kind']!='TRADE_OUTCOME': continue
            try:
                if cutoff and datetime.fromisoformat(e['ts'])<cutoff: continue
            except Exception: pass
            out.append(e['payload'])
        return out

    def entry_decision(self, bucket, base_threshold):
        if not self.cfg.adaptive_enabled: return {'threshold':base_threshold,'adjusted':False,'reason':'DISABLED'}
        losses=[x for x in self._outcomes(self.cfg.adaptive_loss_days) if x.get('bucket_key')==bucket and float(x.get('net',0))<0]
        threshold=min(self.cfg.adaptive_max_threshold, base_threshold + (self.cfg.adaptive_threshold_step if len(losses)>=self.cfg.adaptive_loss_count else 0))
        return {'threshold':threshold,'adjusted':threshold>base_threshold,'losses':len(losses),'reason':'LOSS_BUCKET_COOLDOWN' if threshold>base_threshold else 'DEFAULT'}

    def trail_params(self,bucket):
        outs=[x for x in self._outcomes() if x.get('bucket_key')==bucket]
        # Only tune after enough labeled outcomes. Conservative bounded increments.
        buffer=self.cfg.trailing_stop_buffer_r; be=self.cfg.breakeven_trigger_r; changes=[]
        if len(outs)>=self.cfg.adaptive_min_shadow_trades:
            trail=[x for x in outs if x.get('reason')=='TRAIL_STOP']
            if len(trail)>=10:
                high=[float(x.get('highest_r',0)) for x in trail]
                captured=[float(x.get('captured_r',0)) for x in trail]
                if sum(h>=1.6 and c< h for h,c in zip(high,captured))/len(trail) >= 0.5:
                    buffer=min(self.cfg.adaptive_max_buffer_r, buffer+self.cfg.adaptive_buffer_step_r); changes.append('WIDEN_TRAIL_BUFFER')
            early=[x for x in outs if x.get('reason')=='STRUCTURE_STOP']
            if len(early)>=10 and sum(float(x.get('highest_r',0))<1.0 for x in early)/len(early) >= 0.7:
                be=max(self.cfg.adaptive_min_breakeven_r, be-self.cfg.adaptive_breakeven_step_r); changes.append('EARLY_BREAKEVEN')
        return {'buffer_r':buffer,'breakeven_trigger_r':be,'changed':bool(changes),'changes':changes,'sample':len(outs)}
