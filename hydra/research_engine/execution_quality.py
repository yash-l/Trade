from __future__ import annotations
from statistics import fmean

def execution_report(records:list[dict])->dict:
    if not records:return {'fills':0}
    sl=[float(x.get('slippage_points',0)) for x in records]
    lat=[float(x.get('latency_ms',0)) for x in records]
    spread=[float(x.get('spread_pct',0)) for x in records]
    return {'fills':len(records),'mean_slippage_points':fmean(sl),'p95_latency_ms':sorted(lat)[max(0,int(.95*len(lat))-1)],'mean_spread_pct':fmean(spread)}

def negative_control(nets:list[float])->dict:
    # Time-reversed / sign-flipped controls are diagnostic, not proof of absence of bias.
    rev=list(reversed(nets)); flip=[-x for x in nets]
    return {'reverse_mean':fmean(rev) if rev else 0,'sign_flip_mean':fmean(flip) if flip else 0,'status':'DIAGNOSTIC'}

def ablation_score(base:float, variants:dict[str,float])->dict:
    return {'base':base,'variants':variants,'delta_vs_base':{k:v-base for k,v in variants.items()}}
