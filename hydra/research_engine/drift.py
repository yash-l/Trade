from __future__ import annotations
from statistics import fmean

def rolling_edge_drift(nets:list[float], window:int=30)->dict:
    if len(nets)<window*2:return {'status':'INSUFFICIENT_SAMPLE'}
    a=fmean(nets[-2*window:-window]); b=fmean(nets[-window:]);
    return {'status':'OK','previous_expectancy':a,'recent_expectancy':b,'delta':b-a,'degraded':b<a and a>0}

def change_point_proxy(nets:list[float], min_segment:int=20)->dict:
    if len(nets)<2*min_segment:return {'status':'INSUFFICIENT_SAMPLE'}
    best=None
    for i in range(min_segment,len(nets)-min_segment+1):
        a=fmean(nets[:i]); b=fmean(nets[i:]); score=abs(a-b)
        if best is None or score>best[0]:best=(score,i,a,b)
    return {'status':'RESEARCH_ONLY','index':best[1],'before_mean':best[2],'after_mean':best[3],'delta':best[3]-best[2]}
