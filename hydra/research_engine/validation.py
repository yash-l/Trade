from __future__ import annotations
import math, random
from statistics import fmean

def purged_embargo_splits(n:int, folds:int=5, purge:int=0, embargo:int=0):
    if n<=0 or folds<2: raise ValueError('invalid split parameters')
    edges=[round(i*n/folds) for i in range(folds+1)]
    for i in range(folds):
        test=set(range(edges[i],edges[i+1])); lo=max(0,edges[i]-purge); hi=min(n,edges[i+1]+embargo)
        train=[j for j in range(n) if j not in test and not (lo<=j<hi)]
        yield train,sorted(test)

def cpcv_splits(n:int, groups:int=6, test_groups:int=2, purge:int=0, embargo:int=0):
    if groups<3 or test_groups<1 or test_groups>=groups: raise ValueError('invalid CPCV parameters')
    from itertools import combinations
    edges=[round(i*n/groups) for i in range(groups+1)]
    for combo in combinations(range(groups),test_groups):
        test=[]
        for g in combo: test.extend(range(edges[g],edges[g+1]))
        test=set(test); lo=max(0,min(test)-purge); hi=min(n,max(test)+1+embargo)
        train=[j for j in range(n) if j not in test and not(lo<=j<hi)]
        yield train,sorted(test)

def monte_carlo(nets:list[float], iterations:int=10000, seed:int=7):
    if not nets: return {'status':'INSUFFICIENT_SAMPLE'}
    rng=random.Random(seed); finals=[]; maxdds=[]
    for _ in range(iterations):
        eq=peak=dd=0.0
        for _ in nets:
            eq+=rng.choice(nets); peak=max(peak,eq); dd=min(dd,eq-peak)
        finals.append(eq); maxdds.append(dd)
    finals.sort(); maxdds.sort()
    return {'iterations':iterations,'final_mean':fmean(finals),'final_p05':finals[max(0,int(.05*len(finals))-1)],'final_p95':finals[int(.95*len(finals))], 'max_dd_p95':maxdds[int(.95*len(maxdds))]}

def profit_concentration(nets:list[float])->dict:
    if not nets:return {'trades':0}
    pos=sorted((x for x in nets if x>0),reverse=True); total=sum(pos)
    return {'trades':len(nets),'positive_profit':total,'top1_share':(pos[0]/total if pos else 0),'top5_share':(sum(pos[:5])/total if pos else 0),'top10_share':(sum(pos[:10])/total if pos else 0)}

def simple_pbo(nets:list[float], splits:int=8)->dict:
    if len(nets)<splits*4:return {'status':'INSUFFICIENT_SAMPLE'}
    chunk=max(1,len(nets)//splits); scores=[]
    for i in range(splits):
        x=nets[i*chunk:(i+1)*chunk]; scores.append(fmean(x) if x else 0)
    median=sorted(scores)[len(scores)//2]
    return {'status':'RESEARCH_ONLY','median_fold_expectancy':median,'positive_fold_fraction':sum(x>0 for x in scores)/len(scores),'note':'PBO requires a full combinatorial strategy trial matrix; this metric is a conservative fold-screen, not a formal PBO estimate.'}

def deflated_sharpe_proxy(nets:list[float], trials:int=1)->dict:
    if len(nets)<30:return {'status':'INSUFFICIENT_SAMPLE'}
    mean=fmean(nets); sd=(sum((x-mean)**2 for x in nets)/(len(nets)-1))**0.5 if len(nets)>1 else 0
    sharpe=mean/sd if sd else 0
    penalty=math.sqrt(max(1,2*math.log(max(2,trials))))
    return {'status':'RESEARCH_ONLY','sharpe_proxy':sharpe,'deflated_proxy':sharpe-penalty,'trials':trials,'note':'Approximation; not a formal DSR without strategy-selection distribution inputs.'}
