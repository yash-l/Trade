from __future__ import annotations
import math
from itertools import product
from statistics import fmean

FEATURES = ['score', 'rvol', 'atr', 'r_distance', 'spread_pct']

class ShadowEntryModel:
    """Dependency-free logistic classifier for shadow research only.

    Fit stores normalization statistics and predict_proba uses the exact same
    normalization. This model never changes execution by itself.
    """
    def __init__(self, weights=None, bias=0.0, feature_names=None, means=None, scales=None):
        self.weights = weights or {}
        self.bias = float(bias)
        self.feature_names = feature_names or FEATURES[:]
        self.means = means or {n: 0.0 for n in self.feature_names}
        self.scales = scales or {n: 1.0 for n in self.feature_names}

    @staticmethod
    def _sigmoid(x):
        x=max(-30.0,min(30.0,x)); return 1.0/(1.0+math.exp(-x))

    @staticmethod
    def _value(row, name):
        try: return float(row.get(name) if row.get(name) is not None else 0.0)
        except Exception: return 0.0

    @classmethod
    def _prepare(cls, rows):
        xs=[[cls._value(r,n) for n in FEATURES] for r in rows]
        mu=[sum(x[j] for x in xs)/len(xs) for j in range(len(FEATURES))]
        sd=[max(1e-9,(sum((x[j]-mu[j])**2 for x in xs)/len(xs))**0.5) for j in range(len(FEATURES))]
        z=[[(x[j]-mu[j])/sd[j] for j in range(len(FEATURES))] for x in xs]
        return xs,z,mu,sd

    @classmethod
    def fit(cls, rows, epochs=600, lr=0.03):
        if len(rows)<50: return None
        _, xs, mu, sd = cls._prepare(rows)
        ys=[1.0 if cls._value(r,'outcome_net')>0 else 0.0 for r in rows]
        w=[0.0]*len(FEATURES); b=0.0
        for _ in range(epochs):
            gw=[0.0]*len(FEATURES); gb=0.0
            for x,y in zip(xs,ys):
                z=b+sum(w[j]*x[j] for j in range(len(FEATURES))); p=cls._sigmoid(z); d=p-y
                gb+=d
                for j in range(len(FEATURES)): gw[j]+=d*x[j]
            n=float(len(xs)); b-=lr*gb/n
            for j in range(len(FEATURES)): w[j]-=lr*gw[j]/n
        return cls(dict(zip(FEATURES,w)),b,FEATURES[:],dict(zip(FEATURES,mu)),dict(zip(FEATURES,sd)))

    def predict_proba(self,row):
        z=self.bias
        for n,w in self.weights.items():
            v=(self._value(row,n)-self.means.get(n,0.0))/max(1e-9,self.scales.get(n,1.0))
            z+=w*v
        return self._sigmoid(z)


def cross_validated_shadow(rows, folds=5, purge=1, embargo=1):
    """Purged/embargoed chronological shadow evaluation; never used for execution."""
    if len(rows) < 50 or folds < 2: return {'status':'INSUFFICIENT_SAMPLE'}
    from .validation import purged_embargo_splits
    metrics=[]
    for train_idx,test_idx in purged_embargo_splits(len(rows),folds,purge,embargo):
        if len(train_idx)<50 or not test_idx: continue
        model=ShadowEntryModel.fit([rows[i] for i in train_idx])
        if model is None: continue
        probs=[model.predict_proba(rows[i]) for i in test_idx]
        ys=[1 if ShadowEntryModel._value(rows[i],'outcome_net')>0 else 0 for i in test_idx]
        pred=[1 if p>=0.5 else 0 for p in probs]
        acc=sum(a==b for a,b in zip(pred,ys))/len(ys)
        metrics.append({'accuracy':acc,'n_test':len(ys)})
    if not metrics: return {'status':'INSUFFICIENT_SAMPLE'}
    return {'status':'SHADOW_ONLY','folds':len(metrics),'mean_accuracy':fmean(x['accuracy'] for x in metrics),'fold_metrics':metrics}


def trailing_param_sweep(outcomes):
    """Bounded shadow-only comparison over the specified parameter box."""
    if len(outcomes)<50: return {'status':'INSUFFICIENT_SAMPLE'}
    results=[]
    for buffer_r,be_r in product([0.05,0.10,0.15,0.20],[0.5,0.75,1.0,1.25,1.5]):
        captured=[]
        for o in outcomes:
            h=float(o.get('highest_r',0)); c=float(o.get('captured_r',0)); adj=c
            if h>=be_r and o.get('reason')=='STRUCTURE_STOP': adj=max(c,be_r-0.05)
            if h>=1.4 and o.get('reason')=='TRAIL_STOP' and h-c>=0.6: adj=min(h,c+buffer_r)
            captured.append(adj)
        results.append({'buffer_r':buffer_r,'breakeven_trigger_r':be_r,'mean_captured_r':sum(captured)/len(captured)})
    results.sort(key=lambda x:x['mean_captured_r'],reverse=True)
    return {'status':'SHADOW_ONLY','best':results[0],'grid':results}
