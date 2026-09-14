from __future__ import annotations
import random
from statistics import fmean


def metrics_from_nets(nets: list[float]) -> dict:
    if not nets:
        return {"trades": 0}
    vals = [float(x) for x in nets]
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for pnl in vals:
        equity += pnl
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return {
        "trades": len(vals),
        "win_rate": sum(1 for x in vals if x > 0) / len(vals),
        "expectancy": fmean(vals),
        "profit_factor": (gross_win / gross_loss if gross_loss else None),
        "max_drawdown": max_dd,
    }


def _percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        raise ValueError("empty sample")
    pos = (len(sorted_vals) - 1) * pct
    lo = int(pos)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = pos - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def bootstrap_expectancy(nets: list[float], iterations: int = 2000, seed: int = 7) -> dict:
    if len(nets) < 10:
        return {"status": "INSUFFICIENT_SAMPLE"}
    vals = [float(x) for x in nets]
    rng = random.Random(seed)
    n = len(vals)
    means = []
    for _ in range(iterations):
        means.append(sum(rng.choice(vals) for _ in range(n)) / n)
    means.sort()
    return {
        "mean": fmean(vals),
        "ci_2_5": _percentile(means, 0.025),
        "ci_97_5": _percentile(means, 0.975),
        "iterations": iterations,
    }
