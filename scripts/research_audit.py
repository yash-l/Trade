#!/usr/bin/env python3
import json
from pathlib import Path
from hydra.research_engine import *
from hydra.research_engine.execution_quality import execution_report,negative_control
from hydra.research_engine.drift import rolling_edge_drift,change_point_proxy

nets=[float(x) for x in __import__('sys').argv[1:]]
if not nets:
    print(json.dumps({'status':'NO_INPUT','required':'python scripts/research_audit.py <net1> <net2> ...'},indent=2)); raise SystemExit
out={'metrics':{'trades':len(nets),'expectancy':sum(nets)/len(nets)},'monte_carlo':monte_carlo(nets),'concentration':profit_concentration(nets),'pbo_screen':simple_pbo(nets),'dsr_proxy':deflated_sharpe_proxy(nets),'negative_control':negative_control(nets),'drift':rolling_edge_drift(nets),'change_point':change_point_proxy(nets)}
print(json.dumps(out,indent=2))
