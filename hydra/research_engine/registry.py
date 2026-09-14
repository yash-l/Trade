from __future__ import annotations
import json, sqlite3, time
from pathlib import Path
class ExperimentRegistry:
    def __init__(self,path='data/research/experiments.sqlite'):
        self.path=Path(path); self.path.parent.mkdir(parents=True,exist_ok=True)
        with sqlite3.connect(self.path) as c:
            c.execute('CREATE TABLE IF NOT EXISTS experiments(id TEXT PRIMARY KEY, hypothesis TEXT, status TEXT, payload TEXT, created REAL)')
    def upsert(self,eid,hypothesis,status='RESEARCH ONLY',payload=None):
        with sqlite3.connect(self.path) as c:c.execute('INSERT OR REPLACE INTO experiments VALUES(?,?,?,?,?)',(eid,hypothesis,status,json.dumps(payload or {}),time.time()))
    def list(self):
        with sqlite3.connect(self.path) as c:return c.execute('SELECT id,hypothesis,status,payload,created FROM experiments ORDER BY created DESC').fetchall()
