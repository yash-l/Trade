from __future__ import annotations
import sqlite3, json, hashlib, threading
from pathlib import Path
from datetime import datetime
from .timeutil import now_ist

class Journal:
    def __init__(self,path:Path):
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.parent.is_dir():
            raise RuntimeError(f"HYDRA database parent is not a directory: {self.path.parent}")
        self.lock=threading.Lock()
        self._init()
    def _conn(self):
        try:
            c=sqlite3.connect(str(self.path), timeout=10)
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=NORMAL")
            c.row_factory=sqlite3.Row
            return c
        except sqlite3.OperationalError as e:
            raise RuntimeError(f"Unable to open HYDRA database at {self.path}: {e}") from e
    def _init(self):
        with self._conn() as c:
            c.execute("CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL, prev_hash TEXT, hash TEXT NOT NULL)")
            c.execute("CREATE TABLE IF NOT EXISTS trades(id TEXT PRIMARY KEY, opened_at TEXT, closed_at TEXT, symbol TEXT, qty INTEGER, entry REAL, exit REAL, gross REAL, costs REAL, net REAL, side TEXT, reason TEXT)")
    def append(self,kind:str,payload:dict):
        with self.lock, self._conn() as c:
            row=c.execute("SELECT hash FROM events ORDER BY id DESC LIMIT 1").fetchone(); prev=row[0] if row else "GENESIS"
            ts=now_ist().isoformat(); body=json.dumps(payload,sort_keys=True,default=str,separators=(",",":")); h=hashlib.sha256((prev+ts+kind+body).encode()).hexdigest()
            c.execute("INSERT INTO events(ts,kind,payload,prev_hash,hash) VALUES(?,?,?,?,?)",(ts,kind,body,prev,h))
    def trade(self,row:dict):
        with self._conn() as c:
            c.execute("INSERT OR REPLACE INTO trades(id,opened_at,closed_at,symbol,qty,entry,exit,gross,costs,net,side,reason) VALUES(:id,:opened_at,:closed_at,:symbol,:qty,:entry,:exit,:gross,:costs,:net,:side,:reason)",row)
    def events(self,limit=80):
        with self._conn() as c:
            rows=c.execute("SELECT id,ts,kind,payload,hash FROM events ORDER BY id DESC LIMIT ?",(limit,)).fetchall()
            return [{"id":r[0],"ts":r[1],"kind":r[2],"payload":json.loads(r[3]),"hash":r[4][:12]} for r in rows]
    def trades(self,limit=100):
        with self._conn() as c:
            return [dict(r) for r in c.execute("SELECT * FROM trades ORDER BY opened_at DESC LIMIT ?",(limit,)).fetchall()]
    def verify_chain(self):
        """Verify every event hash against its predecessor and payload.
        Returns a compact audit result without exposing or truncating hashes.
        """
        with self._conn() as c:
            rows=c.execute("SELECT id,ts,kind,payload,prev_hash,hash FROM events ORDER BY id").fetchall()
        prev="GENESIS"
        for r in rows:
            body=r[3]
            expected=hashlib.sha256((prev+r[1]+r[2]+body).encode()).hexdigest()
            if r[4] != prev or r[5] != expected:
                return {"valid":False,"event_id":r[0],"reason":"HASH_MISMATCH"}
            prev=r[5]
        return {"valid":True,"events":len(rows)}

