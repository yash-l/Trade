from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, PlainTextResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from .config import APP_ROOT, Settings
from .engine import HydraEngine

cfg = Settings()
engine = HydraEngine(cfg)
STATIC = Path(__file__).resolve().parent.parent / "dashboard"
ENV_PATH = APP_ROOT / ".env"


def _upsert_env(values: dict[str, str]):
    lines = ENV_PATH.read_text().splitlines() if ENV_PATH.exists() else []
    keys = set(values)
    out = []
    seen = set()
    for line in lines:
        if "=" in line and not line.lstrip().startswith("#"):
            k = line.split("=", 1)[0].strip()
            if k in values:
                out.append(f"{k}={values[k]}")
                seen.add(k)
                continue
        out.append(line)
    for k in keys - seen:
        out.append(f"{k}={values[k]}")
    ENV_PATH.write_text("\n".join(out).rstrip() + "\n")
    try:
        os.chmod(ENV_PATH, 0o600)
    except Exception:
        pass



SAFE_SETTINGS = {
    "HYDRA_SCORE_THRESHOLD": int,
    "HYDRA_MAX_SPREAD_PCT": float,
    "HYDRA_PREFERRED_SPREAD_PCT": float,
    "HYDRA_MAX_PRICE_DRIFT_PCT": float,
    "HYDRA_MAX_QUOTE_AGE_SEC": float,
    "HYDRA_BREAKEVEN_TRIGGER_R": float,
    "HYDRA_BREAKEVEN_LOCK_R": float,
    "HYDRA_TRAILING_STOP_TRIGGER_R": float,
    "HYDRA_TRAILING_STOP_BUFFER_R": float,
    "HYDRA_TRAILING_TARGET_TRIGGER_R": float,
    "HYDRA_TRAILING_TARGET_GAP_R": float,
    "HYDRA_TRAILING_TARGET_STEP_R": float,
    "HYDRA_MAX_TARGET_R": float,
    "HYDRA_ADAPTIVE_ENABLED": bool,
    "HYDRA_ADAPTIVE_SHADOW_ONLY": bool,
    "HYDRA_ADAPTIVE_TRAIL_SHADOW_ONLY": bool,
    "HYDRA_ADAPTIVE_LOSS_DAYS": int,
    "HYDRA_ADAPTIVE_LOSS_COUNT": int,
    "HYDRA_ADAPTIVE_THRESHOLD_STEP": int,
    "HYDRA_ADAPTIVE_MAX_THRESHOLD": int,
    "HYDRA_ADAPTIVE_MIN_BUFFER_R": float,
    "HYDRA_ADAPTIVE_MAX_BUFFER_R": float,
    "HYDRA_ADAPTIVE_BUFFER_STEP_R": float,
    "HYDRA_ADAPTIVE_MIN_BREAKEVEN_R": float,
    "HYDRA_ADAPTIVE_MAX_BREAKEVEN_R": float,
    "HYDRA_ADAPTIVE_BREAKEVEN_STEP_R": float,
    "HYDRA_ADAPTIVE_MIN_SHADOW_TRADES": int,
}


def _env_snapshot():
    current=Settings()
    field_map={
        "HYDRA_SCORE_THRESHOLD":"score_threshold","HYDRA_MAX_SPREAD_PCT":"max_spread_pct","HYDRA_PREFERRED_SPREAD_PCT":"preferred_spread_pct",
        "HYDRA_MAX_PRICE_DRIFT_PCT":"max_price_drift_pct","HYDRA_MAX_QUOTE_AGE_SEC":"max_quote_age_sec","HYDRA_BREAKEVEN_TRIGGER_R":"breakeven_trigger_r",
        "HYDRA_BREAKEVEN_LOCK_R":"breakeven_lock_r","HYDRA_TRAILING_STOP_TRIGGER_R":"trailing_stop_trigger_r","HYDRA_TRAILING_STOP_BUFFER_R":"trailing_stop_buffer_r",
        "HYDRA_TRAILING_TARGET_TRIGGER_R":"trailing_target_trigger_r","HYDRA_TRAILING_TARGET_GAP_R":"trailing_target_gap_r","HYDRA_TRAILING_TARGET_STEP_R":"trailing_target_step_r",
        "HYDRA_MAX_TARGET_R":"max_target_r","HYDRA_ADAPTIVE_ENABLED":"adaptive_enabled","HYDRA_ADAPTIVE_SHADOW_ONLY":"adaptive_shadow_only",
        "HYDRA_ADAPTIVE_TRAIL_SHADOW_ONLY":"adaptive_trail_shadow_only","HYDRA_ADAPTIVE_LOSS_DAYS":"adaptive_loss_days","HYDRA_ADAPTIVE_LOSS_COUNT":"adaptive_loss_count",
        "HYDRA_ADAPTIVE_THRESHOLD_STEP":"adaptive_threshold_step","HYDRA_ADAPTIVE_MAX_THRESHOLD":"adaptive_max_threshold","HYDRA_ADAPTIVE_MIN_BUFFER_R":"adaptive_min_buffer_r",
        "HYDRA_ADAPTIVE_MAX_BUFFER_R":"adaptive_max_buffer_r","HYDRA_ADAPTIVE_BUFFER_STEP_R":"adaptive_buffer_step_r","HYDRA_ADAPTIVE_MIN_BREAKEVEN_R":"adaptive_min_breakeven_r",
        "HYDRA_ADAPTIVE_MAX_BREAKEVEN_R":"adaptive_max_breakeven_r","HYDRA_ADAPTIVE_BREAKEVEN_STEP_R":"adaptive_breakeven_step_r","HYDRA_ADAPTIVE_MIN_SHADOW_TRADES":"adaptive_min_shadow_trades",
    }
    values={k:getattr(current,field) for k,field in field_map.items()}
    values.update({
        "HYDRA_MODE": current.mode, "HYDRA_DATA_MODE": current.data_mode, "HYDRA_HARD_RISK": current.hard_risk,
        "HYDRA_DAILY_LOSS": current.daily_loss, "HYDRA_MAX_TRADES": current.max_trades,
        "HYDRA_LIVE_ENABLED": current.live_enabled, "HYDRA_STATIC_IP_CONFIRMED": current.static_ip_confirmed, "HYDRA_ALGO_ID": current.algo_id,
    })
    return values


def _read_env_value(key):
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text().splitlines():
            if line.startswith(key+"="): return line.split("=",1)[1]
    return os.getenv(key)


def _write_safe_settings(values):
    clean={}
    for key,val in values.items():
        if key not in SAFE_SETTINGS: continue
        typ=SAFE_SETTINGS[key]
        if typ is bool: clean[key]="true" if bool(val) else "false"
        elif typ is int: clean[key]=str(int(val))
        else: clean[key]=str(float(val))
    # Validate proposed config by overlaying environment in a child-like temporary view.
    old={k:os.environ.get(k) for k in clean}
    try:
        for k,v in clean.items(): os.environ[k]=v
        proposed=Settings(); errs=proposed.validate()
    finally:
        for k,v in old.items():
            if v is None: os.environ.pop(k,None)
            else: os.environ[k]=v
    if errs: raise ValueError("; ".join(errs))
    _upsert_env(clean)
    return clean


async def settings(request: Request):
    return JSONResponse({"values":_env_snapshot(),"restart_required":True,"editable":sorted(SAFE_SETTINGS),"locked":["HYDRA_MODE","HYDRA_DATA_MODE","HYDRA_HARD_RISK","HYDRA_DAILY_LOSS","HYDRA_MAX_TRADES","HYDRA_LIVE_ENABLED","HYDRA_STATIC_IP_CONFIRMED","HYDRA_ALGO_ID"]})


async def save_settings(request: Request):
    try:
        body=await request.json(); saved=_write_safe_settings(body.get("values",{}))
        engine.journal.append("SETTINGS_UPDATED", {"keys":sorted(saved),"restart_required":True})
        return JSONResponse({"ok":True,"saved":sorted(saved),"restart_required":True})
    except Exception as exc:
        return JSONResponse({"ok":False,"error":str(exc)},status_code=400)


def _csv_response(rows, filename):
    import csv, io, json
    if not rows: text="\n"
    else:
        keys=sorted({k for r in rows for k in r})
        buf=io.StringIO(); w=csv.DictWriter(buf,fieldnames=keys); w.writeheader();
        for r in rows: w.writerow({k:(json.dumps(r[k],default=str) if isinstance(r.get(k), (dict,list)) else r.get(k)) for k in keys})
        text=buf.getvalue()
    return PlainTextResponse(text,media_type="text/csv",headers={"Content-Disposition":f'attachment; filename="{filename}"'})


async def export_trades(request: Request): return _csv_response(engine.journal.trades(5000),"hydra-trades.csv")
async def export_events(request: Request): return _csv_response(engine.journal.events(5000),"hydra-events.csv")


async def export_training(request: Request):
    import json, csv, io
    events=engine.journal.events(5000); features={}; outcomes=[]
    for e in events:
        payload=e.get("payload",{})
        if e.get("kind")=="SIGNAL_FEATURES": features[payload.get("signal_id")]=payload
        elif e.get("kind")=="TRADE_OUTCOME": outcomes.append(payload)
    trades={r.get("id"):r for r in engine.journal.trades(5000)}
    rows=[]
    for o in outcomes:
        f=features.get(o.get("signal_id"),{}); t=trades.get(o.get("position_id"),{})
        rows.append({**{k:v for k,v in f.items() if k!="event_ts"},**{f"outcome_{k}":v for k,v in o.items() if k!="event_ts"},**{f"trade_{k}":v for k,v in t.items() if k!="id"}})
    return _csv_response(rows,"hydra-training-data.csv")


@asynccontextmanager
async def lifespan(app: Starlette):
    await engine.start()
    try:
        yield
    finally:
        await engine.stop()


async def root(request: Request):
    return FileResponse(
        STATIC / "index.html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
            "X-HYDRA-UI-Build": "2.3.0-premium-paper",
        },
    )


async def status(request: Request):
    return JSONResponse(engine.snapshot(), headers={"Cache-Control": "no-store"})


async def health(request: Request):
    snap = engine.snapshot()
    return JSONResponse(
        {
            "ok": True,
            "version": snap.get("version"),
            "ui_build": "2.3.0-premium-paper",
            "status": snap.get("status"),
            "mode": snap.get("mode"),
            "data_mode": snap.get("data_mode"),
            "feed_status": snap.get("feed_status"),
        },
        headers={"Cache-Control": "no-store"},
    )


async def events(request: Request):
    return JSONResponse(engine.journal.events(100))


async def trades(request: Request):
    return JSONResponse(engine.journal.trades(200))


async def oauth_url(request: Request):
    response_url = request.query_params.get("response_url", "http://127.0.0.1:8181/")
    try:
        return JSONResponse({"url": engine.five.oauth_url(response_url)})
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)


async def exchange_token(request: Request):
    try:
        body = await request.json()
        token = str(body.get("request_token", "")).strip()
        if not token:
            return JSONResponse({"error": "request_token is required"}, status_code=400)
        result = await engine.five.exchange_request_token(token)
        access_token = str(result.pop("access_token", ""))
        _upsert_env(
            {
                "HYDRA_5PAISA_ACCESS_TOKEN": access_token,
                "HYDRA_5PAISA_CLIENT_CODE": str(result.get("client_code", "")),
            }
        )
        engine.health["broker"] = "CONFIRMED"
        engine.journal.append("AUTH_OK", {**result, "token_saved": True})
        return JSONResponse({**result, "token_saved": True, "restart_required": True})
    except Exception as exc:
        engine.journal.append("AUTH_FAIL", {"error": str(exc)})
        return JSONResponse({"error": str(exc)}, status_code=400)


async def reconcile(request: Request):
    try:
        ob = await engine.five.order_book()
        tb = await engine.five.trade_book()
        pos = await engine.five.positions()
        mar = await engine.five.margin()
        engine.journal.append(
            "RECONCILE_SNAPSHOT",
            {
                "order_book": "fetched",
                "trade_book": "fetched",
                "positions": "fetched",
                "margin": "fetched",
            },
        )
        return JSONResponse(
            {"order_book": ob, "trade_book": tb, "positions": pos, "margin": mar}
        )
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)


async def live_data_check(request: Request):
    """Read-only 5paisa diagnostics. Never places or modifies an order."""
    try:
        code = await engine.five.resolve_nifty_underlying()
        feed = await engine.five.market_feed_snapshot(
            [{"Exch": "N", "ExchType": engine.cfg.nifty_exch_type, "ScripCode": code, "ScripData": ""}]
        )
        margin = await engine.five.margin()
        return JSONResponse(
            {
                "ok": True,
                "orders_sent": 0,
                "authenticated": True,
                "nifty_scrip_code": code,
                "market_feed": feed,
                "margin": margin,
            }
        )
    except Exception as exc:
        return JSONResponse({"ok": False, "orders_sent": 0, "error": str(exc)}, status_code=400)


routes = [
    Route("/", root),
    Route("/api/status", status),
    Route("/api/settings", settings),
    Route("/api/settings/save", save_settings, methods=["POST"]),
    Route("/api/export/trades.csv", export_trades),
    Route("/api/export/events.csv", export_events),
    Route("/api/export/training.csv", export_training),
    Route("/api/health", health),
    Route("/api/events", events),
    Route("/api/trades", trades),
    Route("/api/5paisa/oauth-url", oauth_url),
    Route("/api/5paisa/exchange-token", exchange_token, methods=["POST"]),
    Route("/api/5paisa/reconcile", reconcile),
    Route("/api/5paisa/live-data-check", live_data_check),
    Mount("/static", app=StaticFiles(directory=STATIC), name="static"),
]

app = Starlette(debug=False, routes=routes, lifespan=lifespan)
