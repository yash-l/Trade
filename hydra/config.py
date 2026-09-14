from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import os
from dotenv import load_dotenv

APP_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(APP_ROOT / ".env")

def _path(name: str, default_rel: str) -> Path:
    raw = os.getenv(name, default_rel)
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = APP_ROOT / path
    return path.resolve()


def _b(name: str, default: bool=False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1","true","yes","on"}

def _f(name: str, default: float) -> float:
    try: return float(os.getenv(name, default))
    except Exception: return default

def _i(name: str, default: int) -> int:
    try: return int(os.getenv(name, default))
    except Exception: return default

@dataclass(frozen=True)
class Settings:
    mode: str = os.getenv("HYDRA_MODE", "paper").lower()
    data_mode: str = os.getenv("HYDRA_DATA_MODE", "demo").lower()
    host: str = os.getenv("HYDRA_HOST", "127.0.0.1")
    port: int = _i("HYDRA_PORT", 8181)
    capital: float = _f("HYDRA_CAPITAL", 15000)
    normal_risk: float = _f("HYDRA_NORMAL_RISK", 275)
    hard_risk: float = _f("HYDRA_HARD_RISK", 375)
    daily_loss: float = _f("HYDRA_DAILY_LOSS", 600)
    max_trades: int = _i("HYDRA_MAX_TRADES", 2)
    score_threshold: int = _i("HYDRA_SCORE_THRESHOLD", 85)
    lot_size: int = _i("HYDRA_LOT_SIZE", 65)
    max_capital_per_trade: float = _f("HYDRA_MAX_CAPITAL_PER_TRADE", 8000)
    max_spread_pct: float = _f("HYDRA_MAX_SPREAD_PCT", 1.0)
    preferred_spread_pct: float = _f("HYDRA_PREFERRED_SPREAD_PCT", 0.5)
    max_price_drift_pct: float = _f("HYDRA_MAX_PRICE_DRIFT_PCT", 1.5)
    max_quote_age_sec: float = _f("HYDRA_MAX_QUOTE_AGE_SEC", 3.0)
    breakeven_trigger_r: float = _f("HYDRA_BREAKEVEN_TRIGGER_R", 1.0)
    breakeven_lock_r: float = _f("HYDRA_BREAKEVEN_LOCK_R", 0.05)
    trailing_stop_trigger_r: float = _f("HYDRA_TRAILING_STOP_TRIGGER_R", 1.4)
    trailing_stop_buffer_r: float = _f("HYDRA_TRAILING_STOP_BUFFER_R", 0.10)
    trailing_target_trigger_r: float = _f("HYDRA_TRAILING_TARGET_TRIGGER_R", 1.6)
    trailing_target_gap_r: float = _f("HYDRA_TRAILING_TARGET_GAP_R", 0.75)
    trailing_target_step_r: float = _f("HYDRA_TRAILING_TARGET_STEP_R", 0.25)
    max_target_r: float = _f("HYDRA_MAX_TARGET_R", 4.0)
    db_path: Path = _path("HYDRA_DB_PATH", "data/hydra.sqlite3")
    app_key: str = os.getenv("HYDRA_5PAISA_APP_KEY", "")
    user_id: str = os.getenv("HYDRA_5PAISA_USER_ID", "")
    encryption_key: str = os.getenv("HYDRA_5PAISA_ENCRYPTION_KEY", "")
    access_token: str = os.getenv("HYDRA_5PAISA_ACCESS_TOKEN", "")
    client_code: str = os.getenv("HYDRA_5PAISA_CLIENT_CODE", "")
    nifty_scrip_code: int = _i("HYDRA_5PAISA_NIFTY_SCRIP_CODE", 0)
    nifty_exch_type: str = os.getenv("HYDRA_5PAISA_NIFTY_EXCH_TYPE", "C")
    nifty_symbol: str = os.getenv("HYDRA_5PAISA_NIFTY_SYMBOL", "NIFTY")
    option_strike_step: int = _i("HYDRA_5PAISA_OPTION_STRIKE_STEP", 50)
    min_dte: int = _i("HYDRA_5PAISA_MIN_DTE", 2)
    scrip_master_url: str = os.getenv("HYDRA_5PAISA_SCRIP_MASTER_URL", "https://Openapi.5paisa.com/VendorsAPI/Service1.svc/ScripMaster/segment/All")
    live_enabled: bool = _b("HYDRA_LIVE_ENABLED", False)
    live_confirm: str = os.getenv("HYDRA_LIVE_CONFIRM", "")
    static_ip_confirmed: bool = _b("HYDRA_STATIC_IP_CONFIRMED", False)
    algo_id: int = _i("HYDRA_ALGO_ID", 0)
    adaptive_enabled: bool = _b("HYDRA_ADAPTIVE_ENABLED", True)
    adaptive_shadow_only: bool = _b("HYDRA_ADAPTIVE_SHADOW_ONLY", True)
    adaptive_trail_shadow_only: bool = _b("HYDRA_ADAPTIVE_TRAIL_SHADOW_ONLY", True)
    adaptive_loss_days: int = _i("HYDRA_ADAPTIVE_LOSS_DAYS", 5)
    adaptive_loss_count: int = _i("HYDRA_ADAPTIVE_LOSS_COUNT", 3)
    adaptive_threshold_step: int = _i("HYDRA_ADAPTIVE_THRESHOLD_STEP", 10)
    adaptive_max_threshold: int = _i("HYDRA_ADAPTIVE_MAX_THRESHOLD", 95)
    adaptive_min_buffer_r: float = _f("HYDRA_ADAPTIVE_MIN_BUFFER_R", 0.05)
    adaptive_max_buffer_r: float = _f("HYDRA_ADAPTIVE_MAX_BUFFER_R", 0.20)
    adaptive_buffer_step_r: float = _f("HYDRA_ADAPTIVE_BUFFER_STEP_R", 0.05)
    adaptive_min_breakeven_r: float = _f("HYDRA_ADAPTIVE_MIN_BREAKEVEN_R", 0.50)
    adaptive_max_breakeven_r: float = _f("HYDRA_ADAPTIVE_MAX_BREAKEVEN_R", 1.00)
    adaptive_breakeven_step_r: float = _f("HYDRA_ADAPTIVE_BREAKEVEN_STEP_R", 0.10)
    adaptive_min_shadow_trades: int = _i("HYDRA_ADAPTIVE_MIN_SHADOW_TRADES", 50)

    def validate(self) -> list[str]:
        errs=[]
        if self.capital <= 0: errs.append("capital must be > 0")
        if not (0 < self.normal_risk <= self.hard_risk): errs.append("normal_risk must be >0 and <= hard_risk")
        if self.hard_risk > self.daily_loss: errs.append("hard_risk must be <= daily_loss")
        if self.max_trades < 1: errs.append("max_trades must be >=1")
        if self.breakeven_trigger_r <= 0: errs.append("breakeven_trigger_r must be >0")
        if self.breakeven_lock_r < 0: errs.append("breakeven_lock_r must be >=0")
        if self.trailing_stop_trigger_r < self.breakeven_trigger_r: errs.append("trailing_stop_trigger_r must be >= breakeven_trigger_r")
        if self.trailing_stop_buffer_r < 0: errs.append("trailing_stop_buffer_r must be >=0")
        if self.trailing_target_trigger_r <= 0: errs.append("trailing_target_trigger_r must be >0")
        if self.trailing_target_gap_r <= 0: errs.append("trailing_target_gap_r must be >0")
        if self.trailing_target_step_r <= 0: errs.append("trailing_target_step_r must be >0")
        if self.max_target_r < 2.0: errs.append("max_target_r must be >=2.0")
        if self.mode not in {"paper","live"}: errs.append("mode must be paper|live")
        if self.data_mode not in {"demo","5paisa","replay"}: errs.append("data_mode must be demo|5paisa|replay")
        if self.option_strike_step <= 0: errs.append("option_strike_step must be >0")
        if self.min_dte < 0: errs.append("min_dte must be >=0")
        if self.adaptive_loss_days < 1: errs.append("adaptive_loss_days must be >=1")
        if self.adaptive_loss_count < 1: errs.append("adaptive_loss_count must be >=1")
        if self.adaptive_threshold_step < 0: errs.append("adaptive_threshold_step must be >=0")
        if not (0.05 <= self.adaptive_min_buffer_r <= self.adaptive_max_buffer_r <= 0.20): errs.append("adaptive buffer bounds must be within 0.05..0.20R")
        if not (0.50 <= self.adaptive_min_breakeven_r <= self.adaptive_max_breakeven_r <= 1.50): errs.append("adaptive breakeven bounds must be within 0.50..1.50R")
        if self.adaptive_min_shadow_trades < 50: errs.append("adaptive_min_shadow_trades must be >=50")
        if self.data_mode == "5paisa":
            if not self.access_token: errs.append("5paisa data blocked: access token missing")
            if not self.client_code: errs.append("5paisa data blocked: client code missing")
            if not self.app_key: errs.append("5paisa data blocked: app key missing")
        if self.mode == "live":
            if not self.live_enabled: errs.append("LIVE blocked: HYDRA_LIVE_ENABLED=false")
            if self.live_confirm != "I_UNDERSTAND_REAL_ORDERS": errs.append("LIVE blocked: second confirmation missing")
            if not self.static_ip_confirmed: errs.append("LIVE blocked: static-IP compliance not confirmed")
        return errs
