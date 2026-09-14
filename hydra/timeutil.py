from __future__ import annotations

from datetime import datetime, time, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def _load_ist() -> tzinfo:
    """Return Asia/Kolkata when tzdata is available, otherwise fixed IST.

    India has no daylight-saving transition, so UTC+05:30 is a safe runtime
    fallback on minimal Termux/Python installations where the IANA zone database
    is absent. We still prefer ZoneInfo so canonical timezone metadata is used
    whenever available.
    """
    try:
        return ZoneInfo("Asia/Kolkata")
    except (ZoneInfoNotFoundError, ModuleNotFoundError):
        return timezone(timedelta(hours=5, minutes=30), name="IST")


IST = _load_ist()


def now_ist() -> datetime:
    return datetime.now(IST)


def ensure_ist(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def market_session(dt: datetime) -> bool:
    dt = ensure_ist(dt)
    return dt.weekday() < 5 and time(9, 15) <= dt.time() <= time(15, 30)
