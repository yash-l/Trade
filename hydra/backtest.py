from __future__ import annotations
import csv
from datetime import datetime
from zoneinfo import ZoneInfo
from .models import Candle
from .engine import HydraEngine

IST = ZoneInfo("Asia/Kolkata")


def _parse_ts(value: str) -> datetime:
    text = str(value).strip()
    # Accept normal ISO-8601 plus the common trailing Z form.
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        ts = datetime.fromisoformat(text)
    except ValueError:
        # Conservative fallback for common CSV timestamps.
        ts = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    return ts.replace(tzinfo=IST) if ts.tzinfo is None else ts.astimezone(IST)


async def replay_csv(engine: HydraEngine, path: str):
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames:
            raise ValueError("CSV has no header")
        fieldmap = {name.lower(): name for name in reader.fieldnames}
        required = {"timestamp", "open", "high", "low", "close"}
        if not required.issubset(fieldmap):
            raise ValueError("CSV needs timestamp,open,high,low,close[,volume]")
        for row in reader:
            def val(name: str, default: float = 0.0) -> float:
                raw = row.get(fieldmap.get(name, ""), "")
                return float(raw) if raw not in (None, "") else default
            await engine.process_candle(
                Candle(
                    _parse_ts(row[fieldmap["timestamp"]]),
                    val("open"), val("high"), val("low"), val("close"), val("volume", 0.0),
                )
            )
