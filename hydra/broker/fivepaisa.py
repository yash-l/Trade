from __future__ import annotations

import base64
import csv
import json
import math
import re
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta
from io import StringIO
from urllib.parse import quote

import httpx
import websockets

from ..config import Settings
from ..models import Quote
from ..timeutil import IST, now_ist


class FivePaisaError(RuntimeError):
    pass


@dataclass(frozen=True)
class OptionContract:
    scrip_code: int
    symbol: str
    option_type: str
    strike: float
    expiry: date
    lot_size: int = 0
    exch: str = "N"
    exch_type: str = "D"

    def dict(self):
        d = asdict(self)
        d["expiry"] = self.expiry.isoformat()
        return d


class FivePaisaAdapter:
    """Thin verified-TLS adapter around current 5paisa/XStream endpoints.

    py5paisa is intentionally not required. The public SDK remains useful as a
    payload reference, but HYDRA keeps its own fail-closed transport layer.
    """

    BASE = "https://Openapi.5paisa.com"

    def __init__(self, cfg: Settings):
        self.cfg = cfg
        self.access_token = cfg.access_token
        self.client_code = cfg.client_code
        self.http = httpx.AsyncClient(
            timeout=httpx.Timeout(15.0, connect=10.0),
            verify=True,
            headers={"Content-Type": "application/json"},
            follow_redirects=True,
        )
        self._scrip_rows: list[dict] | None = None
        self._scrip_loaded_at: datetime | None = None

    async def close(self):
        await self.http.aclose()

    def oauth_url(self, response_url: str, state: str = "HYDRA") -> str:
        if not self.cfg.app_key:
            raise FivePaisaError("HYDRA_5PAISA_APP_KEY missing")
        return (
            "https://dev-openapi.5paisa.com/WebVendorLogin/VLogin/Index"
            f"?VendorKey={quote(self.cfg.app_key)}"
            f"&ResponseURL={quote(response_url, safe='')}"
            f"&State={quote(state)}"
        )

    async def exchange_request_token(self, request_token: str) -> dict:
        if not all([self.cfg.app_key, self.cfg.user_id, self.cfg.encryption_key]):
            raise FivePaisaError("AppKey/UserID/EncryptionKey missing")
        payload = {
            "head": {"Key": self.cfg.app_key},
            "body": {
                "RequestToken": request_token,
                "EncryKey": self.cfg.encryption_key,
                "UserId": self.cfg.user_id,
            },
        }
        r = await self.http.post(
            f"{self.BASE}/VendorsAPI/Service1.svc/GetAccessToken", json=payload
        )
        r.raise_for_status()
        data = r.json()
        body = data.get("body") or {}
        if not body.get("AccessToken"):
            raise FivePaisaError(f"access-token failed: {data}")
        self.access_token = body["AccessToken"]
        self.client_code = str(body.get("ClientCode", ""))
        return {
            "client_code": self.client_code,
            "access_token": self.access_token,
            "allow_nse_deriv": body.get("AllowNseDeriv"),
            "status": body.get("Status"),
            "message": body.get("Message"),
        }

    def _headers(self):
        if not self.access_token:
            raise FivePaisaError("AccessToken missing")
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    def _body(self):
        return {"head": {"key": self.cfg.app_key}, "body": {"ClientCode": self.client_code}}

    async def _post(self, path: str, payload: dict):
        r = await self.http.post(
            f"{self.BASE}{path}", headers=self._headers(), json=payload
        )
        r.raise_for_status()
        data = r.json()
        head = data.get("head") or {}
        if str(head.get("status", "0")) not in {"0", ""}:
            raise FivePaisaError(
                f"5paisa API error {head.get('status')}: {head.get('statusDescription')}"
            )
        return data

    async def margin(self):
        return await self._post("/VendorsAPI/Service1.svc/V4/Margin", self._body())

    async def positions(self):
        return await self._post("/VendorsAPI/Service1.svc/V3/NetPositionNetWise", self._body())

    async def order_book(self):
        return await self._post("/VendorsAPI/Service1.svc/V3/OrderBook", self._body())

    async def trade_book(self):
        return await self._post("/VendorsAPI/Service1.svc/V1/TradeBook", self._body())

    async def order_status(self, remote_order_id: str, exch: str = "N"):
        p = self._body()
        p["body"]["OrdStatusReqList"] = [
            {"Exch": exch, "RemoteOrderID": remote_order_id}
        ]
        return await self._post("/VendorsAPI/Service1.svc/V3/OrderStatus", p)

    async def historical(
        self,
        exch: str,
        exch_type: str,
        scrip_code: int,
        interval: str,
        from_date: str,
        end_date: str,
    ):
        url = (
            f"{self.BASE}/V2/historical/{exch}/{exch_type}/{scrip_code}/{interval}"
            f"?from={from_date}&end={end_date}"
        )
        r = await self.http.get(url, headers=self._headers())
        r.raise_for_status()
        return r.json()

    async def market_feed_snapshot(self, instruments: list[dict]):
        p = self._body()
        p["body"].update(
            {
                "MarketFeedData": instruments,
                "ClientLoginType": 0,
                "LastRequestTime": f"/Date({int(now_ist().timestamp()*1000)})/",
                "RefreshRate": "H",
            }
        )
        return await self._post("/VendorsAPI/Service1.svc/V1/MarketFeed", p)

    async def market_depth(self, scrip_code: int, exch: str = "N", exch_type: str = "D"):
        p = self._body()
        p["body"].update(
            {
                "Exchange": exch,
                "ExchangeType": exch_type,
                "ScripCode": int(scrip_code),
                "ScripData": "",
            }
        )
        return await self._post("/VendorsAPI/Service1.svc/V2/MarketDepth", p)

    @staticmethod
    def _parse_5paisa_date(value) -> datetime | None:
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            try:
                return datetime.fromtimestamp(float(value) / 1000.0, tz=IST)
            except Exception:
                return None
        s = str(value).strip()
        m = re.search(r"/Date\((-?\d+)", s)
        if m:
            try:
                ms = int(m.group(1))
                # .NET minimum / invalid placeholder should not become a real quote timestamp.
                if ms <= 0:
                    return None
                return datetime.fromtimestamp(ms / 1000.0, tz=IST)
            except Exception:
                return None
        if s.isdigit():
            try:
                v = int(s)
                if v > 10_000_000_000:
                    v = v / 1000.0
                return datetime.fromtimestamp(v, tz=IST)
            except Exception:
                pass
        for fmt in (
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%d-%b-%Y",
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%m/%d/%Y",
        ):
            try:
                d = datetime.strptime(s, fmt)
                return d.astimezone(IST) if d.tzinfo else d.replace(tzinfo=IST)
            except Exception:
                pass
        return None

    @classmethod
    def tick_datetime(cls, item: dict) -> datetime:
        for key in ("TickDt", "TimeStamp", "Timestamp", "LastTradeTime"):
            d = cls._parse_5paisa_date(item.get(key))
            if d:
                return d
        return now_ist()

    @staticmethod
    def quote_from_feed_item(
        item: dict,
        *,
        symbol: str,
        scrip_code: int,
        previous: Quote | None = None,
        option_type: str | None = None,
        strike: float | None = None,
        expiry: str | None = None,
    ) -> Quote | None:
        try:
            ltp = float(item.get("LastRate") or (previous.ltp if previous else 0) or 0)
            bid = float(item.get("BidRate") or (previous.bid if previous else 0) or 0)
            ask = float(item.get("OffRate") or item.get("AskRate") or (previous.ask if previous else 0) or 0)
            if ltp <= 0 and bid > 0 and ask > 0:
                ltp = (bid + ask) / 2
            if ltp <= 0:
                return None
            return Quote(
                ts=FivePaisaAdapter.tick_datetime(item),
                symbol=symbol,
                scrip_code=int(scrip_code),
                bid=bid,
                ask=ask,
                ltp=ltp,
                bid_qty=int(float(item.get("BidQty") or (previous.bid_qty if previous else 0) or 0)),
                ask_qty=int(float(item.get("OffQty") or item.get("AskQty") or (previous.ask_qty if previous else 0) or 0)),
                strike=strike,
                option_type=option_type,
                expiry=expiry,
            )
        except Exception:
            return None

    @staticmethod
    def quote_from_market_depth(
        data: dict,
        contract: OptionContract,
        *,
        ts: datetime | None = None,
    ) -> Quote:
        body = data.get("body") or data
        depth = body.get("MarketDepthData") or body.get("Data") or []
        bids: list[tuple[float, int]] = []
        asks: list[tuple[float, int]] = []
        for row in depth:
            if not isinstance(row, dict):
                continue
            try:
                flag = int(float(row.get("BbBuySellFlag") or 0))
                price = float(row.get("Price") or 0)
                qty = int(float(row.get("Quantity") or 0))
            except Exception:
                continue
            if price <= 0:
                continue
            if flag == 66:
                bids.append((price, qty))
            elif flag == 83:
                asks.append((price, qty))
        if not bids or not asks:
            raise FivePaisaError(f"NO_EXECUTABLE_DEPTH:{contract.scrip_code}")
        bid, bid_qty = max(bids, key=lambda x: x[0])
        ask, ask_qty = min(asks, key=lambda x: x[0])
        qts = ts or FivePaisaAdapter._parse_5paisa_date(body.get("TimeStamp")) or now_ist()
        return Quote(
            ts=qts,
            symbol=contract.symbol,
            scrip_code=contract.scrip_code,
            bid=bid,
            ask=ask,
            ltp=(bid + ask) / 2,
            bid_qty=bid_qty,
            ask_qty=ask_qty,
            strike=contract.strike,
            option_type=contract.option_type,
            expiry=contract.expiry.isoformat(),
        )

    async def executable_quote(self, contract: OptionContract) -> Quote:
        return self.quote_from_market_depth(
            await self.market_depth(contract.scrip_code, contract.exch, contract.exch_type),
            contract,
        )

    async def scrip_master(self, force: bool = False) -> list[dict]:
        if (
            not force
            and self._scrip_rows is not None
            and self._scrip_loaded_at is not None
            and now_ist() - self._scrip_loaded_at < timedelta(minutes=30)
        ):
            return self._scrip_rows
        r = await self.http.get(self.cfg.scrip_master_url)
        r.raise_for_status()
        text = r.text.lstrip("\ufeff").strip()
        rows = list(csv.DictReader(StringIO(text)))
        if not rows:
            raise FivePaisaError("SCRIP_MASTER_EMPTY")
        self._scrip_rows = rows
        self._scrip_loaded_at = now_ist()
        return rows

    @staticmethod
    def _field(row: dict, *names: str, default=""):
        lower = {str(k).strip().lower(): v for k, v in row.items()}
        for name in names:
            key = name.lower()
            if key in lower and lower[key] not in (None, ""):
                return lower[key]
        return default

    @classmethod
    def _row_expiry(cls, row: dict) -> date | None:
        raw = cls._field(row, "Expiry", "ExpiryDate", "ExpiryDateString")
        d = cls._parse_5paisa_date(raw)
        return d.date() if d else None

    @classmethod
    def _norm_scrip(cls, row: dict) -> dict:
        def num(*names, default=0.0):
            try:
                return float(cls._field(row, *names, default=default) or default)
            except Exception:
                return float(default)

        try:
            sc = int(float(cls._field(row, "ScripCode", "Token", default=0) or 0))
        except Exception:
            sc = 0
        try:
            lot = int(float(cls._field(row, "LotSize", "MarketLot", default=0) or 0))
        except Exception:
            lot = 0
        return {
            "exch": str(cls._field(row, "Exch", "Exchange")).strip().upper(),
            "exch_type": str(cls._field(row, "ExchType", "ExchangeType")).strip().upper(),
            "root": str(cls._field(row, "SymbolRoot", "Root", "Symbol")).strip().upper(),
            "scrip_type": str(cls._field(row, "ScripType", "OptionType", "CPType")).strip().upper(),
            "strike": num("StrikeRate", "StrikePrice", "Strike"),
            "expiry": cls._row_expiry(row),
            "scrip_code": sc,
            "lot_size": lot,
            "symbol": str(
                cls._field(
                    row,
                    "Name",
                    "FullName",
                    "ScripData",
                    "DisplayName",
                    default="",
                )
            ).strip(),
        }

    async def resolve_nifty_underlying(self) -> int:
        if self.cfg.nifty_scrip_code:
            return int(self.cfg.nifty_scrip_code)
        symbol = self.cfg.nifty_symbol.upper().strip()
        candidates = []
        for raw in await self.scrip_master():
            r = self._norm_scrip(raw)
            if r["exch"] != "N" or r["exch_type"] != self.cfg.nifty_exch_type.upper():
                continue
            if r["root"] not in {symbol, "NIFTY 50" if symbol == "NIFTY" else symbol}:
                continue
            if r["scrip_code"] <= 0:
                continue
            # Prefer non-derivative/zero-strike index rows.
            penalty = 0
            if abs(r["strike"]) > 1e-9:
                penalty += 10
            if r["scrip_type"] in {"CE", "PE"}:
                penalty += 20
            candidates.append((penalty, r["scrip_code"]))
        if not candidates:
            raise FivePaisaError(
                "NIFTY_UNDERLYING_NOT_FOUND; set HYDRA_5PAISA_NIFTY_SCRIP_CODE manually"
            )
        return min(candidates)[1]

    async def resolve_option_candidates(
        self,
        *,
        spot: float,
        side: str,
        asof: date,
    ) -> list[OptionContract]:
        option_type = "CE" if side.upper() == "LONG" else "PE"
        root = self.cfg.nifty_symbol.upper().strip()
        min_expiry = asof + timedelta(days=max(self.cfg.min_dte, 0))
        rows: list[dict] = []
        for raw in await self.scrip_master():
            r = self._norm_scrip(raw)
            if r["exch"] != "N" or r["exch_type"] != "D":
                continue
            if r["root"] not in {root, "NIFTY 50" if root == "NIFTY" else root}:
                continue
            if r["scrip_type"] != option_type or not r["expiry"] or r["scrip_code"] <= 0:
                continue
            if r["expiry"] < asof:
                continue
            rows.append(r)
        if not rows:
            raise FivePaisaError(f"NO_{root}_{option_type}_CONTRACTS_IN_SCRIP_MASTER")

        eligible = [r for r in rows if r["expiry"] >= min_expiry]
        if not eligible:
            eligible = rows
        expiry = min(r["expiry"] for r in eligible)
        same_exp = [r for r in eligible if r["expiry"] == expiry]

        step = float(self.cfg.option_strike_step)
        atm = round(float(spot) / step) * step
        one_itm = atm - step if option_type == "CE" else atm + step
        desired = [atm, one_itm]

        out: list[OptionContract] = []
        used: set[int] = set()
        for target in desired:
            ranked = sorted(
                same_exp,
                key=lambda r: (abs(float(r["strike"]) - target), abs(float(r["strike"]) - spot)),
            )
            if not ranked:
                continue
            r = ranked[0]
            if r["scrip_code"] in used:
                continue
            used.add(r["scrip_code"])
            symbol = r["symbol"] or f"{root}_{expiry.isoformat()}_{option_type}_{r['strike']:g}"
            out.append(
                OptionContract(
                    scrip_code=r["scrip_code"],
                    symbol=symbol,
                    option_type=option_type,
                    strike=float(r["strike"]),
                    expiry=expiry,
                    lot_size=int(r["lot_size"] or 0),
                )
            )
        if not out:
            raise FivePaisaError(f"NO_USABLE_{root}_{option_type}_CONTRACT")
        return out

    async def best_executable_option(
        self,
        *,
        spot: float,
        side: str,
        asof: date,
    ) -> tuple[OptionContract, Quote, list[dict]]:
        candidates = await self.resolve_option_candidates(spot=spot, side=side, asof=asof)
        checked: list[dict] = []
        viable: list[tuple[float, int, OptionContract, Quote]] = []
        for idx, c in enumerate(candidates):
            try:
                q = await self.executable_quote(c)
                debit = q.ask * self.cfg.lot_size
                spread = q.spread_pct()
                checked.append(
                    {
                        "contract": c.dict(),
                        "quote": q.dict(),
                        "debit": debit,
                        "viable": q.bid > 0
                        and q.ask > 0
                        and spread <= self.cfg.max_spread_pct
                        and debit <= self.cfg.max_capital_per_trade,
                    }
                )
                if (
                    q.bid > 0
                    and q.ask > 0
                    and spread <= self.cfg.max_spread_pct
                    and debit <= self.cfg.max_capital_per_trade
                ):
                    # Prefer preferred-spread contracts, then tighter spread, then ATM over 1-ITM.
                    pref_penalty = 0 if spread <= self.cfg.preferred_spread_pct else 1
                    viable.append((pref_penalty * 10 + spread, idx, c, q))
            except Exception as exc:
                checked.append({"contract": c.dict(), "error": str(exc)[:220], "viable": False})
        if not viable:
            raise FivePaisaError(f"NO_EXECUTABLE_OPTION:{checked}")
        _, _, contract, quote_obj = min(viable, key=lambda x: (x[0], x[1]))
        return contract, quote_obj, checked

    async def place_limit_order(
        self,
        *,
        scrip_code: int,
        qty: int,
        price: float,
        side: str,
        remote_order_id: str,
        stop_loss_price: float = 0.0,
    ):
        # This method remains unreachable unless Engine's live gates have passed.
        if (
            self.cfg.mode != "live"
            or not self.cfg.live_enabled
            or self.cfg.live_confirm != "I_UNDERSTAND_REAL_ORDERS"
            or not self.cfg.static_ip_confirmed
        ):
            raise FivePaisaError("LIVE_ORDER_BLOCKED_BY_HYDRA_GATES")
        if price <= 0:
            raise FivePaisaError("HYDRA forbids naked market orders")
        payload = {
            "head": {"key": self.cfg.app_key},
            "body": {
                "Exchange": "N",
                "ExchangeType": "D",
                "ScripCode": str(scrip_code),
                "Price": round(price, 2),
                "StopLossPrice": round(stop_loss_price, 2),
                "OrderType": "Buy" if side.upper() in {"BUY", "B"} else "Sell",
                "Qty": qty,
                "DisQty": 0,
                "IsIntraday": True,
                "iOrderValidity": "0",
                "AHPlaced": "N",
                "RemoteOrderID": remote_order_id,
                "AlgoID": self.cfg.algo_id,
            },
        }
        return await self._post("/VendorsAPI/Service1.svc/V1/PlaceOrderRequest", payload)

    def _redirect_server(self) -> str:
        try:
            payload = self.access_token.split(".")[1]
            payload += "=" * (-len(payload) % 4)
            data = json.loads(base64.urlsafe_b64decode(payload))
            return str(data.get("RedirectServer", "C")).upper()
        except Exception:
            return "C"

    def websocket_url(self):
        host = {
            "A": "aopenfeed.5paisa.com",
            "B": "bopenfeed.5paisa.com",
            "C": "openfeed.5paisa.com",
        }.get(self._redirect_server(), "openfeed.5paisa.com")
        return f"wss://{host}/feeds/api/chat?Value1={self.access_token}|{self.client_code}"

    async def market_stream(self, instruments: list[dict], on_message):
        if not self.access_token or not self.client_code:
            raise FivePaisaError("Access token/client code missing")
        async with websockets.connect(
            self.websocket_url(),
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
            max_size=2**20,
            open_timeout=12,
        ) as ws:
            req = {
                "Method": "MarketFeedV3",
                "Operation": "Subscribe",
                "ClientCode": self.client_code,
                "MarketFeedData": instruments,
            }
            await ws.send(json.dumps(req))
            async for raw in ws:
                try:
                    data = json.loads(raw)
                except Exception:
                    continue
                await on_message(data)
