#!/usr/bin/env python
"""
Engine Signal Strategy -- eight-check momentum gate for index option buying.

Implements the specification in "Engine Signal Strategy -- Research Document &
OpenAlgo Automation Specification" (v1.0, Oct 2025):

  * Two operating modes (Sec 11.2):
      - gate    : engine alerts dropped into the inbox file are gated and executed
      - scanner : own |m5| momentum-burst detector feeds the same gate
  * Eight veto checks C1..C8 (Sec 3) -- one failed check kills the trade.
  * Bracket exits (Sec 5): SL-M -15%, T1 (+8% standalone), T2 +25%,
    SL -> cost after T1 touch, flat by 15:15 IST.
  * Risk brakes (Sec 8): midday kill zone, 2-SL 60-min cooldown,
    -7000 INR day brake, 12-trade cap, EOD flatten.
  * Logging (Sec 11.8): every signal, taken or skipped, with full context.

Every threshold is an environment variable (prefix ESS_) so nothing is buried
in code (Sec 11.7). Defaults below are the spec defaults.

Run inside OpenAlgo's /python strategy host (it injects OPENALGO_API_KEY and
inherits HOST_SERVER / WEBSOCKET_URL from .env), or standalone with those
variables set. Flip the OpenAlgo UI to Analyzer Mode for Phase-1 sandbox
forward validation (Sec 12).
"""

import csv
import json
import os
import sys
import time
import threading
from datetime import datetime, date, timedelta, timezone

import requests

try:
    from openalgo import api as oa_api
except ImportError:
    print("openalgo SDK not installed: pip install openalgo")
    sys.exit(1)

# ----------------------------------------------------------------------------
# Configuration (Spec Sec 11.7) -- every threshold visible, env-overridable
# ----------------------------------------------------------------------------

def _env(key, default):
    return os.getenv(key, default)

def _env_int(key, default):
    try:
        return int(os.getenv(key, default))
    except (TypeError, ValueError):
        return default

def _env_float(key, default):
    try:
        return float(os.getenv(key, default))
    except (TypeError, ValueError):
        return default

def _env_bool(key, default=False):
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")


IST = timezone(timedelta(hours=5, minutes=30))  # Asia/Kolkata, no DST

STRATEGY_NAME = _env("ESS_STRATEGY_NAME", "Engine Signal Strategy")

# Mode: "gate" (engine alerts via inbox), "scanner" (own burst detector), "both"
MODE = _env("ESS_MODE", "both").strip().lower()

# Per-index parameters. Threshold order per spec: NIFTY first / SENSEX second.
INDICES = {
    "NIFTY": {
        "quote_symbol": _env("ESS_NIFTY_SYMBOL", "NIFTY"),
        "quote_exchange": _env("ESS_NIFTY_EXCHANGE", "NSE_INDEX"),
        "underlying": _env("ESS_NIFTY_UNDERLYING", "NIFTY"),
        "trigger": _env_float("ESS_TRIGGER_NIFTY", 20),      # C1: 5-min signed move, pts
        "exhaustion": _env_float("ESS_EXH_NIFTY", 40),       # C2: 30-min signed move, pts
        "flip": _env_float("ESS_FLIP_NIFTY", 30),            # C3 flip burst, pts
        "premium_band": (_env_float("ESS_BAND_LO_NIFTY", 120),
                         _env_float("ESS_BAND_HI_NIFTY", 215)),   # C8, INR
        "lot_default": _env_int("ESS_LOT_NIFTY", 65),
    },
    "SENSEX": {
        "quote_symbol": _env("ESS_SENSEX_SYMBOL", "SENSEX"),
        "quote_exchange": _env("ESS_SENSEX_EXCHANGE", "BSE_INDEX"),
        "underlying": _env("ESS_SENSEX_UNDERLYING", "SENSEX"),
        "trigger": _env_float("ESS_TRIGGER_SENSEX", 60),
        "exhaustion": _env_float("ESS_EXH_SENSEX", 130),
        "flip": _env_float("ESS_FLIP_SENSEX", 90),
        "premium_band": (_env_float("ESS_BAND_LO_SENSEX", 300),
                         _env_float("ESS_BAND_HI_SENSEX", 460)),
        "lot_default": _env_int("ESS_LOT_SENSEX", 20),
    },
}

# Range position (C4)
RNG_MIN = _env_float("ESS_RNG_MIN", 0.15)
RNG_MAX = _env_float("ESS_RNG_MAX", 0.85)

# Midday kill zone (C5), IST
KILL_START = _env("ESS_KILL_START", "12:00")
KILL_END = _env("ESS_KILL_END", "13:00")

# Revenge chain (C6)
COOLDOWN_MIN = _env_int("ESS_COOLDOWN_MIN", 60)          # after 2 consecutive SL
SL_CHAIN_LIMIT = _env_int("ESS_SL_CHAIN_LIMIT", 2)

# Day brake (C7) and trade cap (Sec 8)
DAY_BRAKE = _env_float("ESS_DAY_BRAKE", -7000)           # realised INR
MAX_TRADES = _env_int("ESS_MAX_TRADES", 12)
TRADE_ALERT_AT = _env_int("ESS_TRADE_ALERT_AT", 10)

# Bracket (Sec 5)
SL_PCT = _env_float("ESS_SL_PCT", 15.0)                  # % of premium
T1_PCT = _env_float("ESS_T1_PCT", 8.0)                   # standalone default
T2_PCT = _env_float("ESS_T2_PCT", 25.0)
T1_FRACTION = _env_float("ESS_T1_FRACTION", 0.5)         # booked at T1 when qty allows

# Flip rule: provisional (Sec 13) -- disabled by default
FLIP_ENABLED = _env_bool("ESS_FLIP_ENABLED", False)
FLIP_LIFE_MIN = _env_int("ESS_FLIP_LIFE_MIN", 30)

# Contract selection (Sec 6)
STEP_ITM_IF_BELOW_BAND = _env_bool("ESS_STEP_ITM", True)  # ATM -> ITM1, never OTM

# Session
EOD_FLATTEN = _env("ESS_EOD_FLATTEN", "15:15")           # IST
SESSION_END = _env("ESS_SESSION_END", "15:20")

# Engine-alert inbox (gate mode). Drop JSON alerts here; consumed + cleared.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INBOX_PATH = _env("ESS_INBOX", os.path.join(SCRIPT_DIR, "engine_signal_inbox.json"))
LOG_DIR = _env("ESS_LOG_DIR", os.path.join(SCRIPT_DIR, "logs"))
LOG_CSV = os.path.join(LOG_DIR, "engine_signal_log.csv")

# Loop timings (seconds)
POLL_SEC = _env_int("ESS_POLL_SEC", 5)
FEED_REFRESH_SEC = _env_int("ESS_FEED_REFRESH_SEC", 30)
STALE_SEC = _env_int("ESS_STALE_SEC", 90)                # freeze entries when feed stale
ORDER_TIMEOUT_SEC = _env_int("ESS_ORDER_TIMEOUT_SEC", 30)

# Telegram (optional): OpenAlgo Telegram bot username registered in the UI
TELEGRAM_USER = _env("ESS_TELEGRAM_USER", "")

# Connection (OpenAlgo /python host injects OPENALGO_API_KEY; HOST_SERVER /
# WEBSOCKET_URL come from .env -- see strategies/README.md)
API_KEY = os.getenv("OPENALGO_API_KEY", "")
API_HOST = os.getenv("HOST_SERVER") or os.getenv("OPENALGO_HOST", "http://127.0.0.1:5000")
WS_URL = os.getenv("WEBSOCKET_URL") or (
    f"ws://{os.getenv('WEBSOCKET_HOST', '127.0.0.1')}:{os.getenv('WEBSOCKET_PORT', '8765')}"
)

if not API_KEY:
    print("Error: OPENALGO_API_KEY environment variable not set")
    sys.exit(1)

client = oa_api(api_key=API_KEY, host=API_HOST, ws_url=WS_URL)

os.makedirs(LOG_DIR, exist_ok=True)


def now_ist():
    return datetime.now(IST)


def _hm(s):
    h, m = s.split(":")
    return int(h) * 60 + int(m)


KILL_START_M, KILL_END_M = _hm(KILL_START), _hm(KILL_END)
EOD_M = _hm(EOD_FLATTEN)
SESSION_END_M = _hm(SESSION_END)


# ----------------------------------------------------------------------------
# Raw REST helper (endpoints the SDK does not wrap: optionsymbol)
# ----------------------------------------------------------------------------

def api_post(endpoint, payload, timeout=10):
    url = f"{API_HOST}/api/v1/{endpoint}"
    body = {"apikey": API_KEY}
    body.update(payload)
    r = requests.post(url, json=body, timeout=timeout)
    return r.json()


def notify(message):
    """Console + optional Telegram push for decisions, fills and brakes."""
    print(f"[{now_ist():%H:%M:%S}] {message}", flush=True)
    if TELEGRAM_USER:
        try:
            client.telegram(username=TELEGRAM_USER, message=message)
        except Exception as e:
            print(f"telegram notify failed: {e}")


# ----------------------------------------------------------------------------
# Logger (Spec Sec 11.8) -- CSV mirrors the Excel tracker schema
# ----------------------------------------------------------------------------

CSV_FIELDS = [
    "date", "time", "index", "contract", "side", "entry", "lot", "exit",
    "exit_reason", "pnl", "sm5", "sm30", "rng",
    "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8",
    "verdict", "skip_reason", "mode", "slippage_vs_signal_price",
]


class SignalLogger:
    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        if not os.path.exists(path):
            with open(path, "w", newline="") as f:
                csv.writer(f).writerow(CSV_FIELDS)

    def log(self, **kw):
        row = {k: kw.get(k, "") for k in CSV_FIELDS}
        with self._lock:
            with open(self.path, "a", newline="") as f:
                csv.writer(f).writerow([row[k] for k in CSV_FIELDS])


# ----------------------------------------------------------------------------
# Market data: ticks/quotes -> 5-min candles -> features (Spec components 2-3)
# ----------------------------------------------------------------------------

class IndexFeed:
    """Builds 5-min candles for one index from 1-min history + live quote."""

    def __init__(self, name, cfg):
        self.name = name
        self.cfg = cfg
        self.closes5 = []          # list of (candle_start_minute, close)
        self.day_open = None
        self.day_high = None
        self.day_low = None
        self.last_ltp = None
        self.last_refresh = None   # datetime of last successful data pull
        self.last_candle_key = None
        self.flip_bias = 0         # +1 / -1 while a flip is active
        self.flip_expires = None   # datetime

    # -- data ---------------------------------------------------------------

    def refresh(self):
        """Reload today's 1-min candles and rebuild the 5-min series."""
        today = now_ist().strftime("%Y-%m-%d")
        df = client.history(
            symbol=self.cfg["quote_symbol"],
            exchange=self.cfg["quote_exchange"],
            interval="1m",
            start_date=today,
            end_date=today,
        )
        if df is None or getattr(df, "empty", True):
            return False

        closes5 = []
        bucket = None
        o = h = l = c = None
        day_o = day_h = day_l = None
        for ts, row in df.iterrows():
            try:
                price = float(row["close"])
                hi = float(row.get("high", price))
                lo = float(row.get("low", price))
            except Exception:
                continue
            minute = ts.hour * 60 + ts.minute
            b = minute // 5
            if bucket is None or b != bucket:
                if bucket is not None:
                    closes5.append((bucket, c))
                bucket = b
                c = price
            else:
                c = price
            day_o = price if day_o is None else day_o
            day_h = hi if day_h is None else max(day_h, hi)
            day_l = lo if day_l is None else min(day_l, lo)
        if bucket is not None:
            closes5.append((bucket, c))  # last (possibly partial) bucket

        self.closes5 = closes5
        self.day_open, self.day_high, self.day_low = day_o, day_h, day_l
        self.last_ltp = c
        self.last_refresh = now_ist()
        return True

    def stale(self):
        if not self.last_refresh:
            return True
        return (now_ist() - self.last_refresh).total_seconds() > STALE_SEC

    # -- features (Spec Sec 4) ----------------------------------------------

    def features(self):
        """(m5, m30, rng, trend) from completed context + latest close.

        m5/m30/trend use the last close vs closes 1 / 6 / 12 five-minute
        bars back. rng uses the running day high/low.
        """
        n = len(self.closes5)
        if n < 2:
            return None
        close = self.closes5[-1][1]
        m5 = close - self.closes5[-2][1]
        m30 = close - self.closes5[-7][1] if n >= 7 else None
        m60 = close - self.closes5[-13][1] if n >= 13 else None
        if self.day_high is None or self.day_low is None or self.day_high == self.day_low:
            rng = 0.5
        else:
            rng = (close - self.day_low) / (self.day_high - self.day_low)
        trend = 0
        ref = m60 if m60 is not None else m30
        if ref is not None:
            trend = 1 if ref > 0 else (-1 if ref < 0 else 0)
        return {"close": close, "m5": m5, "m30": m30, "rng": rng, "trend": trend}

    def new_candle_closed(self):
        """True once per newly completed 5-min bucket."""
        if len(self.closes5) < 2:
            return False
        key = self.closes5[-2][0]  # last *completed* bucket
        if key != self.last_candle_key:
            self.last_candle_key = key
            return True
        return False

    # -- provisional flip detection (Spec Sec 13) ----------------------------

    def update_flip(self, feats):
        if not FLIP_ENABLED or feats is None or self.day_open is None:
            return
        n = len(self.closes5)
        if n < 2:
            return
        prev = self.closes5[-2][1]
        cur = feats["close"]
        burst = feats["m5"]
        crossed = (prev - self.day_open) * (cur - self.day_open) < 0
        if crossed and abs(burst) >= self.cfg["flip"]:
            self.flip_bias = 1 if burst > 0 else -1
            self.flip_expires = now_ist() + timedelta(minutes=FLIP_LIFE_MIN)
            notify(f"FLIP {self.name}: bias {'UP' if self.flip_bias > 0 else 'DOWN'} "
                   f"for {FLIP_LIFE_MIN} min (burst {burst:+.1f} across day open)")

    def flip_active(self):
        if self.flip_expires is None:
            return 0
        if now_ist() > self.flip_expires:
            self.flip_expires = None
            return 0
        return self.flip_bias


# ----------------------------------------------------------------------------
# Risk governor (Spec component 7 / Sec 8) -- brakes are software state
# ----------------------------------------------------------------------------

class RiskGovernor:
    def __init__(self):
        self.day = date.today()
        self.realised = 0.0
        self.trades_today = 0
        self.consecutive_sl = 0
        self.cooldown_until = None
        self.done_for_day = False

    def rollover(self):
        today = now_ist().date()
        if today != self.day:
            self.__init__()
            notify("New session: risk governor reset")

    def in_cooldown(self):
        if self.cooldown_until and now_ist() < self.cooldown_until:
            return True
        self.cooldown_until = None
        return False

    def record_result(self, pnl, exit_reason):
        self.realised += pnl
        self.trades_today += 1
        if exit_reason == "SL":
            self.consecutive_sl += 1
            if self.consecutive_sl >= SL_CHAIN_LIMIT:
                self.cooldown_until = now_ist() + timedelta(minutes=COOLDOWN_MIN)
                notify(f"REVENGE BRAKE: {self.consecutive_sl} consecutive SL -> "
                       f"{COOLDOWN_MIN}-min timeout")
        else:
            self.consecutive_sl = 0
        if self.realised <= DAY_BRAKE:
            self.done_for_day = True
            notify(f"DAY BRAKE TRIPPED: realised {self.realised:.0f} <= {DAY_BRAKE:.0f} "
                   f"-- flat and done for the day")
        if self.trades_today >= MAX_TRADES:
            self.done_for_day = True
            notify(f"TRADE CAP reached ({MAX_TRADES}) -- done for the day")
        elif self.trades_today >= TRADE_ALERT_AT:
            notify(f"trade count alert: {self.trades_today}/{MAX_TRADES}")


# ----------------------------------------------------------------------------
# Checklist gate (Spec Sec 3-4) -- a check is a veto, not a score
# ----------------------------------------------------------------------------

def in_kill_zone():
    m = now_ist().hour * 60 + now_ist().minute
    return KILL_START_M <= m < KILL_END_M


def run_gate(name, side, feed, gov, premium_ok):
    """Returns (checks dict C1..C8 -> bool, failed list, feature snapshot)."""
    cfg = feed.cfg
    feats = feed.features()
    if feats is None:
        return None, ["DATA"], None
    sgn = 1 if side == "CE" else -1
    sm5 = feats["m5"] * sgn
    sm30 = (feats["m30"] or 0.0) * sgn

    flip = feed.flip_active()
    c3 = (feats["trend"] == sgn and feats["trend"] != 0) or (flip == sgn and flip != 0)

    checks = {
        "C1": sm5 >= cfg["trigger"],
        "C2": sm30 < cfg["exhaustion"],
        "C3": c3,
        "C4": RNG_MIN < feats["rng"] < RNG_MAX,
        "C5": not in_kill_zone(),
        "C6": gov.consecutive_sl < SL_CHAIN_LIMIT and not gov.in_cooldown(),
        "C7": (not gov.done_for_day) and gov.realised > DAY_BRAKE,
        "C8": premium_ok,
    }
    failed = [k for k, v in checks.items() if not v]
    snap = dict(feats)
    snap["sm5"] = sm5
    snap["sm30"] = sm30
    return checks, failed, snap


# ----------------------------------------------------------------------------
# Contract selection (Spec Sec 6) + C8 premium sanity
# ----------------------------------------------------------------------------

def resolve_option(name, side, offset):
    """Resolve tradingsymbol via /api/v1/optionsymbol (no strike_int ->
    actual strikes from the instrument master, never hardcoded)."""
    cfg = INDICES[name]
    resp = api_post("optionsymbol", {
        "underlying": cfg["underlying"],
        "exchange": cfg["quote_exchange"],
        "offset": offset,
        "option_type": side,
    })
    if resp.get("status") != "success":
        return None
    return resp


def option_ltp(symbol, exchange):
    try:
        q = client.quotes(symbol=symbol, exchange=exchange)
        if q.get("status") == "success":
            return float(q["data"]["ltp"])
    except Exception:
        pass
    return None


def select_contract(name, side):
    """ATM strike; one step ITM when ATM premium is below the band (never OTM).
    Returns (contract dict, premium, premium_ok)."""
    band_lo, band_hi = INDICES[name]["premium_band"]
    for offset in (["ATM", "ITM1"] if STEP_ITM_IF_BELOW_BAND else ["ATM"]):
        c = resolve_option(name, side, offset)
        if not c:
            continue
        premium = option_ltp(c["symbol"], c["exchange"])
        if premium is None:
            continue
        c["premium"] = premium
        if band_lo <= premium <= band_hi:
            return c, premium, True
        if premium < band_lo and offset == "ATM":
            continue  # step one ITM and re-check
        return c, premium, False  # above band, or ITM still below: veto C8
    return None, None, False


# ----------------------------------------------------------------------------
# Execution engine (Spec component 6 / Sec 5)
# ----------------------------------------------------------------------------

def wait_fill(order_id, timeout=ORDER_TIMEOUT_SEC):
    """Poll orderstatus until complete/rejected. Returns (status, avg_price, qty)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = client.orderstatus(order_id=order_id, strategy=STRATEGY_NAME)
            d = r.get("data", {}) if isinstance(r, dict) else {}
            st = str(d.get("order_status", d.get("status", ""))).lower()
            if st in ("complete", "completed", "filled", "traded"):
                price = float(d.get("average_price") or d.get("price") or 0)
                qty = int(float(d.get("filled_quantity") or d.get("quantity") or 0))
                return "complete", price, qty
            if st in ("rejected", "cancelled", "canceled"):
                return st, 0.0, 0
        except Exception as e:
            print(f"orderstatus error: {e}")
        time.sleep(2)
    return "timeout", 0.0, 0


class Trade:
    """One gated entry and its bracket. Spec state machine Sec 11.5."""

    def __init__(self, name, side, contract, entry_price, qty, lot,
                 engine_sl=None, engine_t1=None):
        self.name = name
        self.side = side
        self.symbol = contract["symbol"]
        self.exchange = contract["exchange"]
        self.entry = entry_price
        self.qty = qty
        self.lot = lot
        # SL: -SL_PCT default; engine SL wins when tighter (higher for a long)
        sl = entry_price * (1 - SL_PCT / 100.0)
        if engine_sl:
            sl = max(sl, float(engine_sl))
        self.sl_price = round(sl, 1)
        # T1: engine T1 when given, else +T1_PCT
        self.t1_price = round(float(engine_t1), 1) if engine_t1 else \
            round(entry_price * (1 + T1_PCT / 100.0), 1)
        self.t2_price = round(entry_price * (1 + T2_PCT / 100.0), 1)
        self.t1_qty = 0
        if qty >= 2 * lot:
            self.t1_qty = int(qty * T1_FRACTION) // lot * lot
        self.state = "BRACKET_LIVE"
        self.sl_order = None
        self.t1_order = None
        self.t2_order = None
        self.t1_touched = False
        self.exit_price = None
        self.exit_reason = None
        self.pnl = 0.0

    # -- order placement -----------------------------------------------------

    def place_bracket(self):
        """Resting SL-M always; T1 limit only when quantity splits in lots.
        An unprotected long option is never allowed to exist (Sec 11.6)."""
        try:
            r = client.placeorder(
                strategy=STRATEGY_NAME, symbol=self.symbol, action="SELL",
                exchange=self.exchange, price_type="SL-M", product="MIS",
                quantity=self.qty, trigger_price=str(self.sl_price), price="0",
            )
            if r.get("status") != "success":
                raise RuntimeError(f"SL-M rejected: {r}")
            self.sl_order = r.get("orderid")
        except Exception as e:
            notify(f"SL PLACEMENT FAILED ({e}) -- market exit NOW")
            client.placeorder(
                strategy=STRATEGY_NAME, symbol=self.symbol, action="SELL",
                exchange=self.exchange, price_type="MARKET", product="MIS",
                quantity=self.qty,
            )
            self.exit_reason = "SL_REJECT"
            self.state = "CLOSED"
            return False

        if self.t1_qty > 0:
            try:
                r = client.placeorder(
                    strategy=STRATEGY_NAME, symbol=self.symbol, action="SELL",
                    exchange=self.exchange, price_type="LIMIT", product="MIS",
                    quantity=self.t1_qty, price=str(self.t1_price),
                )
                if r.get("status") == "success":
                    self.t1_order = r.get("orderid")
            except Exception as e:
                print(f"T1 limit placement failed (SL still live): {e}")
        notify(f"BRACKET LIVE {self.symbol} qty {self.qty} entry {self.entry} "
               f"SL {self.sl_price} T1 {self.t1_price} T2 {self.t2_price}")
        return True

    def _modify_sl(self, new_price, qty):
        try:
            client.modifyorder(
                order_id=self.sl_order, strategy=STRATEGY_NAME, symbol=self.symbol,
                action="SELL", exchange=self.exchange, price_type="SL-M",
                product="MIS", quantity=qty, price="0",
                trigger_price=str(round(new_price, 1)),
            )
            self.sl_price = round(new_price, 1)
            notify(f"SL moved to {self.sl_price} on {self.symbol}")
        except Exception as e:
            print(f"SL modify failed: {e}")

    # -- monitoring ----------------------------------------------------------

    def manage(self, gov, logger, signal_ctx):
        """Poll legs, watch T1 touch, enforce EOD. Returns True while open."""
        ltp = option_ltp(self.symbol, self.exchange)

        # T1 touch -> SL to cost (covers the 1-lot case where no T1 limit exists)
        if not self.t1_touched and ltp and ltp >= self.t1_price:
            self.t1_touched = True
            self._modify_sl(self.entry, self.qty)
            notify(f"T1 TOUCHED on {self.symbol} -- SL to cost {self.entry}")

        # T1 limit filled -> cancel SL, re-bracket remainder: SL at cost + T2 limit
        if self.t1_order:
            st, price, _ = wait_fill(self.t1_order, timeout=1)
            if st == "complete":
                remaining = self.qty - self.t1_qty
                booked = (price - self.entry) * self.t1_qty
                self.pnl += booked
                self.t1_order = None
                notify(f"T1 FILLED {self.symbol} +{booked:.0f} -- remainder {remaining}")
                if remaining > 0:
                    if self.sl_order:
                        try:
                            client.cancelorder(order_id=self.sl_order,
                                               strategy=STRATEGY_NAME)
                        except Exception:
                            pass
                    try:
                        r = client.placeorder(
                            strategy=STRATEGY_NAME, symbol=self.symbol, action="SELL",
                            exchange=self.exchange, price_type="SL-M", product="MIS",
                            quantity=remaining, trigger_price=str(self.entry), price="0",
                        )
                        self.sl_order = r.get("orderid") if r.get("status") == "success" else None
                        self.sl_price = self.entry
                    except Exception as e:
                        notify(f"re-bracket SL failed ({e}) -- market exit remainder")
                        client.placeorder(
                            strategy=STRATEGY_NAME, symbol=self.symbol, action="SELL",
                            exchange=self.exchange, price_type="MARKET", product="MIS",
                            quantity=remaining)
                        return self._close(gov, logger, signal_ctx, "T1", None)
                    try:
                        r = client.placeorder(
                            strategy=STRATEGY_NAME, symbol=self.symbol, action="SELL",
                            exchange=self.exchange, price_type="LIMIT", product="MIS",
                            quantity=remaining, price=str(self.t2_price),
                        )
                        self.t2_order = r.get("orderid") if r.get("status") == "success" else None
                    except Exception as e:
                        print(f"T2 limit failed: {e}")

        # SL leg
        if self.sl_order:
            st, price, _ = wait_fill(self.sl_order, timeout=1)
            if st == "complete":
                if self.t2_order:
                    try:
                        client.cancelorder(order_id=self.t2_order, strategy=STRATEGY_NAME)
                    except Exception:
                        pass
                reason = "SL" if not self.t1_touched and price < self.entry else "TSL"
                return self._close(gov, logger, signal_ctx, reason, price)

        # T2 leg
        if self.t2_order:
            st, price, _ = wait_fill(self.t2_order, timeout=1)
            if st == "complete":
                if self.sl_order:
                    try:
                        client.cancelorder(order_id=self.sl_order, strategy=STRATEGY_NAME)
                    except Exception:
                        pass
                return self._close(gov, logger, signal_ctx, "T2", price)

        # EOD flatten (Spec: flat by 15:15, no overnight option buys)
        m = now_ist().hour * 60 + now_ist().minute
        if m >= EOD_M:
            self.flatten()
            return self._close(gov, logger, signal_ctx, "EOD", ltp)

        return True

    def flatten(self):
        try:
            client.cancelallorder(strategy=STRATEGY_NAME)
        except Exception:
            pass
        try:
            client.closeposition(strategy=STRATEGY_NAME)
        except Exception as e:
            notify(f"EOD flatten error: {e}")

    def _close(self, gov, logger, signal_ctx, reason, price):
        if price is None:
            price = option_ltp(self.symbol, self.exchange) or self.entry
        self.exit_price = price
        self.exit_reason = reason
        # pnl: partial T1 already booked in self.pnl; add remainder leg
        legs_qty = self.qty - (self.t1_qty if self.pnl else 0)
        self.pnl += (price - self.entry) * legs_qty
        self.state = "CLOSED"
        gov.record_result(self.pnl, "SL" if reason in ("SL", "SL_REJECT") else reason)
        logger.log(
            date=now_ist().strftime("%Y-%m-%d"), time=now_ist().strftime("%H:%M:%S"),
            index=self.name, contract=self.symbol, side=self.side,
            entry=self.entry, lot=self.lot, exit=price, exit_reason=reason,
            pnl=round(self.pnl, 2),
            sm5=round(signal_ctx.get("sm5", 0), 2), sm30=round(signal_ctx.get("sm30", 0), 2),
            rng=round(signal_ctx.get("rng", 0), 3),
            **{k: "Y" for k in ("C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8")},
            verdict="CLOSE", mode=MODE,
            slippage_vs_signal_price=signal_ctx.get("slippage", ""),
        )
        notify(f"CLOSED {self.symbol} reason={reason} exit={price} pnl={self.pnl:+.0f} "
               f"day={gov.realised:+.0f}")
        return False


# ----------------------------------------------------------------------------
# Signal intake (Spec component 1): engine-alert inbox + own burst scanner
# ----------------------------------------------------------------------------

_seen_signals = {}


def dedupe(name, side):
    """Dedupe key = index + direction + 5-minute bucket (Spec Sec 11.6)."""
    n = now_ist()
    key = (name, side, n.hour * 60 + n.minute // 5)
    if key in _seen_signals:
        return True
    _seen_signals[key] = True
    if len(_seen_signals) > 500:
        _seen_signals.clear()
    return False


def drain_inbox():
    """Gate mode: engine alerts as a JSON list in the inbox file. Each alert:
    {"index": "NIFTY"|"SENSEX", "side": "CE"|"PE",
     "engine_sl": <premium, optional>, "engine_t1": <premium, optional>}
    Consumed alerts are removed."""
    if not os.path.exists(INBOX_PATH):
        return []
    try:
        with open(INBOX_PATH) as f:
            alerts = json.load(f)
        with open(INBOX_PATH, "w") as f:
            json.dump([], f)
        return alerts if isinstance(alerts, list) else []
    except Exception as e:
        print(f"inbox read error: {e}")
        return []


# ----------------------------------------------------------------------------
# Main loop
# ----------------------------------------------------------------------------

def attempt_signal(name, side, feeds, gov, logger, engine_sl=None, engine_t1=None):
    """Full pipeline for one candidate signal (Spec Sec 4 + 11.5)."""
    if gov.done_for_day:
        return
    feed = feeds[name]
    if feed.stale():
        notify(f"SKIP {name} {side}: feed stale > {STALE_SEC}s (entries frozen)")
        return
    if dedupe(name, side):
        return

    # C8 first: contract + premium sanity (needs a live quote on the strike)
    contract, premium, premium_ok = select_contract(name, side)
    checks, failed, snap = run_gate(name, side, feed, gov, premium_ok)
    if checks is None:
        notify(f"SKIP {name} {side}: no feature data yet")
        return

    verdict = "TAKE" if not failed else "SKIP"
    logger.log(
        date=now_ist().strftime("%Y-%m-%d"), time=now_ist().strftime("%H:%M:%S"),
        index=name, contract=(contract or {}).get("symbol", ""), side=side,
        entry=premium or "", lot=(contract or {}).get("lotsize", ""),
        sm5=round(snap["sm5"], 2), sm30=round(snap["sm30"], 2), rng=round(snap["rng"], 3),
        **{k: ("Y" if v else "N") for k, v in checks.items()},
        verdict=verdict, skip_reason=",".join(failed), mode=MODE,
    )

    if failed:
        notify(f"SKIP {name} {side} failed={','.join(failed)} "
               f"sm5={snap['sm5']:+.1f} sm30={snap['sm30']:+.1f} rng={snap['rng']:.2f}")
        return

    # --- execute: BUY 1 lot MARKET MIS (Spec Sec 6) ---
    lot = int(contract.get("lotsize") or INDICES[name]["lot_default"])
    qty = lot
    notify(f"TAKE {name} {side} {contract['symbol']} @~{premium} "
           f"sm5={snap['sm5']:+.1f} rng={snap['rng']:.2f}")
    try:
        r = client.placeorder(
            strategy=STRATEGY_NAME, symbol=contract["symbol"], action="BUY",
            exchange=contract["exchange"], price_type="MARKET", product="MIS",
            quantity=qty,
        )
        if r.get("status") != "success":
            notify(f"ENTRY REJECTED {contract['symbol']}: {r}")
            return
        order_id = r.get("orderid")
    except Exception as e:
        notify(f"ENTRY ERROR {contract['symbol']}: {e}")
        return

    st, fill_price, fill_qty = wait_fill(order_id)
    if st != "complete" or fill_qty <= 0:
        notify(f"ENTRY NOT FILLED ({st}) {contract['symbol']} -- cancelling remainder")
        try:
            client.cancelorder(order_id=order_id, strategy=STRATEGY_NAME)
        except Exception:
            pass
        return
    if fill_price <= 0:
        fill_price = premium or 0.0
    if fill_qty != qty:  # partial fill: bracket the filled quantity (Sec 11.6)
        notify(f"partial fill {fill_qty}/{qty} -- bracket on filled qty")
        qty = fill_qty

    trade = Trade(name, side, contract, fill_price, qty, lot,
                  engine_sl=engine_sl, engine_t1=engine_t1)
    signal_ctx = dict(snap)
    signal_ctx["slippage"] = round(fill_price - premium, 2) if premium else ""
    if trade.place_bracket():
        return trade, signal_ctx
    return None


def main():
    print(f"Starting {STRATEGY_NAME} | mode={MODE} host={API_HOST}")
    print(f"kill zone {KILL_START}-{KILL_END} IST | day brake {DAY_BRAKE} | "
          f"max trades {MAX_TRADES} | EOD {EOD_FLATTEN}")
    print(f"flip rule {'ENABLED' if FLIP_ENABLED else 'disabled (provisional)'}")
    notify(f"{STRATEGY_NAME} armed ({MODE} mode)")

    feeds = {name: IndexFeed(name, cfg) for name, cfg in INDICES.items()}
    gov = RiskGovernor()
    logger = SignalLogger(LOG_CSV)
    open_trade = None
    open_ctx = None
    last_refresh = 0.0

    # Warm-up: load today's candles before gating anything
    for name, feed in feeds.items():
        try:
            ok = feed.refresh()
            print(f"warm-up {name}: {'ok' if ok else 'no data yet'}")
        except Exception as e:
            print(f"warm-up {name} failed: {e}")

    while True:
        try:
            gov.rollover()
            now_m = now_ist().hour * 60 + now_ist().minute

            # Session over -> idle until tomorrow
            if now_m >= SESSION_END_M:
                if open_trade:
                    open_trade.flatten()
                    open_trade = None
                time.sleep(60)
                continue

            # Refresh index feeds
            if time.time() - last_refresh >= FEED_REFRESH_SEC:
                for feed in feeds.values():
                    try:
                        feed.refresh()
                    except Exception as e:
                        print(f"feed refresh error: {e}")
                last_refresh = time.time()

            # Manage open bracket
            if open_trade:
                alive = open_trade.manage(gov, logger, open_ctx)
                if not alive:
                    open_trade = None
                    open_ctx = None
                time.sleep(POLL_SEC)
                continue

            # No open position: look for signals
            if not gov.done_for_day and not gov.in_cooldown():

                # Mode A: engine alerts (gate mode)
                if MODE in ("gate", "both"):
                    for alert in drain_inbox():
                        name = str(alert.get("index", "")).upper()
                        side = str(alert.get("side", "")).upper()
                        if name not in feeds or side not in ("CE", "PE"):
                            print(f"bad alert ignored: {alert}")
                            continue
                        res = attempt_signal(
                            name, side, feeds, gov, logger,
                            engine_sl=alert.get("engine_sl"),
                            engine_t1=alert.get("engine_t1"),
                        )
                        if res:
                            open_trade, open_ctx = res
                            break

                # Mode B: own momentum scanner on new 5-min candle close
                if open_trade is None and MODE in ("scanner", "both"):
                    for name, feed in feeds.items():
                        try:
                            if not feed.new_candle_closed():
                                continue
                            feats = feed.features()
                            if feats is None:
                                continue
                            feed.update_flip(feats)
                            if abs(feats["m5"]) >= feed.cfg["trigger"]:
                                side = "CE" if feats["m5"] > 0 else "PE"
                                res = attempt_signal(name, side, feeds, gov, logger)
                                if res:
                                    open_trade, open_ctx = res
                                    break
                        except Exception as e:
                            print(f"scanner error {name}: {e}")

            time.sleep(POLL_SEC)

        except KeyboardInterrupt:
            print("Strategy stopped by user")
            if open_trade:
                print("NOTE: open position -- bracket orders remain live at broker")
            break
        except Exception as e:
            print(f"main loop error: {e}")
            time.sleep(POLL_SEC)


if __name__ == "__main__":
    main()
