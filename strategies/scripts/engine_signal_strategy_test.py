#!/usr/bin/env python
"""
Offline test harness for engine_signal_strategy.py.

Mocks the OpenAlgo SDK and the /api/v1/optionsymbol REST call, feeds
synthetic 1-min candles into the real IndexFeed/gate/bracket code, and
asserts the spec's behaviour (v1.0, Oct 2025 + v1.1 expiry-aware C8, Oct
2026): the eight checks veto individually, a full-pass signal produces the
right orders, brackets exit correctly, the brakes trip, and the C8 premium
band scales its lower bound on the index's weekly expiry day.

No broker, no network, no real time needed. Run:

    python3 engine_signal_strategy_test.py
"""

import csv
import os
import sys
import types
from datetime import datetime, timedelta, timezone

# --- environment before importing the strategy (it reads env at import) -----
os.environ["OPENALGO_API_KEY"] = "test-key"
os.environ["ESS_LOG_DIR"] = "/tmp/ess_test_logs"
os.environ["ESS_TELEGRAM_USER"] = ""

import pandas as pd  # noqa: E402

IST = timezone(timedelta(hours=5, minutes=30))
DAY = datetime(2025, 10, 16)  # a Thursday: SENSEX weekly expiry day


# --- fake OpenAlgo client ----------------------------------------------------
class FakeClient:
    """Records every call; MARKET BUY auto-fills at self.entry_fill, all
    exit legs stay 'open' until the test fills them via fill_order()."""

    def __init__(self):
        self.calls = []            # list of (method, kwargs)
        self.history_df = None
        self.option_premiums = {}  # symbol -> ltp
        self.order_states = {}     # orderid -> data dict
        self.entry_fill = 150.0
        self.seq = 0

    def _record(self, method, kw):
        self.calls.append((method, kw))

    def history(self, **kw):
        return self.history_df

    def quotes(self, *, symbol, exchange, **kw):
        ltp = self.option_premiums.get(symbol)
        if ltp is None:
            return {"status": "error", "message": "no quote"}
        return {"status": "success", "data": {"ltp": ltp}}

    def placeorder(self, **kw):
        self._record("placeorder", kw)
        self.seq += 1
        oid = f"ORD{self.seq}"
        if kw.get("price_type") == "MARKET" and kw.get("action") == "BUY":
            self.order_states[oid] = {
                "order_status": "complete",
                "average_price": self.entry_fill,
                "quantity": kw["quantity"],
            }
        else:
            self.order_states[oid] = {"order_status": "open",
                                      "quantity": kw["quantity"]}
        return {"status": "success", "orderid": oid}

    def orderstatus(self, *, order_id, strategy="x", **kw):
        d = self.order_states.get(order_id, {"order_status": "open"})
        return {"status": "success", "data": d}

    def modifyorder(self, **kw):
        self._record("modifyorder", kw)
        oid = kw["order_id"]
        if oid in self.order_states:
            self.order_states[oid]["trigger_price"] = kw.get("trigger_price")
        return {"status": "success", "orderid": oid}

    def cancelorder(self, **kw):
        self._record("cancelorder", kw)
        if kw["order_id"] in self.order_states:
            self.order_states[kw["order_id"]]["order_status"] = "cancelled"
        return {"status": "success"}

    def cancelallorder(self, **kw):
        self._record("cancelallorder", kw)
        return {"status": "success"}

    def closeposition(self, **kw):
        self._record("closeposition", kw)
        return {"status": "success"}

    def telegram(self, **kw):
        self._record("telegram", kw)
        return {"status": "success"}

    def fill_order(self, oid, price):
        self.order_states[oid] = {"order_status": "complete",
                                  "average_price": price,
                                  "quantity": self.order_states[oid]["quantity"]}

    def calls_of(self, method):
        return [kw for m, kw in self.calls if m == method]


# --- install fake SDK before importing the strategy --------------------------
_fake = FakeClient()
_mod = types.ModuleType("openalgo")
_mod.api = lambda **kw: _fake
sys.modules["openalgo"] = _mod

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import engine_signal_strategy as ess  # noqa: E402


def fake_api_post(endpoint, payload, timeout=10):
    if endpoint == "optionsymbol":
        sym = f"{payload['underlying']}_{payload['offset']}_{payload['option_type']}"
        return {"status": "success", "symbol": sym, "exchange": "NFO",
                "lotsize": 65, "underlying_ltp": 24070.0}
    return {"status": "error"}


ess.api_post = fake_api_post

# --- time control -------------------------------------------------------------
_T = datetime(2025, 10, 16, 10, 7, tzinfo=IST)


def set_time(h, m):
    global _T
    _T = datetime(2025, 10, 16, h, m, tzinfo=IST)
    ess.now_ist = lambda: _T


set_time(10, 7)

# --- candle builders ----------------------------------------------------------


def make_df(closes5, spike=None):
    """1-min DataFrame whose 5-min buckets close at the given values.
    spike=(bucket_idx, minute_in_bucket, high) injects one high print."""
    base = DAY.replace(hour=9, minute=15)
    idx, rows = [], []
    for i, c in enumerate(closes5):
        for m in range(5):
            t = base + timedelta(minutes=5 * i + m)
            h, l = c + 2, c - 2
            if spike and i == spike[0] and m == spike[1]:
                h = spike[2]
            idx.append(t)
            rows.append((c, h, l))
    return pd.DataFrame(
        {"close": [r[0] for r in rows], "high": [r[1] for r in rows],
         "low": [r[2] for r in rows]},
        index=pd.DatetimeIndex(idx))


def burst_closes(total_buckets, burst_idx, burst_pts=30.0, dip=None):
    """Rise, go flat, then burst on candle `burst_idx` (the last completed
    candle in the scenario). dip=(start,end,level) lowers a mid stretch to
    manufacture an exhausted 30-min move."""
    closes = [24000.0, 24020.0, 24038.0]
    closes += [24045.0] * max(0, burst_idx - 3)
    closes = closes[:burst_idx]
    closes.append(24045.0 + burst_pts)
    closes += [24045.0 + burst_pts] * (total_buckets - burst_idx - 1)
    if dip:
        s, e, lvl = dip
        for i in range(s, min(e, burst_idx)):
            closes[i] = lvl
        for i in range(min(e, burst_idx), burst_idx):
            if closes[i] < 24045.0:
                closes[i] = 24045.0
    return closes


# --- shared fixtures ------------------------------------------------------------


def fresh(name="t", closes5=None, spike=(0, 0, 24150.0)):
    """Fresh feed/gov/logger; loads candles and returns them."""
    ess._seen_signals.clear()
    feed = ess.IndexFeed("NIFTY", ess.INDICES["NIFTY"])
    if closes5 is not None:
        _fake.history_df = make_df(closes5, spike=spike)
        assert feed.refresh(), "feed refresh failed"
    gov = ess.RiskGovernor()
    path = f"/tmp/ess_test_logs/{name}.csv"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        os.remove(path)
    logger = ess.SignalLogger(path)
    return feed, gov, logger


def load_feed(feed, closes5, spike=(0, 0, 24150.0)):
    _fake.history_df = make_df(closes5, spike=spike)
    assert feed.refresh()


def log_rows(logger):
    with open(logger.path) as f:
        return list(csv.DictReader(f))


def take_trade(feed, gov, logger, side="CE", premium=150.0, **kw):
    """Run one signal through the real pipeline; asserts it is TAKEN."""
    _fake.calls.clear()
    _fake.option_premiums = {"NIFTY_ATM_CE": premium, "NIFTY_ITM1_CE": premium,
                             "NIFTY_ATM_PE": premium, "NIFTY_ITM1_PE": premium}
    _fake.entry_fill = premium
    res = ess.attempt_signal("NIFTY", side, {"NIFTY": feed}, gov, logger, **kw)
    return res


# --- assertion helpers ----------------------------------------------------------
PASSED = []
FAILED = []


def check(name, cond, detail=""):
    if cond:
        PASSED.append(name)
        print(f"  PASS {name}")
    else:
        FAILED.append(name)
        print(f"  FAIL {name} {detail}")


# ================================================================================
# Tests
# ================================================================================

def t1_take_all_pass():
    print("T1: all checks pass -> TAKE with correct orders")
    set_time(10, 7)
    closes = burst_closes(total_buckets=11, burst_idx=9, burst_pts=25.0)
    feed, gov, logger = fresh("t1", closes)
    res = take_trade(feed, gov, logger)
    check("trade opened", res is not None)
    if not res:
        return
    trade, ctx = res
    buys = [c for c in _fake.calls_of("placeorder") if c["action"] == "BUY"]
    sls = [c for c in _fake.calls_of("placeorder")
           if c["action"] == "SELL" and c["price_type"] == "SL-M"]
    limits = [c for c in _fake.calls_of("placeorder") if c["price_type"] == "LIMIT"]
    check("1-lot MARKET MIS buy", len(buys) == 1 and buys[0]["quantity"] == 65
          and buys[0]["price_type"] == "MARKET" and buys[0]["product"] == "MIS")
    check("resting SL-M at -15%", len(sls) == 1
          and float(sls[0]["trigger_price"]) == 127.5, f"got {sls}")
    check("no T1 limit at 1 lot", len(limits) == 0)
    check("bracket levels", trade.sl_price == 127.5 and trade.t1_price == 162.0
          and trade.t2_price == 187.5)
    rows = log_rows(logger)
    check("TAKE logged with context", len(rows) == 1 and rows[0]["verdict"] == "TAKE"
          and float(rows[0]["sm5"]) == 25.0 and rows[0]["C1"] == "Y"
          and rows[0]["C8"] == "Y", f"got {rows}")
    check("feature math", abs(ctx["m5"] - 25.0) < 1e-9
          and abs(ctx["m30"] - 25.0) < 1e-9 and 0.15 < ctx["rng"] < 0.85)


def t2_c1_weak_trigger():
    print("T2: weak trigger -> SKIP C1")
    set_time(10, 7)
    closes = burst_closes(11, 9, burst_pts=10.0)
    feed, gov, logger = fresh("t2", closes)
    _fake.calls.clear()
    _fake.option_premiums = {"NIFTY_ATM_CE": 150.0}
    res = ess.attempt_signal("NIFTY", "CE", {"NIFTY": feed}, gov, logger)
    rows = log_rows(logger)
    check("skipped", res is None and not _fake.calls_of("placeorder"))
    check("C1 in skip_reason", rows and rows[0]["verdict"] == "SKIP"
          and "C1" in rows[0]["skip_reason"], f"got {rows}")


def t3_c2_exhaustion():
    print("T3: exhausted 30-min move -> SKIP C2")
    set_time(10, 7)
    closes = burst_closes(11, 9, burst_pts=30.0, dip=(3, 6, 24000.0))
    # m30 = 24075 - 24000 = 75 >= 40 -> exhausted
    feed, gov, logger = fresh("t3", closes)
    res = take_trade(feed, gov, logger)
    rows = log_rows(logger)
    check("skipped", res is None and not _fake.calls_of("placeorder"))
    check("C2 in skip_reason", rows and "C2" in rows[0]["skip_reason"],
          f"got {rows}")


def t4_c4_range_extreme():
    print("T4: buying the day's extreme -> SKIP C4")
    set_time(10, 7)
    closes = burst_closes(11, 9, burst_pts=25.0)
    feed, gov, logger = fresh("t4", closes, spike=None)  # no spike: close is day high
    res = take_trade(feed, gov, logger)
    rows = log_rows(logger)
    check("skipped", res is None)
    check("C4 in skip_reason", rows and "C4" in rows[0]["skip_reason"],
          f"got {rows}")


def t5_c5_kill_zone():
    print("T5: 12:00-13:00 kill zone -> SKIP C5")
    set_time(12, 37)
    # buckets 09:15..12:35; last completed = 12:30 bucket (index 43)
    closes = burst_closes(45, 43, burst_pts=25.0)
    feed, gov, logger = fresh("t5", closes)
    res = take_trade(feed, gov, logger)
    rows = log_rows(logger)
    check("skipped", res is None)
    check("only C5 fails", rows and rows[0]["skip_reason"] == "C5",
          f"got {rows[0]['skip_reason'] if rows else None}")


def t6_c6_cooldown_and_c7_daybrake():
    print("T6: revenge cooldown and day brake -> SKIP C6 / C7")
    set_time(10, 7)
    closes = burst_closes(11, 9, burst_pts=25.0)
    feed, gov, logger = fresh("t6", closes)
    gov.consecutive_sl = 2
    gov.cooldown_until = _T + timedelta(minutes=30)
    res = take_trade(feed, gov, logger)
    rows = log_rows(logger)
    check("C6 veto in cooldown", res is None and rows
          and "C6" in rows[0]["skip_reason"], f"got {rows}")

    ess._seen_signals.clear()
    feed2, gov2, logger2 = fresh("t6b", closes)
    gov2.realised = -7100.0
    res2 = take_trade(feed2, gov2, logger2)
    rows2 = log_rows(logger2)
    check("C7 veto past day brake", res2 is None and rows2
          and "C7" in rows2[0]["skip_reason"], f"got {rows2}")


def t7_c8_premium_band():
    print("T7: premium sanity band -> SKIP C8 / step ITM")
    set_time(10, 7)
    closes = burst_closes(11, 9, burst_pts=25.0)
    feed, gov, logger = fresh("t7", closes)
    res = take_trade(feed, gov, logger, premium=250.0)  # above 215 band
    rows = log_rows(logger)
    check("C8 veto above band", res is None and rows
          and "C8" in rows[0]["skip_reason"], f"got {rows}")

    ess._seen_signals.clear()
    feed2, gov2, logger2 = fresh("t7b", closes)
    # ATM below band (100) -> must step one ITM where premium is 150
    _fake.calls.clear()
    _fake.option_premiums = {"NIFTY_ATM_CE": 100.0, "NIFTY_ITM1_CE": 150.0}
    _fake.entry_fill = 150.0
    res2 = ess.attempt_signal("NIFTY", "CE", {"NIFTY": feed2}, gov2, logger2)
    buys = [c for c in _fake.calls_of("placeorder") if c["action"] == "BUY"]
    check("stepped ITM and took", res2 is not None and buys
          and buys[0]["symbol"] == "NIFTY_ITM1_CE", f"got {buys}")


def t8_bracket_sl_flow():
    print("T8: SL fill -> close, loss booked, chain counter")
    set_time(10, 7)
    closes = burst_closes(11, 9, burst_pts=25.0)
    feed, gov, logger = fresh("t8", closes)
    trade, ctx = take_trade(feed, gov, logger)
    _fake.fill_order(trade.sl_order, 127.5)
    alive = trade.manage(gov, logger, ctx)
    check("trade closed", alive is False and trade.exit_reason == "SL")
    check("pnl = -1462.5", abs(trade.pnl - (-1462.5)) < 1e-6, f"got {trade.pnl}")
    check("chain counter = 1", gov.consecutive_sl == 1)
    check("day realised", abs(gov.realised - (-1462.5)) < 1e-6)


def t9_t1_touch_sl_to_cost():
    print("T9: T1 touch moves SL to entry (1-lot rule)")
    set_time(10, 7)
    closes = burst_closes(11, 9, burst_pts=25.0)
    feed, gov, logger = fresh("t9", closes)
    trade, ctx = take_trade(feed, gov, logger)
    _fake.option_premiums["NIFTY_ATM_CE"] = 163.0  # >= T1 162
    trade.manage(gov, logger, ctx)
    mods = _fake.calls_of("modifyorder")
    check("SL modified to entry", len(mods) == 1
          and float(mods[0]["trigger_price"]) == 150.0, f"got {mods}")
    check("t1 flagged", trade.t1_touched is True)
    # trailing SL then fills at cost -> scratch trade
    _fake.fill_order(trade.sl_order, 150.0)
    alive = trade.manage(gov, logger, ctx)
    check("closed as TSL scratch", alive is False
          and trade.exit_reason == "TSL" and abs(trade.pnl) < 1e-6)
    check("chain reset after non-SL", gov.consecutive_sl == 0)


def t10_two_sl_cooldown():
    print("T10: two consecutive SL -> 60-min timeout (governor)")
    set_time(10, 7)
    closes = burst_closes(11, 9, burst_pts=25.0)
    feed, gov, logger = fresh("t10", closes)
    # trade 1
    trade1, ctx1 = take_trade(feed, gov, logger)
    _fake.fill_order(trade1.sl_order, 127.5)
    trade1.manage(gov, logger, ctx1)
    check("no cooldown after 1 SL", not gov.in_cooldown())
    # trade 2 (clear dedupe to simulate a later signal)
    ess._seen_signals.clear()
    trade2, ctx2 = take_trade(feed, gov, logger)
    _fake.fill_order(trade2.sl_order, 127.5)
    trade2.manage(gov, logger, ctx2)
    check("cooldown after 2 SL", gov.in_cooldown()
          and gov.consecutive_sl == 2)


def t11_eod_flatten():
    print("T11: 15:15 EOD flatten")
    closes = burst_closes(11, 9, burst_pts=25.0)
    set_time(10, 7)
    feed, gov, logger = fresh("t11", closes)
    trade, ctx = take_trade(feed, gov, logger)
    set_time(15, 16)
    alive = trade.manage(gov, logger, ctx)
    check("closed at EOD", alive is False and trade.exit_reason == "EOD")
    check("flatten called", len(_fake.calls_of("cancelallorder")) == 1
          and len(_fake.calls_of("closeposition")) == 1)


def t12_dedupe_and_stale():
    print("T12: dedupe (index+side+5-min bucket) and stale-feed freeze")
    set_time(10, 7)
    closes = burst_closes(11, 9, burst_pts=25.0)
    feed, gov, logger = fresh("t12", closes)
    res1 = take_trade(feed, gov, logger)
    n_orders = len(_fake.calls_of("placeorder"))
    # call the pipeline directly (take_trade clears the call log)
    _fake.option_premiums = {"NIFTY_ATM_CE": 150.0}
    res2 = ess.attempt_signal("NIFTY", "CE", {"NIFTY": feed}, gov, logger)
    n_after_dupe = len(_fake.calls_of("placeorder"))
    check("first taken, second deduped", res1 is not None and res2 is None
          and n_after_dupe == n_orders, f"{n_orders} -> {n_after_dupe}")

    ess._seen_signals.clear()
    feed.last_refresh = _T - timedelta(seconds=120)  # stale
    res3 = ess.attempt_signal("NIFTY", "CE", {"NIFTY": feed}, gov, logger)
    check("stale feed freezes entries", res3 is None
          and len(_fake.calls_of("placeorder")) == n_orders)


def t13_scanner_new_candle():
    print("T13: scanner fires once per newly completed candle")
    closes = burst_closes(11, 9, burst_pts=25.0)  # through 10:05 partial
    set_time(10, 7)
    feed, gov, logger = fresh("t13", closes)
    check("fires on first completed candle", feed.new_candle_closed() is True)
    check("does not refire on same candle", feed.new_candle_closed() is False)
    # extend by two candles: 24080 completes at 10:10 close; 10:15 still forming
    set_time(10, 17)
    closes2 = closes + [24080.0, 24082.0]
    load_feed(feed, closes2)
    check("fires on next candle", feed.new_candle_closed() is True)
    feats = feed.features()
    check("m5 from completed candles", abs(feats["m5"] - 10.0) < 1e-9,
          f"got {feats['m5']}")


def t14_flip_rule():
    print("T14: provisional flip rule (disabled vs enabled)")
    # open 24000, dip below, last completed candle (idx 9) bursts 36 pts
    # closing back across the day open; idx 10 is the still-forming candle
    closes = [24000.0, 23990.0, 24050.0, 24060.0, 24030.0, 23990.0,
              23980.0, 23975.0, 23972.0, 24008.0, 24008.0]
    # trend (m30) is DOWN; without flip a CE must fail C3
    set_time(10, 7)
    ess.FLIP_ENABLED = False
    feed, gov, logger = fresh("t14", closes)
    res = take_trade(feed, gov, logger)
    rows = log_rows(logger)
    check("C3 veto without flip", res is None and rows
          and "C3" in rows[0]["skip_reason"], f"got {rows}")

    ess.FLIP_ENABLED = True
    ess._seen_signals.clear()
    feed2, gov2, logger2 = fresh("t14b", closes)
    feats = feed2.features()
    feed2.update_flip(feats)
    check("flip armed UP", feed2.flip_active() == 1)
    res2 = take_trade(feed2, gov2, logger2)
    check("CE taken on flip", res2 is not None)
    ess.FLIP_ENABLED = False


def t15_c8_expiry_day_band():
    print("T15: expiry-aware C8 band (v1.1) -- SENSEX Thursday expiry")
    # Test clock sits on Thursday 2025-10-16 == SENSEX weekly expiry weekday.
    set_time(10, 7)
    n_lo, n_hi = ess.premium_band("NIFTY")
    check("NIFTY band unchanged (its expiry is Tuesday)",
          (n_lo, n_hi) == tuple(ess.INDICES["NIFTY"]["premium_band"]),
          f"got {(n_lo, n_hi)}")
    s_lo, s_hi = ess.premium_band("SENSEX")
    check("SENSEX lower bound scaled on expiry day",
          abs(s_lo - 300.0 * ess.BAND_EXPIRY_SCALE) < 1e-9 and s_hi == 460.0,
          f"got {(s_lo, s_hi)}")
    # Rs 200 premium: below the normal 300 lower bound, inside the expiry band
    _fake.option_premiums = {"SENSEX_ATM_CE": 200.0, "SENSEX_ITM1_CE": 200.0}
    c, prem, ok = ess.select_contract("SENSEX", "CE")
    check("expiry-day Rs 200 premium passes C8",
          ok is True and prem == 200.0 and c["symbol"] == "SENSEX_ATM_CE",
          f"got {c} {prem} {ok}")
    # Same premium on a Wednesday (non-expiry) must still veto C8
    ess.now_ist = lambda: datetime(2025, 10, 15, 10, 7, tzinfo=IST)
    s_lo2, _ = ess.premium_band("SENSEX")
    check("mid-week band unscaled", s_lo2 == 300.0, f"got {s_lo2}")
    c2, prem2, ok2 = ess.select_contract("SENSEX", "CE")
    check("mid-week Rs 200 premium still vetoed", ok2 is False,
          f"got {c2} {prem2} {ok2}")
    set_time(10, 7)  # restore clock


def main():
    os.makedirs("/tmp/ess_test_logs", exist_ok=True)
    t1_take_all_pass()
    t2_c1_weak_trigger()
    t3_c2_exhaustion()
    t4_c4_range_extreme()
    t5_c5_kill_zone()
    t6_c6_cooldown_and_c7_daybrake()
    t7_c8_premium_band()
    t8_bracket_sl_flow()
    t9_t1_touch_sl_to_cost()
    t10_two_sl_cooldown()
    t11_eod_flatten()
    t12_dedupe_and_stale()
    t13_scanner_new_candle()
    t14_flip_rule()
    t15_c8_expiry_day_band()
    print()
    print(f"{len(PASSED)} passed, {len(FAILED)} failed")
    if FAILED:
        print("failed:", ", ".join(FAILED))
        sys.exit(1)
    print("ALL TESTS PASSED")


if __name__ == "__main__":
    main()
