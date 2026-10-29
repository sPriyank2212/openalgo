#!/usr/bin/env python
"""
Backtest: independent (scanner-mode) signal generator, engine-signal strategy.

Replays historical 5-min index candles through the SAME eight-check gate and
risk-brake rules as strategies/scripts/engine_signal_strategy.py, generating
signals independently (|m5| >= trigger bursts, direction = sign of burst).

Two arms per burst signal:
  - RAW:      every burst taken (the "engine raw" equivalent)
  - FILTERED: only bursts passing C1-C8 + brakes (the checklist arm)

Trade simulation (disclosed approximations - see README output):
  * ATM weekly option, entry premium E0 = mid-band (NIFTY 160 / SENSEX 380)
  * Premium path approximated from the index path: P = E0 + DELTA * signed
    index move since entry (delta 0.55, no theta decay)
  * SL = -15% of premium, T1 = +8%, T2 = +25%; SL -> cost after T1 touch
  * Intrabar ambiguity resolved pessimistically: if a candle's range touches
    both SL and a target, SL is assumed hit first
  * Flat by the 15:15 IST candle; one open position at a time
  * C8 (premium sanity) assumed pass - entry premium is synthetic by design
  * Lot sizes: NIFTY 65, SENSEX 20 (verify against instrument master live)

Usage: python3 engine_signal_backtest.py   (reads nsei_5m.csv / bsesn_5m.csv)
"""

import csv
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd

IST = timezone(timedelta(hours=5, minutes=30))

# --- parameters (spec defaults, Sec 11.7) -------------------------------------
PARAMS = {
    "NIFTY": dict(trigger=20.0, exhaustion=40.0, lot=65, premium0=160.0),
    "SENSEX": dict(trigger=60.0, exhaustion=130.0, lot=20, premium0=380.0),
}
DELTA = 0.55
RNG_MIN, RNG_MAX = 0.15, 0.85
KILL = (12 * 60, 13 * 60)            # 12:00-13:00 IST
EOD = 15 * 60 + 15                   # 15:15 IST
SL_PCT, T1_PCT, T2_PCT = 0.15, 0.08, 0.25
DAY_BRAKE = -7000.0
MAX_TRADES = 12
COOLDOWN_MIN = 60
SL_CHAIN_LIMIT = 2


def load(path, name):
    df = pd.read_csv(path)
    df["ts"] = pd.to_datetime(df["Date"], utc=True).dt.tz_convert(IST)
    df["date"] = df["ts"].dt.date
    df["minute"] = df["ts"].dt.hour * 60 + df["ts"].dt.minute
    df = df[["ts", "date", "minute", "Open", "High", "Low", "Close"]]
    df.columns = ["ts", "date", "minute", "open", "high", "low", "close"]
    return name, df


class DayState:
    def __init__(self, day_df):
        self.df = day_df.reset_index(drop=True)
        self.day_open = float(self.df["open"].iloc[0])


def gate(day, i, sgn, gov, minute):
    """Evaluate checks C1-C8 at completed candle i. Returns (failed, snap)."""
    df = day.df
    close = float(df["close"].iloc[i])
    m5 = close - float(df["close"].iloc[i - 1]) if i >= 1 else 0.0
    m30 = close - float(df["close"].iloc[i - 6]) if i >= 6 else None
    m60 = close - float(df["close"].iloc[i - 12]) if i >= 12 else None
    hi = float(df["high"].iloc[: i + 1].max())
    lo = float(df["low"].iloc[: i + 1].min())
    rng = 0.5 if hi == lo else (close - lo) / (hi - lo)
    ref = m60 if m60 is not None else m30
    trend = 0 if ref is None else (1 if ref > 0 else (-1 if ref < 0 else 0))
    sm5 = m5 * sgn
    sm30 = (m30 or 0.0) * sgn

    c3 = True if trend == 0 else trend == sgn  # flip rule disabled (default)
    in_cooldown = gov["cooldown_until"] is not None and minute < gov["cooldown_until"]

    checks = {
        "C1": sm5 >= gov["trigger"],
        "C2": sm30 < gov["exhaustion"],
        "C3": c3,
        "C4": RNG_MIN < rng < RNG_MAX,
        "C5": not (KILL[0] <= minute < KILL[1]),
        "C6": gov["consecutive_sl"] < SL_CHAIN_LIMIT and not in_cooldown,
        "C7": (not gov["done"]) and gov["realised"] > DAY_BRAKE,
        "C8": True,  # premium synthetic; band assumed (disclosed)
    }
    failed = [k for k, v in checks.items() if not v]
    return failed, dict(close=close, m5=m5, m30=m30, sm5=sm5, sm30=sm30,
                        rng=rng, trend=trend)


def simulate_trade(day, entry_i, sgn, p):
    """Walk candles after the burst close; return (exit_i, exit_reason,
    pnl_points_premium, t1_touched). Prices in premium points."""
    df = day.df
    entry_index = float(df["close"].iloc[entry_i])
    e0 = p["premium0"]
    sl_pts = e0 * SL_PCT
    t1_pts = e0 * T1_PCT
    t2_pts = e0 * T2_PCT
    sl_level, t1_level, t2_level = -sl_pts, t1_pts, t2_pts
    t1_touched = False

    for j in range(entry_i + 1, len(df)):
        minute = int(df["minute"].iloc[j])
        # premium excursion approximated from index path
        hi = DELTA * sgn * (float(df["high"].iloc[j]) - entry_index)
        lo = DELTA * sgn * (float(df["low"].iloc[j]) - entry_index)
        cl = DELTA * sgn * (float(df["close"].iloc[j]) - entry_index)

        # SL first within the candle (pessimistic) when both touched
        if lo <= sl_level:
            return j, ("TSL" if t1_touched else "SL"), sl_level, t1_touched
        if not t1_touched and hi >= t1_level:
            t1_touched = True
            sl_level = 0.0  # SL to cost
        if hi >= t2_level:
            return j, "T2", t2_level, t1_touched
        if minute >= EOD:
            return j, "EOD", cl, t1_touched
    # ran out of candles
    cl = DELTA * sgn * (float(df["close"].iloc[-1]) - entry_index)
    return len(df) - 1, "EOD", cl, t1_touched


def run_day(name, day, p, filtered):
    """One arm of one day. Returns list of trade dicts."""
    gov = dict(realised=0.0, consecutive_sl=0, cooldown_until=None, done=False,
               trades=0, trigger=p["trigger"], exhaustion=p["exhaustion"])
    trades = []
    df = day.df
    i = 1
    while i < len(df) - 1:
        minute = int(df["minute"].iloc[i])
        m5 = float(df["close"].iloc[i]) - float(df["close"].iloc[i - 1])
        if abs(m5) < p["trigger"] or gov["done"] or gov["trades"] >= MAX_TRADES:
            i += 1
            continue
        if gov["cooldown_until"] is not None and minute < gov["cooldown_until"]:
            i += 1
            continue
        sgn = 1 if m5 > 0 else -1
        failed, snap = gate(day, i, sgn, gov, minute)
        if filtered and failed:
            trades.append(dict(taken=False, skip=",".join(failed), minute=minute,
                               side="CE" if sgn > 0 else "PE", **snap))
            i += 1
            continue
        exit_i, reason, pnl_pts, t1t = simulate_trade(day, i, sgn, p)
        pnl = pnl_pts * p["lot"]
        gov["realised"] += pnl
        gov["trades"] += 1
        if reason == "SL":
            gov["consecutive_sl"] += 1
            if gov["consecutive_sl"] >= SL_CHAIN_LIMIT:
                gov["cooldown_until"] = int(df["minute"].iloc[exit_i]) + COOLDOWN_MIN
        else:
            gov["consecutive_sl"] = 0
        if gov["realised"] <= DAY_BRAKE:
            gov["done"] = True
        trades.append(dict(taken=True, skip="", minute=minute,
                           side="CE" if sgn > 0 else "PE", reason=reason,
                           pnl=round(pnl, 1), t1=t1t, **snap))
        i = max(i + 1, exit_i)  # one position at a time; resume after exit
    return trades


def main():
    datasets = [load("nsei_5m.csv", "NIFTY"),
                load("bsesn_5m.csv", "SENSEX")]
    out_rows = []
    for name, df in datasets:
        p = PARAMS[name]
        for day_date, day_df in df.groupby("date"):
            day = DayState(day_df)
            if len(day.df) < 20:
                continue
            for arm, filtered in (("RAW", False), ("FILTERED", True)):
                for t in run_day(name, day, p, filtered):
                    t.update(index=name, date=str(day_date), arm=arm)
                    out_rows.append(t)

    fields = ["date", "index", "arm", "minute", "side", "taken", "skip",
              "reason", "pnl", "t1", "m5", "m30", "sm5", "sm30", "rng", "trend"]
    with open("backtest_trades.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(out_rows)

    # ---- summary ----
    def summarize(rows):
        taken = [r for r in rows if r["taken"]]
        pnl = sum(r["pnl"] for r in taken)
        wins = sum(1 for r in taken if r["pnl"] > 0)
        return len(taken), wins, pnl

    dates = sorted(set(r["date"] for r in out_rows))
    print(f"\n{'='*72}\nBACKTEST: independent scanner, {dates[0]} -> {dates[-1]}")
    days = sorted(set((r["date"], r["index"]) for r in out_rows))
    print(f"days: {len(set(d for d, _ in days))} sessions x2 indices\n")

    for name in ("NIFTY", "SENSEX", "ALL"):
        for arm in ("RAW", "FILTERED"):
            rows = [r for r in out_rows if r["arm"] == arm
                    and (name == "ALL" or r["index"] == name)]
            n, w, pnl = summarize(rows)
            skipped = sum(1 for r in rows if not r["taken"])
            wr = f"{100*w/n:.0f}%" if n else "-"
            print(f"{name:7s} {arm:9s}  taken {n:3d}  skipped {skipped:3d}  "
                  f"WR {wr:>4s}  P&L Rs {pnl:>10,.0f}")

    # per-day table, FILTERED arm
    print(f"\n{'date':12s}{'idx':8s}{'trades':>7s}{'P&L':>10s}   skip reasons")
    for (d, idx) in days:
        rows = [r for r in out_rows if r["arm"] == "FILTERED"
                and r["date"] == d and r["index"] == idx]
        taken = [r for r in rows if r["taken"]]
        pnl = sum(r["pnl"] for r in taken)
        skips = {}
        for r in rows:
            if not r["taken"]:
                for c in r["skip"].split(","):
                    skips[c] = skips.get(c, 0) + 1
        skipstr = ", ".join(f"{k}x{v}" for k, v in sorted(skips.items()))
        print(f"{d:12s}{idx:8s}{len(taken):>7d}{pnl:>10,.0f}   {skipstr}")

    # veto effectiveness: P&L of raw trades that the gate would have skipped
    print(f"\n{'='*72}\nVETO VALUE (what the filter saved):")
    for name in ("NIFTY", "SENSEX"):
        raw = [r for r in out_rows if r["arm"] == "RAW" and r["index"] == name and r["taken"]]
        # match by minute/side against filtered skips
        fskip = {(r["minute"], r["side"], r["date"]) for r in out_rows
                 if r["arm"] == "FILTERED" and r["index"] == name and not r["taken"]}
        vetoed = [r for r in raw if (r["minute"], r["side"], r["date"]) in fskip]
        saved = sum(r["pnl"] for r in vetoed)
        print(f"  {name}: {len(vetoed)} raw trades vetoed, their raw P&L was Rs {saved:,.0f}")


if __name__ == "__main__":
    main()
