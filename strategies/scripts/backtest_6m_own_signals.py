#!/usr/bin/env python
"""
6-month OWN-SIGNAL backtest: engine_signal_strategy scanner, no engine feed.

Signals are generated ONLY by our independent scanner: every completed 5-min
candle with |m5| >= trigger fires a signal in the burst direction, gated by
the same C1-C8 + risk brakes as the live strategy (v1.1 expiry-aware C8).

Data tiers (disclosed):
  * Last ~60 days: REAL 5-min bars (nsei_5m.csv / bsesn_5m.csv).
  * Older ~4 months: 5-min bars SYNTHESISED from real hourly bars
    (nsei_1h.csv / bsesn_1h.csv). Each hour is split into 12x 5-min bars via a
    Brownian-bridge path pinned to the hour's actual O/H/L/C, seeded
    deterministically (date+hour+index) so runs are reproducible. Hourly bars
    are smoother than real 5-min data, so burst frequency in tier 2 is biased
    LOW (conservative for signal count, slight uncertainty on sequence).

Premium model: ATM weekly, E0 = mid-band (NIFTY 160 / SENSEX 380), x0.5 on
the index's expiry weekday (NIFTY Tue, SENSEX Thu); path = E0 + 0.55 * signed
index move since entry; SL -15%, T1 +8% (SL->cost), T2 +25%, flat 15:15 IST;
pessimistic intrabar (SL first on tie); C8 checked for real against the
v1.1 expiry-aware band.

Usage: python3 backtest_6m_own_signals.py   (run in folder with the 4 CSVs)
"""

import csv
import numpy as np
import pandas as pd
from datetime import timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))

PARAMS = {
    "NIFTY": dict(trigger=20.0, exhaustion=40.0, lot=65, premium0=160.0,
                  band=(120.0, 215.0), expiry_weekday=1),
    "SENSEX": dict(trigger=60.0, exhaustion=130.0, lot=20, premium0=380.0,
                   band=(300.0, 460.0), expiry_weekday=3),
}
DELTA = 0.55
RNG_MIN, RNG_MAX = 0.15, 0.85
KILL = (12 * 60, 13 * 60)
EOD = 15 * 60 + 15
SL_PCT, T1_PCT, T2_PCT = 0.15, 0.08, 0.25
DAY_BRAKE = -7000.0
MAX_TRADES = 12
COOLDOWN_MIN = 60
SL_CHAIN_LIMIT = 2
BAND_EXPIRY_SCALE = 0.15
EXPIRY_PREMIUM_FACTOR = 0.5
# cutover: dates >= this use real 5-min data
REAL_5M_FROM = pd.Timestamp("2026-08-31").date()  # first date in 1mo 5m pull


def load_5m(path):
    df = pd.read_csv(path)
    df["ts"] = pd.to_datetime(df["Date"], utc=True).dt.tz_convert(IST)
    df["date"] = df["ts"].dt.date
    df["minute"] = df["ts"].dt.hour * 60 + df["ts"].dt.minute
    df = df[["ts", "date", "minute", "Open", "High", "Low", "Close"]]
    df.columns = ["ts", "date", "minute", "open", "high", "low", "close"]
    return df


def synthesize_5m_from_1h(path):
    """Explode real hourly bars into 5-min bars via seeded Brownian bridge."""
    df = pd.read_csv(path)
    df["ts"] = pd.to_datetime(df["Date"], utc=True).dt.tz_convert(IST)
    rows = []
    for _, r in df.iterrows():
        o, h, l, c = r["Open"], r["High"], r["Low"], r["Close"]
        seed = hash((str(r["ts"]), round(o, 2))) % (2**32)
        rng = np.random.default_rng(seed)
        n = 12
        steps = rng.standard_normal(n)
        steps = np.cumsum(steps)
        steps = steps - steps[-1] * (np.arange(1, n + 1) / n)  # bridge to 0
        path = o + (c - o) * (np.arange(1, n + 1) / n) + steps * (h - l) / 6.0
        path = np.clip(path, l, h)
        path[-1] = c
        prev = o
        for k, close in enumerate(path):
            bar_o = prev
            bar_c = close
            bar_h = min(h, max(bar_o, bar_c) + (h - max(bar_o, bar_c)) * 0.3)
            bar_l = max(l, min(bar_o, bar_c) - (min(bar_o, bar_c) - l) * 0.3)
            ts = r["ts"] + timedelta(minutes=5 * k)
            rows.append((ts, ts.date(), ts.hour * 60 + ts.minute,
                         bar_o, bar_h, bar_l, bar_c))
            prev = bar_c
    out = pd.DataFrame(rows, columns=["ts", "date", "minute",
                                      "open", "high", "low", "close"])
    return out


def effective_band(p, day_date):
    lo, hi = p["band"]
    if day_date.weekday() == p["expiry_weekday"]:
        return (round(lo * BAND_EXPIRY_SCALE, 2), hi, True)
    return (lo, hi, False)


def entry_premium(p, expiry):
    return p["premium0"] * (EXPIRY_PREMIUM_FACTOR if expiry else 1.0)


def gate(df, i, sgn, gov, minute, band, e0):
    close = float(df["close"].iloc[i])
    m5 = close - float(df["close"].iloc[i - 1]) if i >= 1 else 0.0
    m30 = close - float(df["close"].iloc[i - 6]) if i >= 6 else None
    m60 = close - float(df["close"].iloc[i - 12]) if i >= 12 else None
    hi = float(df["high"].iloc[: i + 1].max())
    lo = float(df["low"].iloc[: i + 1].min())
    rng = 0.5 if hi == lo else (close - lo) / (hi - lo)
    ref = m60 if m60 is not None else m30
    trend = 0 if ref is None else (1 if ref > 0 else (-1 if ref < 0 else 0))
    sm5, sm30 = m5 * sgn, (m30 or 0.0) * sgn
    c3 = True if trend == 0 else trend == sgn
    in_cd = gov["cooldown_until"] is not None and minute < gov["cooldown_until"]
    checks = {
        "C1": sm5 >= gov["trigger"],
        "C2": sm30 < gov["exhaustion"],
        "C3": c3,
        "C4": RNG_MIN < rng < RNG_MAX,
        "C5": not (KILL[0] <= minute < KILL[1]),
        "C6": gov["consecutive_sl"] < SL_CHAIN_LIMIT and not in_cd,
        "C7": (not gov["done"]) and gov["realised"] > DAY_BRAKE,
        "C8": band[0] <= e0 <= band[1],
    }
    failed = [k for k, v in checks.items() if not v]
    return failed, dict(close=close, m5=m5, m30=m30, sm5=sm5, sm30=sm30,
                        rng=rng, trend=trend)


def simulate_trade(df, entry_i, sgn, e0):
    entry_index = float(df["close"].iloc[entry_i])
    sl_level, t1_level, t2_level = -e0 * SL_PCT, e0 * T1_PCT, e0 * T2_PCT
    t1_touched = False
    for j in range(entry_i + 1, len(df)):
        minute = int(df["minute"].iloc[j])
        hi = DELTA * sgn * (float(df["high"].iloc[j]) - entry_index)
        lo = DELTA * sgn * (float(df["low"].iloc[j]) - entry_index)
        cl = DELTA * sgn * (float(df["close"].iloc[j]) - entry_index)
        if lo <= sl_level:
            return j, ("TSL" if t1_touched else "SL"), sl_level, t1_touched
        if not t1_touched and hi >= t1_level:
            t1_touched = True
            sl_level = 0.0
        if hi >= t2_level:
            return j, "T2", t2_level, t1_touched
        if minute >= EOD:
            return j, "EOD", cl, t1_touched
    cl = DELTA * sgn * (float(df["close"].iloc[-1]) - entry_index)
    return len(df) - 1, "EOD", cl, t1_touched


def run_day(df, p, day_date, filtered):
    gov = dict(realised=0.0, consecutive_sl=0, cooldown_until=None, done=False,
               trades=0, trigger=p["trigger"], exhaustion=p["exhaustion"])
    trades = []
    band_lo, band_hi, expiry = effective_band(p, day_date)
    e0 = entry_premium(p, expiry)
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
        failed, snap = gate(df, i, sgn, gov, minute, (band_lo, band_hi), e0)
        if filtered and failed:
            trades.append(dict(taken=False, skip=",".join(failed),
                               minute=minute, side="CE" if sgn > 0 else "PE",
                               **snap))
            i += 1
            continue
        exit_i, reason, pnl_pts, t1t = simulate_trade(df, i, sgn, e0)
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
        i = max(i + 1, exit_i)
    return trades


def main():
    real5m = {"NIFTY": load_5m("nsei_5m.csv"), "SENSEX": load_5m("bsesn_5m.csv")}
    synth5m = {"NIFTY": synthesize_5m_from_1h("nsei_1h.csv"),
               "SENSEX": synthesize_5m_from_1h("bsesn_1h.csv")}

    out_rows = []
    for name in ("NIFTY", "SENSEX"):
        p = PARAMS[name]
        real_days = set(real5m[name]["date"].unique())
        syn = synth5m[name][~synth5m[name]["date"].isin(real_days)]
        combined = pd.concat([syn, real5m[name]]).sort_values("ts")
        tiers = {d: ("real" if d in real_days else "synth")
                 for d in combined["date"].unique()}
        for day_date, day_df in combined.groupby("date"):
            day_df = day_df.reset_index(drop=True)
            if len(day_df) < 20:
                continue
            for arm, filtered in (("RAW", False), ("FILTERED", True)):
                for t in run_day(day_df, p, day_date, filtered):
                    t.update(index=name, date=str(day_date), arm=arm,
                             tier=tiers[day_date])
                    out_rows.append(t)

    fields = ["date", "index", "arm", "tier", "minute", "side", "taken",
              "skip", "reason", "pnl", "t1", "m5", "m30", "sm5", "sm30",
              "rng", "trend"]
    with open("backtest_6m_own_signals.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(out_rows)

    def block(rows, label):
        taken = [r for r in rows if r["taken"]]
        n = len(taken)
        w_ = sum(1 for r in taken if r["pnl"] > 0)
        pnl = sum(r["pnl"] for r in taken)
        sk = sum(1 for r in rows if not r["taken"])
        wr = f"{100*w_/n:.0f}%" if n else "-"
        print(f"  {label:38s} taken {n:4d}  skipped {sk:4d}  WR {wr:>4s}  "
              f"P&L Rs {pnl:>12,.0f}")
        return n, pnl

    dates = sorted(set(r["date"] for r in out_rows))
    print(f"\n{'='*76}\n6-MONTH OWN-SIGNAL BACKTEST (scanner only, no engine)  "
          f"{dates[0]} -> {dates[-1]}")
    print(f"sessions: {len(set(d for d in dates))} x2 indices | "
          f"tiers: real 5m from {REAL_5M_FROM}, synthesised 5m before\n")

    for tier in ("real", "synth", None):
        label = {"real": "REAL 5-min tier", "synth": "SYNTHESISED tier",
                 None: "ALL 6 MONTHS"}[tier]
        print(f"--- {label} " + "-" * (60 - len(label)))
        for name in ("NIFTY", "SENSEX", "ALL"):
            for arm in ("RAW", "FILTERED"):
                rows = [r for r in out_rows if r["arm"] == arm
                        and (name == "ALL" or r["index"] == name)
                        and (tier is None or r["tier"] == tier)]
                block(rows, f"{name} {arm}")
        print()

    # monthly P&L, FILTERED arm
    print("Monthly P&L (FILTERED arm, both indices):")
    monthly = {}
    for r in out_rows:
        if r["arm"] == "FILTERED" and r["taken"]:
            monthly.setdefault(r["date"][:7], 0.0)
            monthly[r["date"][:7]] += r["pnl"]
    for m in sorted(monthly):
        print(f"  {m}: Rs {monthly[m]:>10,.0f}")

    # drawdown on FILTERED arm (chronological)
    seq = sorted([r for r in out_rows if r["arm"] == "FILTERED" and r["taken"]],
                 key=lambda r: (r["date"], r["minute"]))
    cum, peak, maxdd = 0.0, 0.0, 0.0
    for r in seq:
        cum += r["pnl"]
        peak = max(peak, cum)
        maxdd = min(maxdd, cum - peak)
    print(f"\nFILTERED arm: total Rs {cum:,.0f} | max drawdown Rs {maxdd:,.0f} | "
          f"trades {len(seq)}")

    # skip-reason census, FILTERED arm
    skips = {}
    for r in out_rows:
        if r["arm"] == "FILTERED" and not r["taken"]:
            for c in r["skip"].split(","):
                skips[c] = skips.get(c, 0) + 1
    print("skip census (FILTERED):",
          ", ".join(f"{k}x{v}" for k, v in sorted(skips.items())))

    # veto value
    print("\nVETO VALUE (raw P&L of trades the gate vetoed):")
    for name in ("NIFTY", "SENSEX"):
        raw = [r for r in out_rows if r["arm"] == "RAW" and r["index"] == name
               and r["taken"]]
        fskip = {(r["minute"], r["side"], r["date"]) for r in out_rows
                 if r["arm"] == "FILTERED" and r["index"] == name
                 and not r["taken"]}
        vetoed = [r for r in raw if (r["minute"], r["side"], r["date"]) in fskip]
        print(f"  {name}: {len(vetoed)} vetoed, raw P&L Rs "
              f"{sum(r['pnl'] for r in vetoed):,.0f}")


if __name__ == "__main__":
    main()
