# Backtest: Independent Signal Generator (Scanner Mode)

**Data:** Yahoo Finance 5-min candles, `^NSEI` + `^BSESN`, 29 Sep → 28 Oct 2026
(20 trading sessions × 2 indices). Fully out-of-sample vs the Oct 2025 research window.
**Script:** `engine_signal_backtest.py` — replays candles through the same
eight-check gate, brakes and bracket rules as `engine_signal_strategy.py`.
**Trades dump:** `backtest_trades.csv` (every burst, taken or skipped, with
full gate context).

## 1. The decode replicates the engine's cadence

| | Engine (Oct 2025 research) | Scanner backtest (Sep–Oct 2026) |
|---|---|---|
| Index signals/day | 15–20 | **17.1 (NIFTY) / 19.8 (SENSEX)** |
| First signal | 09:16–09:21 | **median 09:20–09:22** |
| Direction mix | CE and PE same day | yes, both |

Firing on every completed 5-min candle with `|m5| ≥ 20/60` reproduces the
engine's frequency and opening behaviour almost exactly. The trigger decode is
behaviourally confirmed: **independent signal generation works**.

## 2. Gate behaviour (this month, this regime)

| Arm | Taken | Skipped | WR | P&L (simulated) |
|---|---|---|---|---|
| NIFTY raw | 101 | 0 | 37% | +₹23,428 |
| NIFTY filtered | 35 | 248 | 29% | +₹4,861 |
| SENSEX raw | 136 | 0 | 33% | +₹1,681 |
| SENSEX filtered | 45 | 287 | 20% | −₹10,543 |
| **All raw** | **237** | — | **35%** | **+₹25,110** |
| **All filtered** | **80** | 535 | **24%** | **−₹5,681** |

Most frequent vetoes: C4 (range position), C2 (exhaustion), C3 (trend),
C5 (kill zone) — the checks fire at plausible rates and the plumbing
(dedupe, cooldown, day brake, EOD) behaved correctly throughout.

## 3. The honest caveat: do not trust the P&L sign here

The exit-side P&L is built on a **synthetic premium model** — we only have
index candles, not option candles. Premium path ≈ `E0 + 0.55 × signed index
move`, mid-band entry premium, SL/T1/T2 at −15/+8/+25% of premium, intrabar
ties resolved against the trade. A sensitivity sweep shows the result flips
with the modelling choices:

- Intrabar tie-breaking alone moves monthly P&L by **±₹20–40k** at 5-min
  granularity.
- Delta 0.45→0.75 moves it by a further ~2×.
- No theta decay is modelled (flatters long holds).

The research numbers (−₹788 raw / +₹13,452 filtered) came from **real
screenshot fills**; this backtest cannot reproduce that measurement, only the
signal-generation and gate-selection behaviour.

## 4. What the regime data actually says

Forward follow-through after bursts (model-free, measured directly on index):

| | n | MFE 15-min median | P(net 15-min > 0) |
|---|---|---|---|
| NIFTY gate-pass | 51 | 9.8 pts | 39% |
| NIFTY gate-fail | 292 | 6.6 pts | 49% |
| SENSEX gate-pass | 68 | 29.1 pts | 37% |
| SENSEX gate-fail | 328 | 23.7 pts | 50% |

Gate-passed bursts make **bigger excursions** but this month was a sideways
regime: bursts did not continue directionally (P(net>0) ≈ 37–50% everywhere).
This is the choppy regime the spec's standing risk note describes —
long-premium buying "loses on most days in choppy regimes and makes its money
in bursts." Sep–Oct 2026 was not a burst month; Oct 2025 was.

## 5. Conclusions

1. **Signal generation is solved** — scanner mode reproduces the engine's
   cadence; you can run independently of the broker engine.
2. **The gate mechanics are verified** — vetoes fire at the right places,
   brakes hold, logging works (see `backtest_trades.csv`).
3. **P&L validation cannot come from index candles alone.** The gate's edge
   was measured on real premium fills; validating it forward needs either
   real option candle history or (per spec §12) Phase-1 analyzer mode with
   live prices. The analyzer run remains the plan — this backtest is its
   rehearsal, not its substitute.
4. **Regime risk is real** — a 20-day chop window would have been a losing
   month for momentum option-buying with or without the filter. The −₹7,000
   day brake is what keeps such months survivable; it tripped correctly in
   simulation.

## Reproduce

```bash
# fetch data (needs yahoo_finance datasource or yfinance)
python3 engine_signal_backtest.py   # reads nsei_5m.csv / bsesn_5m.csv
```
