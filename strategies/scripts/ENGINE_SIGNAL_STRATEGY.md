# Engine Signal Strategy

Implementation of *Engine Signal Strategy — Research Document & OpenAlgo
Automation Specification* (v1.0, Oct 2025): the eight-check filter that turned
the broker engine's raw signals from −₹788 into +₹13,452 over the five-day
evidence window, wired into OpenAlgo for gated auto-execution.

**File:** `engine_signal_strategy.py` — a single self-contained script for the
`/python` strategy host (upload via *Add Strategy*, exchange **NFO**, schedule
09:15–15:20 Mon–Fri).

## Spec → code map

| Spec section | Where it lives |
|---|---|
| Sec 3 — the eight checks C1–C8 | `run_gate()` — pure veto AND, one fail kills |
| Sec 4 — entry pseudo-code (m5/m30/rng/trend, sm5/sm30) | `IndexFeed.features()` + `attempt_signal()` |
| Sec 5 — bracket: SL −15%, engine SL when tighter, T1, T2 +25%, SL→cost, flat 15:15 | `Trade.place_bracket()` / `Trade.manage()` |
| Sec 6 — contract selection (ATM→one ITM step, never OTM; lot from instrument master) | `select_contract()` via `/api/v1/optionsymbol` |
| Sec 8 — brakes: kill zone, 2-SL 60-min timeout, −₹7,000 day brake, 12-trade cap | `RiskGovernor` |
| Sec 9 — retired rules | not implemented, by design (do not re-add without a fresh full-sample test) |
| Sec 11.2 — gate mode + independent scanner mode | `ESS_MODE=gate|scanner|both` |
| Sec 11.5 — state machine | `main()` + `Trade` states |
| Sec 11.6 — failure handling | stale-feed entry freeze, SL-reject market exit, partial-fill re-bracket, inbox dedupe |
| Sec 11.7 — config | every threshold is an `ESS_*` env var (defaults = spec) |
| Sec 11.8 — logging schema | `SignalLogger` → `logs/engine_signal_log.csv`, skips logged with full context |
| Sec 13 — flip rule (provisional) | `ESS_FLIP_ENABLED=true` to arm; **disabled by default** |

## Configuration (defaults = spec §11.7)

Set as parameters in the strategy upload form (they become env vars):

| Env var | Default | Meaning |
|---|---|---|
| `ESS_MODE` | `both` | `gate` = engine alerts only, `scanner` = own bursts, `both` |
| `ESS_TRIGGER_NIFTY` / `ESS_TRIGGER_SENSEX` | 20 / 60 | C1 trigger, index pts per 5 min |
| `ESS_EXH_NIFTY` / `ESS_EXH_SENSEX` | 40 / 130 | C2 exhaustion meter, pts per 30 min |
| `ESS_RNG_MIN` / `ESS_RNG_MAX` | 0.15 / 0.85 | C4 day-range position |
| `ESS_KILL_START` / `ESS_KILL_END` | 12:00 / 13:00 | C5 kill zone (IST) |
| `ESS_COOLDOWN_MIN` | 60 | C6 timeout after 2 consecutive SL |
| `ESS_DAY_BRAKE` | -7000 | C7 realised-P&L day brake (₹) |
| `ESS_MAX_TRADES` / `ESS_TRADE_ALERT_AT` | 12 / 10 | daily trade cap / alert |
| `ESS_SL_PCT` / `ESS_T1_PCT` / `ESS_T2_PCT` | 15 / 8 / 25 | bracket levels, % of premium |
| `ESS_FLIP_ENABLED` | false | provisional flip rule (Phase-1 validation) |
| `ESS_BAND_LO_NIFTY` / `ESS_BAND_HI_NIFTY` | 120 / 215 | C8 premium band (₹) |
| `ESS_BAND_LO_SENSEX` / `ESS_BAND_HI_SENSEX` | 300 / 460 | C8 premium band (₹) |
| `ESS_TELEGRAM_USER` | (empty) | OpenAlgo Telegram bot username for pushes |
| `ESS_EOD_FLATTEN` | 15:15 | flat-by time (IST) |

Expiry and lot size are **never hardcoded** — resolved live from the
instrument master through `/api/v1/optionsymbol` (Sec 6).

## Gate mode: feeding engine alerts

Drop alerts into `engine_signal_inbox.json` (same folder) as a JSON list; the
strategy consumes and clears it each cycle:

```json
[{"index": "NIFTY", "side": "CE", "engine_sl": 142.5, "engine_t1": 168.0}]
```

`engine_sl` / `engine_t1` are optional premium levels from the engine alert;
when present the engine's SL wins if tighter (Sec 5). Dedupe key =
index + direction + 5-minute bucket.

## One-position rule and the 1-lot T1 case

The tool holds at most one open trade. With 1 lot the position cannot split
into lot multiples, so T1 is handled as a *touch*: premium LTP ≥ T1 moves the
SL to entry (cost), and the full lot exits at T2 / SL / 15:15. With ≥2 lots,
half (lot-rounded) is booked by a T1 limit order and the remainder runs with
SL at cost toward T2.

## Data notes

- Features are built from 1-minute candles (`client.history`, today's session)
  resampled to 5-minute closes — no order-flow, depth or OI feed needed
  (explicit research finding, Sec 11.4).
- If the feed goes stale (> `ESS_STALE_SEC`, default 90 s) new entries freeze;
  open brackets keep being monitored through REST quote polling (Sec 11.6).

## Go-live (Spec Sec 12)

1. **Phase 0 — replay:** run the five collected days through the gate offline;
   verdicts must reproduce the manual checklist exactly.
2. **Phase 1 — analyzer:** flip the OpenAlgo UI to *Analyzer Mode* and run
   3–4 weeks (≥30 gated signals). Pass: WR ≥ 60%, brake never breached, every
   skip logged, entry slippage ≤ 2 pts, zero unprotected positions.
3. **Phase 2 — live, 1 lot:** 2 weeks / ≥20 trades; fills within 3 pts of
   analyzer.
4. **Phase 3 — scale** size only, never the rules, after ≥50 live trades.

Threshold changes need a written reason and apply from the next session
(Sec 12.1). BANKNIFTY stays disabled until sampled.
