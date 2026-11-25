# ETH Tester — ETHUSDT perpetual research & paper simulation

**Status: experimental foundation. Not production-ready; not a real-money trading system.**

## What is implemented

- Public Bybit V5 history collectors: trade, mark, index and premium-index candles across available Bybit intervals; funding, six OI intervals and six long/short account-ratio intervals. Each source has a resume cursor.
- Full-day public ETHUSDT gzip trade-archive downloader: streaming SHA-256, CSV/schema and UTC-date checks, atomic PostgreSQL COPY staging, resumable progress. Source availability is exchange-dependent.
- Public WebSocket for raw trade prints, depth-configurable orderbook, ticker/mark/index/OI/funding snapshots and all-liquidation events. Exponential reconnection, persistent tick-gap records and REST refresh.
- Local dedicated PostgreSQL database with indexed raw trades, OHLCV, funding, OI, ratios, tickers, liquidation, book messages, reconstructed L2 state, large wall changes, footprint, POC, delta, windowed CVD, tape speed, levels, models, signals, backtests and paper trades.
- Deterministic confirmed-pivot rejection signals; one simulated position at a time, $1,000 notional, TP/SL, risk cap, fees, slippage and approximate funding. Conservative OHLC backtests enter at the *next* bar and assume stop hit first when both TP/SL fit within the same bar.
- Telegram allowlisted /pause /resume /stop /stats /disk /progress /levels /trade /chart ID /gaps; remote database deletion deliberately restricted.
- Chronological training of experimental shadow logistic models from closed paper trades only. Model is **not** auto-deployed or used to promise profits.

## Install on Windows PowerShell

1. Install PostgreSQL 15+ and Python 3.11+; allow the local DB role to CREATE DATABASE or create `eth_tester` yourself.
2. Clone the repository; use a Python virtual environment and edit the local `.env` copied from `.env.example`. Never commit passwords or the Telegram token.

```powershell
git clone https://github.com/YehorBabenko88/eth_tester.git
cd eth_tester
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
Copy-Item .env.example .env
notepad .env
eth-tester init
eth-tester run
```

`run` starts separately supervised live ingestion, analytics, simulation and daily REST/archive ingestion, plus Telegram if configured. The simulated bot starts paused. Use Telegram /resume only after verifying that data are complete and feed health is acceptable.

### Other commands

- `eth-tester backfill` / `eth-tester archives` — historical exchange REST and dated gzip archives.
- `eth-tester --tf 60 levels` / `eth-tester --tf 1 footprint` / `eth-tester book-replay` — analytics.
- `eth-tester --tf 5 backtest` / `eth-tester learn` — paper research and shadow learning.
- `eth-tester --trade-id 1 chart` — local annotated paper-trade chart.
- `eth-tester status` / `eth-tester cleanup` — checkpoints, storage and removal of *diagnostic logs only*.

## Limitations and safety

- A complete historical L2 orderbook is **not** guaranteed by Bybit REST; archived trade prints cannot reconstruct order placement, cancellation or hidden liquidity. Current book streams start at local capture time. A gap requires an exchange snapshot, which does not recover intermediate book events.
- L2 quantity decreases are marked with **unknown attribution** because an apparent removal could be a fill, cancellation or replacement.
- Unresolved trade-data gaps block automatic paper entries; only fully downloaded public UTC archives can be used to mark the affected historical days recovered. Latest-day gap coverage may be unavailable until the next archive appears.
- `verified_volume` checks trade aggregates against official candle volume, not the presence of every exchange print. CVD has an explicit window baseline.
- The funding model estimates settlement using the entry notional rather than exact exchange mark-price settlement. Backtest is OHLC-based, not a latency-aware queue simulator.
- The full multi-year tick database can be extremely large. Storage partitioning, incremental vacuum, archival/recovery, retention and all-day load tests require qualification. Cleanup never silently deletes raw market events.
- GitHub source code and Python unit tests do not establish successful local PostgreSQL installation, successful Bybit connectivity or profitable trading. End-to-end acceptance has NOT been completed.

Reference: https://bybit-exchange.github.io/docs/v5/market/ and https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook
