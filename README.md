# ETH Tester

Bybit ETHUSDT perpetual research system. Only paper trading; no private order-placement API.

**Status:** initial implementation in progress. Public historical trade archives, Bybit market REST and WebSocket, PostgreSQL and deterministic paper-simulation modules. No promise of profitability.

Install Python 3.11+, PostgreSQL 15+, clone repo, copy `.env.example` to `.env`, configure `DATABASE_URL`, `pip install -e .`, run `eth-tester init`, then `eth-tester backfill`, `eth-tester live`, `eth-tester telegram` in separate shells. Verify disk usage and completeness before enabling paper trading.

Historic complete L2 data cannot be reconstructed from trade prints. No model can prove that an order-book level existed historically without archived book observations. All derived metrics have provenance and quality labels.
