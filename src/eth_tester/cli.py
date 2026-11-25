import argparse
import asyncio
from .config import Settings
from .database import connection,init_database

def main():
    parser=argparse.ArgumentParser(description="ETHUSDT public-data and paper-trading laboratory")
    parser.add_argument("--tf",default="60")
    parser.add_argument("--trade-id",type=int)
    commands=parser.add_subparsers(dest="command",required=True)
    for cmd in ("init","backfill","archives","live","levels","footprint",
                "telegram","status","chart","backtest","engine","learn","train-causal-shadow","label-horizons",
                "cleanup","analyse","run","book-replay","aggregate"):
        commands.add_parser(cmd)
    args=parser.parse_args()
    settings=Settings()
    if settings.symbol!="ETHUSDT" or settings.category!="linear":
        raise SystemExit("v0.1 supports ETHUSDT linear perpetual only")
    if args.command=="init":
        init_database(settings)
        print("PostgreSQL tables initialized")
    elif args.command=="run":
        from .supervisor import supervise
        asyncio.run(supervise(settings))
    elif args.command=="backfill":
        from .history import backfill_rest
        asyncio.run(backfill_rest(settings))
    elif args.command=="archives":
        from .archives import backfill_archives
        asyncio.run(backfill_archives(settings))
    elif args.command=="live":
        from .live import run_live
        asyncio.run(run_live(settings))
    elif args.command=="levels":
        from .analytics import build_levels
        print("Levels:",build_levels(settings,args.tf))
    elif args.command=="footprint":
        from .analytics import build_footprint
        print("Buckets:",build_footprint(settings,args.tf))
    elif args.command=="telegram":
        from .telegram_control import run_telegram
        asyncio.run(run_telegram(settings))
    elif args.command=="backtest":
        from .backtest import run_backtest
        print("Backtest:",run_backtest(settings,args.tf))
    elif args.command=="engine":
        from .runner import run_engine
        asyncio.run(run_engine(settings))
    elif args.command=="aggregate":
        from .rollups import aggregate_history
        print("Historical aggregated intervals:",aggregate_history(settings))
    elif args.command=="book-replay":
        from .book_analysis import replay_orderbook
        print("Book events:",replay_orderbook(settings))
    elif args.command=="analyse":
        from .workers import analytics_worker
        asyncio.run(analytics_worker(settings))
    elif args.command in ("learn", "train-causal-shadow", "label-horizons"):
        # 'learn' is a backward-compatible alias for verified shadow ONLY.
        from .control_jobs import run_control_job
        job = "train-causal-shadow" if args.command == "learn" else args.command
        print("Paper research:", run_control_job(settings, job))
    elif args.command=="cleanup":
        from .retention import cleanup_diagnostics
        print(cleanup_diagnostics(settings))
    elif args.command=="status":
        with connection(settings) as conn:
            for row in conn.execute("SELECT source,interval,status,cursor_ts FROM ingestion_cursor"):
                print(dict(row))
            size=conn.execute("SELECT pg_size_pretty(pg_database_size(current_database())) AS n").fetchone()
            print("DB size",size["n"])
    elif args.command=="chart":
        if not args.trade_id:
            raise SystemExit("--trade-id required")
        from .charts import render_trade
        print(render_trade(settings,args.trade_id))

if __name__=="__main__":
    main()
