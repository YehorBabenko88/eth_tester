from datetime import date
from decimal import Decimal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: str = "postgresql://postgres:password@127.0.0.1:5432/eth_tester"
    bybit_rest: str = "https://api.bybit.com"
    bybit_ws: str = "wss://stream.bybit.com/v5/public/linear"
    symbol: str = "ETHUSDT"
    category: str = "linear"
    archive_start: date = date(2020, 1, 1)
    rest_rps: float = 3.0
    archive_downloads_per_second: float = 0.3
    initial_notional_usd: Decimal = Decimal("1000")
    initial_equity_usd: Decimal = Decimal("10000")
    taker_fee_rate: Decimal = Decimal("0.00055")
    maker_fee_rate: Decimal = Decimal("0.0002")
    slippage_bps: Decimal = Decimal("2")
    risk_fraction: Decimal = Decimal("0.01")
    max_daily_drawdown: Decimal = Decimal("0.03")
    tg_bot_token: str = ""
    tg_chat_id: str = ""
    minimum_free_gb: float = 20.0
    dry_run: bool = True
