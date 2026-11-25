"""Defense-in-depth gate: no authenticated order endpoints or credentials in source."""
from pathlib import Path

from eth_tester.config import Settings


SOURCE = Path(__file__).resolve().parents[2] / "src" / "eth_tester"


def test_source_does_not_contain_private_order_endpoints():
    banned = (
        "/v5/order/create", "/v5/order/amend", "/v5/order/cancel",
        "/v5/position/set-leverage", "/v5/position/trading-stop",
        "/v5/account/wallet-balance",
    )
    for path in SOURCE.rglob("*.py"):
        text = path.read_text(encoding="utf-8").lower()
        for endpoint in banned:
            assert endpoint not in text, f"{path}: prohibited trading endpoint {endpoint}"


def test_default_is_dry_run_and_telegram_opt_in():
    settings = Settings(_env_file=None, _env_prefix="QA_UNUSED_")
    assert settings.dry_run is True
    assert not settings.tg_bot_token
    assert not settings.tg_chat_id
