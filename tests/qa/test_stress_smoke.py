"""Deterministic 25k-trade load smoke, no external network or disk writes."""
from decimal import Decimal as D
from time import perf_counter
from eth_tester.analytics import cluster

def test_25000_trades_preserve_volume_and_delta():
    tape = [{"price": str(3000 + i % 250), "qty": "0.02",
             "side": "Buy" if i % 3 else "Sell"} for i in range(25000)]
    started = perf_counter()
    result = cluster(tape)
    elapsed = perf_counter() - started
    expected_delta = D(500) - D("0.04") * D(len(range(0, 25000, 3)))
    assert result["volume"] == D(500)
    assert result["delta"] == expected_delta
    assert result["tps"] == len(tape)
    assert len(result["levels"]) == 250
    assert elapsed < 20, f"25k tape QA limit exceeded: {elapsed:.2f}s"
