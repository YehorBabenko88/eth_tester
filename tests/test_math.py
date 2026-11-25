from datetime import datetime,timezone
from decimal import Decimal
from eth_tester.analytics import classify_levels,cluster,poc
from eth_tester.paper import estimate_trade,min_viable_target

def test_poc_tie_is_deterministic():
    assert poc({Decimal("101"):Decimal("5"),Decimal("100"):Decimal("5")})==100

def test_cluster_delta():
    item=cluster([{"price":"100","qty":"3","side":"Buy"},
                  {"price":"100","qty":"2","side":"Sell"}])
    assert item["delta"]==1 and item["poc"]==100 and item["volume"]==5

def test_paper_cost():
    outcome=estimate_trade("long",Decimal(100),Decimal(101),Decimal(1000),
                           Decimal("0.00055"),Decimal(2))
    assert 0<outcome["net"]<outcome["gross"]
    assert min_viable_target(Decimal(1000),Decimal("0.00055"),Decimal(2))>0

def test_pivot_confirmation():
    highs=[100,102,110,107,105,111]
    lows=[90,91,92,93,94,95]
    closes=[95,99,102,103,104,112]
    candles=[{"ts":datetime(2026,1,i+1,tzinfo=timezone.utc),
              "high":Decimal(highs[i]),"low":Decimal(lows[i]),
              "close":Decimal(closes[i])} for i in range(6)]
    levels=classify_levels(candles)
    resistance=next(x for x in levels if x[0]=="resistance")
    assert resistance[2]==110
    assert resistance[4]==candles[-1]["ts"]
