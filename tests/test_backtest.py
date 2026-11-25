from datetime import datetime,timedelta,timezone
from decimal import Decimal
from eth_tester import backtest
from eth_tester.archives import parse_trade
from eth_tester.strategy import Signal

def test_stop_first_and_next_bar(monkeypatch):
    start=datetime(2026,1,1,tzinfo=timezone.utc)
    bars=[{"ts":start+timedelta(minutes=i),"open":Decimal(100),
      "high":Decimal("100.2"),"low":Decimal("99.8"),"close":Decimal(100)}
      for i in range(30)]
    bars[20]["high"]=Decimal(105)
    bars[20]["low"]=Decimal(95)
    sig=Signal("long",Decimal(100),Decimal(98),Decimal(104),{"test":True})
    monkeypatch.setattr(backtest,"price_signal",
        lambda candles,*a: sig if len(candles)==20 else None)
    deals=backtest.simulate(bars)
    assert len(deals)==1
    assert deals[0]["opened_at"]==bars[20]["ts"]
    assert deals[0]["outcome"]=="stop"

def test_archive_row():
    stamp,identifier,price,qty,side=parse_trade({"timestamp":"1764000000.1234",
      "trdMatchID":"abc","side":"Sell","price":"3500.25","size":"1.5"})
    assert stamp.tzinfo and identifier=="abc" and side=="Sell"
    assert price*qty==Decimal("5250.375")
