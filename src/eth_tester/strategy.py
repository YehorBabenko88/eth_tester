"""Time-causal pivot rejection: no future candle can influence entry."""
from dataclasses import dataclass
from decimal import Decimal
from .analytics import confirmed_pivots
from .paper import min_viable_target

@dataclass(frozen=True)
class Signal:
    direction: str
    level: Decimal
    stop: Decimal
    take: Decimal
    reason: dict

def price_signal(closed_candles,fee_rate=Decimal("0.00055"),slip_bps=Decimal("2"),max_lookback=180):
    if len(closed_candles)<12:
        return None
    candles=closed_candles[-max_lookback:]
    last=candles[-1]
    price=Decimal(last["close"])
    if price<=0:
        return None
    pivots=confirmed_pivots(candles[:-1],2)
    sample=candles[-12:]
    atr=sum((Decimal(x["high"])-Decimal(x["low"]) for x in sample),Decimal(0))/len(sample)
    if atr<=0:
        return None
    minimum=min_viable_target(Decimal(1000),fee_rate,slip_bps)
    candidates=[]
    for side,origin,level,confirmed in pivots:
        if any((Decimal(c["close"])>level if side=="resistance" else Decimal(c["close"])<level)
               for c in candles if confirmed<c["ts"]<last["ts"]):
            continue
        distance=abs(price-level)/level
        if distance>Decimal("0.004"):
            continue
        accepted=((side=="support" and Decimal(last["low"])<=level
            and price>level and price>Decimal(last["open"])) or
            (side=="resistance" and Decimal(last["high"])>=level
            and price<level and price<Decimal(last["open"])))
        if not accepted:
            continue
        direction="long" if side=="support" else "short"
        risk=max(atr,price*Decimal("0.0025"))
        stop=(min(level,Decimal(last["low"]))-risk*Decimal("0.25") if direction=="long"
              else max(level,Decimal(last["high"]))+risk*Decimal("0.25"))
        distance_stop=abs(price-stop)
        take=(price+max(distance_stop*2,price*minimum) if direction=="long"
              else price-max(distance_stop*2,price*minimum))
        if take<=0:
            continue
        reason={"strategy":"pivot_rejection_v1","origin":origin.isoformat(),
                "confirmed":confirmed.isoformat(),"side":side,
                "level":str(level),"atr":str(atr),"source":"closed_candles"}
        candidates.append((distance,Signal(direction,level,stop,take,reason)))
    return min(candidates,key=lambda x:x[0])[1] if candidates else None
