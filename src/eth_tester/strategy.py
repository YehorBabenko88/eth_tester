"""Causal price candidates; live confirmation is mandatory in orderflow."""
from dataclasses import dataclass
from decimal import Decimal as D, InvalidOperation
from .analytics import confirmed_pivots
from .paper import estimate_trade, min_viable_target
from .strategies import Parameters, pattern

@dataclass(frozen=True)
class Signal:
    direction: str
    level: D
    stop: D
    take: D
    reason: dict

def economics(direction, price, stop, take, fee, slip, params=Parameters()):
    """Outcome-at-TP feasibility, not independently calibrated expectancy."""
    if direction not in ("long", "short"):
        return None
    try:
        price, stop, take, fee, slip = (
            D(str(value)) for value in (price, stop, take, fee, slip))
        reserve_fraction = D(params.funding_reserve)
        minimum_net = D(params.min_net)
        minimum_rr = D(params.reward_risk)
        values = (price, stop, take, fee, slip, reserve_fraction,
                  minimum_net, minimum_rr)
        if not all(value.is_finite() for value in values):
            return None
        if (fee < 0 or slip < 0 or reserve_fraction < 0 or minimum_net < 0
                or minimum_rr <= 0):
            return None
        correct = (0 < stop < price < take if direction == "long"
                   else 0 < take < price < stop)
        if not correct:
            return None
        reserve = reserve_fraction * D(1000)
        win = estimate_trade(direction, price, take, D(1000), fee, slip,
                             reserve)["net"]
        loss = -estimate_trade(direction, price, stop, D(1000), fee, slip,
                              reserve)["net"]
        if (not win.is_finite() or not loss.is_finite() or loss <= 0
                or win < minimum_net * D(1000)
                or win / loss < minimum_rr):
            return None
        return {"net_target_fraction": str(win / D(1000)),
                "net_risk_fraction": str(loss / D(1000)),
                "net_reward_risk": str(win / loss),
                "funding_reserve_fraction": str(reserve_fraction),
                "break_even_win_rate": str(loss / (win + loss))}
    except (InvalidOperation, ValueError, TypeError, ZeroDivisionError):
        return None

def calibrated_expectancy(direction, price, stop, take, fee, slip, probability_lower,
                           params=Parameters()):
    """Reject without an independently calibrated conservative win-rate bound.

    The baseline economics() only proves that a successful TP covers modelled
    expenses, never that the strategy has positive expected value.
    """
    if probability_lower is None:
        return None
    try:
        probability = D(str(probability_lower))
        if not probability.is_finite() or not D(0) < probability < D(1):
            return None
        costs = economics(direction, price, stop, take, fee, slip, params)
        if costs is None:
            return None
        win = D(costs["net_target_fraction"])
        loss = D(costs["net_risk_fraction"])
        expected = probability * win - (D(1) - probability) * loss
        if expected < D(params.min_net):
            return None
        return {**costs, "win_probability_lower_bound": str(probability),
                "expected_net_fraction_lower_bound": str(expected),
                "calibration_required": True}
    except (InvalidOperation, ValueError, TypeError):
        return None


def price_signal(closed_candles,fee_rate=D("0.00055"),slip_bps=D("2"),
                 max_lookback=180,*,volume_profiles=()):
    params=Parameters()
    candles=list(closed_candles[-max_lookback:])
    if len(candles)<12:
        return None
    try:
        step=candles[1]["ts"]-candles[0]["ts"]
        if step.total_seconds()<=0 or any(b["ts"]-a["ts"]!=step for a,b in zip(candles,candles[1:])):
            return None
        for b in candles:
            o,h,low,c=(D(b[k]) for k in ("open","high","low","close"))
            if not all(x.is_finite() for x in (o,h,low,c)) or not 0<low<=min(o,c)<=max(o,c)<=h:
                return None
        last=candles[-1]
        price=D(last["close"])
        atr=sum((max(D(b["high"])-D(b["low"]),abs(D(b["high"])-D(a["close"])),
                     abs(D(b["low"])-D(a["close"]))) for a,b in zip(candles[-13:-1],candles[-12:])),D(0))/min(12,len(candles)-1)
        if atr<=0 or atr/price>D("0.03"):
            return None
        candidates=[]
        for side,origin,level,confirmed in confirmed_pivots(candles[:-1],2):
            level=D(level)
            if abs(price-level)/level>D(params.max_distance):
                continue
            history=[b for b in candles[:-1] if b["ts"]>confirmed]
            crossed=[b for b in history if (D(b["close"])>level if side=="resistance" else D(b["close"])<level)]
            broken=bool(crossed)
            if broken and crossed[-1]["ts"]!=candles[-2]["ts"]:
                continue
            match=pattern(side,level,candles[-2],last,broken)
            if not match:
                continue
            direction,name=match
            sign=D(1) if direction=="long" else D(-1)
            anchor=min(level,D(last["low"])) if sign>0 else max(level,D(last["high"]))
            stop=anchor-sign*max(atr*D("0.5"),price*D("0.001"))
            risk=abs(price-stop)
            take=price+sign*max(risk*D(3),price*(min_viable_target(D(1000),fee_rate,slip_bps)+D(params.funding_reserve)+D(params.min_net)))
            costs=economics(direction,price,stop,take,fee_rate,slip_bps,params)
            if costs is None:
                continue
            reason={"strategy":name,"strategy_version":params.version,**params.identity(),
                    "origin":origin.isoformat(),"confirmed":confirmed.isoformat(),
                    "side":side,"level":str(level),"atr":str(atr),"reference_price":str(price),
                    "source":"closed_candles","economics":costs,"requires_live_confirmation":True}
            candidates.append((abs(price-level),name,Signal(direction,level,stop,take,reason)))
        if volume_profiles:
            from .strategies.volume import volume_signal
            for profile in volume_profiles:
                candidate = volume_signal(candles, profile, fee_rate, slip_bps)
                if candidate is not None:
                    candidates.append((abs(price - candidate.level),
                                       candidate.reason["strategy"], candidate))
        return min(candidates,key=lambda x:(x[0],x[1]))[2] if candidates else None
    except (InvalidOperation,ValueError,TypeError,KeyError):
        return None
