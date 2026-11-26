"""Conservative historical win-rate evidence for a *frozen* shadow policy.

Wilson is a one-sided binomial interval, not a guarantee under regime change
or correlated trades. Never interpret the 3-class probabilities as P(win).
"""
from hashlib import sha256
import json
import math
import time

from . import validate, evaluate


def wilson_lower(wins, trials, z=1.6448536269514722):
    """One-sided ~95% lower confidence limit conditional on independent trials."""
    if not isinstance(wins, int) or not isinstance(trials, int) or not 0 <= wins <= trials:
        raise ValueError("invalid Bernoulli counts")
    if not 0 < z <= 5:
        raise ValueError("invalid one-sided critical value")
    if trials == 0:
        return None
    # The exact zero-win boundary must not have a positive rounding residue.
    if wins == 0:
        return 0.0
    p = wins / trials
    denom = 1 + z*z/trials
    center = p + z*z/(2*trials)
    radius = z*math.sqrt(p*(1-p)/trials + z*z/(4*trials*trials))
    return max(0.0, (center-radius)/denom)


def winrate_certificate(model, holdout, *, strategy_version, minimum_ev=0.0,
                        min_trades=30):
    """Bound realized net wins on independent completed, causal observations.

    The caller must precommit to this locked holdout and strategy_version.
    Returns None without enough qualifying entries, positive aggregate net, or
    complete labels. No instrument order or strategy changes are performed.
    """
    if not strategy_version or not isinstance(strategy_version, str):
        raise ValueError("strategy version provenance required")
    if min_trades < 30 or not holdout:
        return None
    try:
        validated = validate(holdout, max(r.exit_ms for r in holdout))
        if len(validated) != len(holdout):
            return None
        evaluate(model, validated, minimum_ev)
    except (ValueError, TypeError, OverflowError):
        return None
    selected = [r for r in validated if model.predict(r.values) != "uncertain"
                and model.expected_net(r.values) > minimum_ev]
    if len(selected) < min_trades or sum(r.net_return for r in selected) <= 0:
        return None
    wins = sum(r.net_return > 0 for r in selected)
    evidence = [(r.event_id, r.decision_ms, r.exit_ms, r.net_return)
                for r in selected]
    digest = sha256(json.dumps(evidence, separators=(",", ":")).encode()).hexdigest()
    issued = time.time_ns() // 1000000
    if issued < max(r.exit_ms for r in validated):
        return None
    return {"schema_version": 2, "issuer": "eth_tester.ml.calibration.v2",
            "issued_at_ms": issued,
            "model_version": model.version, "strategy_version": strategy_version,
            "method": "one_sided_wilson_95_iid_assumption",
            "probability_lower": wilson_lower(wins, len(selected)),
            "observations": len(selected), "net_wins": wins,
            "holdout_start_ms": min(r.decision_ms for r in validated),
            "holdout_end_ms": max(r.exit_ms for r in validated),
            "selected_evidence_sha256": digest,
            "selection": "frozen_model_nonuncertain_positive_predicted_net",
            "independent_trades_assumed": True, "paper_only": True,
            "authenticity_verified": False}
