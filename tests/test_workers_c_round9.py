"""Round-9 C rolling runtime health: diagnostics != model/market certification."""
from collections import deque

from eth_tester.workers import summarize_fast_lane


def test_round9_latency_nearest_rank_only_real_paper_publications():
    samples = [
        {"status": "published", "reason": "b_immutable_paper_only",
         "publication_latency_ms": latency}
        for latency in [100, 10, 40, 20, 30]
    ]
    samples += [
        {"status": "pending", "reason": "l2_replay_backlog_or_invalid",
         "publication_latency_ms": 0},
        {"status": "degraded", "reason": "a" * 100000},
    ]
    metrics = summarize_fast_lane(samples)
    assert metrics["sample_count"] == 7
    assert metrics["published_samples"] == 5
    assert metrics["latency_p50_ms"] == 30
    assert metrics["latency_p95_ms"] == 100
    assert metrics["latency_max_ms"] == 100
    assert metrics["reason_counts"] == {
        "b_immutable_paper_only": 5,
        "l2_replay_backlog_or_invalid": 1,
        "other": 1,
    }
    assert metrics["quality"] == "observed_paper_only_not_ml_certified"


def test_round9_rolling_counters_are_bounded_and_no_fake_latency():
    samples = deque(maxlen=256)
    for i in range(1000):
        samples.append({"status": "pending", "reason": "offline-" + str(i)})
    status = summarize_fast_lane(samples)
    assert status["sample_count"] == 256
    assert status["published_samples"] == 0
    assert status["latency_p50_ms"] is None
    assert status["latency_p95_ms"] is None
    assert status["latency_max_ms"] is None
    assert status["reason_counts"] == {"other": 256}


def test_round9_invalid_or_fabricated_future_latency_not_in_percentiles():
    status = summarize_fast_lane([
        {"status": "published", "reason": "b_witness_already_issued",
         "publication_latency_ms": -1},
        {"status": "published", "reason": "b_witness_already_issued",
         "publication_latency_ms": 90001},
        {"status": "published", "reason": "b_witness_already_issued",
         "publication_latency_ms": 22.4},
    ])
    assert status["published_samples"] == 3
    assert status["latency_max_ms"] is None
    assert status["quality"].endswith("not_ml_certified")
